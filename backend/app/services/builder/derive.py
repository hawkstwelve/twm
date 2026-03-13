"""Phase 2 derivation helpers for multi-component variables.

Builds derived fields directly from model component VarSpecs:
  - wspd10m: hypot(10u, 10v) converted to mph
  - radar_ptype_combo: indexed palette field from refc + categorical masks
  - precip_ptype_blend: indexed palette field from prate + categorical masks
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import re
import time
from typing import Any, Callable, Literal, overload

import numpy as np
import rasterio
import rasterio.transform
import rasterio.crs

from app.services.builder.cog_writer import warp_to_target_grid
from app.services.builder.fetch import convert_units, fetch_variable, inventory_lines_for_pattern
from app.services.builder.fetch import HerbieTransientUnavailableError
from app.services.colormaps import (
    PRECIP_PTYPE_BINS_PER_TYPE,
    PRECIP_PTYPE_BREAKS,
    PRECIP_PTYPE_ORDER,
    PRECIP_PTYPE_RANGE,
    RADAR_PTYPE_BREAKS,
    RADAR_PTYPE_ORDER,
)

logger = logging.getLogger(__name__)
_MISSING_CSNOW_SAMPLE_LOG_COUNT = 0
_KUCHERA_PTYPE_GATE_WARN_INTERVAL_SECONDS = 60.0
_KUCHERA_PTYPE_GATE_LAST_WARN_TS = 0.0
_KUCHERA_PTYPE_GATE_WARN_LOCK = threading.Lock()
_KUCHERA_DEFAULT_LEVELS_HPA: tuple[int, ...] = (925, 850, 700, 600, 500)
_KUCHERA_VENDOR_T0_K = np.float32(271.16)
_KUCHERA_RATIO_CLAMP_MIN = np.float32(5.0)
_KUCHERA_RATIO_CLAMP_MAX = np.float32(30.0)
_KUCHERA_INCREMENTAL_WINDOW_DEFAULT = 6
_KUCHERA_SIMPLIFIED_PROFILE_MAX_LEVELS = 4
_KUCHERA_SFC_PRESSURE_MARGIN_PA_DEFAULT = np.float32(2500.0)
_APCP_ACCUM_HOUR_WINDOW_RE = re.compile(
    r":APCP:surface:(\d+)-(\d+)\s*hour acc(?:\s*fcst|@\([^)]*\))",
    re.IGNORECASE,
)
_APCP_ACCUM_DAY_WINDOW_RE = re.compile(
    r":APCP:surface:(\d+)-(\d+)\s*day acc(?:\s*fcst|@\([^)]*\))",
    re.IGNORECASE,
)


@dataclass
class FetchContext:
    fetch_cache: dict[
        tuple[str, str, str, int, str, str, str, str],
        tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine],
    ] = field(default_factory=dict)
    warp_cache: dict[
        tuple[str, str, str, int, str, str, str, str],
        tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine],
    ] = field(default_factory=dict)
    fetch_meta_cache: dict[
        tuple[str, str, str, int, str, str, str, str],
        dict[str, Any],
    ] = field(default_factory=dict)
    warp_meta_cache: dict[
        tuple[str, str, str, int, str, str, str, str],
        dict[str, Any],
    ] = field(default_factory=dict)
    derive_quality: dict[tuple[str, int], dict[str, Any]] = field(default_factory=dict)
    stats: dict[str, int] = field(default_factory=lambda: {"hits": 0, "misses": 0})
    warp_stats: dict[str, int] = field(default_factory=lambda: {"hits": 0, "misses": 0})
    coverage: str | None = None
    bundle_fetch_cache: Any | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)


@dataclass(frozen=True)
class DeriveStrategy:
    id: str
    required_inputs: tuple[str, ...]
    output_var_key: str | None
    execute: Callable[..., tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]]


def _parse_hint_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_hint_int(value: Any, *, default: int, minimum: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = int(default)
    return max(int(minimum), int(parsed))


def _parse_kuchera_levels_hpa(value: Any) -> list[int]:
    if isinstance(value, (list, tuple, set)):
        tokens = list(value)
    elif value is None:
        tokens = list(_KUCHERA_DEFAULT_LEVELS_HPA)
    else:
        raw = str(value).replace(";", ",")
        tokens = [token.strip() for token in raw.split(",") if token.strip()]

    levels: list[int] = []
    for token in tokens:
        try:
            parsed = int(token)
        except (TypeError, ValueError):
            continue
        if parsed <= 0 or parsed in levels:
            continue
        levels.append(parsed)

    if not levels:
        levels = list(_KUCHERA_DEFAULT_LEVELS_HPA)
    return levels


def _kuchera_maxt_low500_from_temp_stack_k(temp_stack_c: list[np.ndarray]) -> np.ndarray:
    if not temp_stack_c:
        raise ValueError("kuchera requires at least one temperature level")

    shape = temp_stack_c[0].shape
    for layer in temp_stack_c[1:]:
        if layer.shape != shape:
            raise ValueError(f"kuchera temperature shape mismatch: {layer.shape} != {shape}")

    max_temp_k = np.full(shape, -np.inf, dtype=np.float32)
    any_valid = np.zeros(shape, dtype=bool)
    for temp_layer_c in temp_stack_c:
        temp_layer_k = (temp_layer_c.astype(np.float32, copy=False) + np.float32(273.15)).astype(np.float32, copy=False)
        finite = np.isfinite(temp_layer_k)
        any_valid = any_valid | finite
        max_temp_k = np.maximum(
            max_temp_k,
            np.where(finite, temp_layer_k, -np.inf).astype(np.float32, copy=False),
        )

    return np.where(any_valid, max_temp_k, np.nan).astype(np.float32, copy=False)


def _kuchera_ratio_from_maxt_low500_k(max_temp_k: np.ndarray) -> np.ndarray:
    ratio = np.full(max_temp_k.shape, np.nan, dtype=np.float32)
    finite = np.isfinite(max_temp_k)
    if not np.any(finite):
        return ratio

    warm_branch = finite & (max_temp_k > _KUCHERA_VENDOR_T0_K)
    if np.any(warm_branch):
        ratio[warm_branch] = 12.0 + 2.0 * (_KUCHERA_VENDOR_T0_K - max_temp_k[warm_branch])

    cold_branch = finite & ~warm_branch
    if np.any(cold_branch):
        ratio[cold_branch] = 12.0 + 1.0 * (_KUCHERA_VENDOR_T0_K - max_temp_k[cold_branch])

    return np.clip(ratio, _KUCHERA_RATIO_CLAMP_MIN, _KUCHERA_RATIO_CLAMP_MAX).astype(np.float32, copy=False)


def _compute_kuchera_slr(
    *,
    levels_hpa: list[int],
    temp_stack_c: list[np.ndarray],
) -> np.ndarray:
    if len(temp_stack_c) != len(levels_hpa):
        raise ValueError("kuchera temperature level count mismatch")
    max_temp_k = _kuchera_maxt_low500_from_temp_stack_k(temp_stack_c)
    ratio = _kuchera_ratio_from_maxt_low500_k(max_temp_k)
    return np.where(np.isfinite(ratio), ratio, 10.0).astype(np.float32, copy=False)


def _run_id_from_date(run_date: datetime) -> str:
    run_date_utc = run_date.astimezone(timezone.utc) if run_date.tzinfo else run_date.replace(tzinfo=timezone.utc)
    return run_date_utc.strftime("%Y%m%d_%Hz")


def _kuchera_select_profile_levels(levels_hpa: list[int], *, simplified: bool) -> list[int]:
    """Select Kuchera profile levels deterministically for operational mode.

    In simplified mode we cap profile fetches to a small fixed set (prefer
    850/700/600/500 hPa) to keep per-frame cost low while retaining a stable
    warm-layer estimate.
    """
    if not levels_hpa:
        return []
    if not simplified:
        return list(levels_hpa)
    preferred_order = (850, 700, 600, 500, 925)
    selected: list[int] = []
    for level in preferred_order:
        if level in levels_hpa and level not in selected:
            selected.append(level)
        if len(selected) >= _KUCHERA_SIMPLIFIED_PROFILE_MAX_LEVELS:
            break
    if len(selected) < _KUCHERA_SIMPLIFIED_PROFILE_MAX_LEVELS:
        for level in levels_hpa:
            if level not in selected:
                selected.append(level)
            if len(selected) >= _KUCHERA_SIMPLIFIED_PROFILE_MAX_LEVELS:
                break
    return selected


def _read_value_cog(path: Path) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    with rasterio.open(path) as ds:
        data = ds.read(1).astype(np.float32, copy=False)
        return data, ds.crs, ds.transform


def _kuchera_load_prior_cumulative(
    *,
    model_id: str,
    run_date: datetime,
    var_key: str,
    fh: int,
    ctx: FetchContext | None,
    scale_divisor: float = 0.03937007874015748,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine] | None:
    if fh <= 0:
        return None
    if not np.isfinite(scale_divisor) or float(scale_divisor) <= 0.0:
        return None
    run_id = _run_id_from_date(run_date)
    cache_key = (str(model_id), str(run_id), str(var_key), int(fh))
    if ctx is not None:
        cache = getattr(ctx, "kuchera_cumulative_cache", None)
        if isinstance(cache, dict) and cache_key in cache:
            cached = cache[cache_key]
            return cached[0], cached[1], cached[2]

    data_root_raw = getattr(ctx, "data_root", None) if ctx is not None else None
    if data_root_raw is None:
        data_root_raw = (
            os.getenv("CARTOSKY_DATA_ROOT")
            or os.getenv("CARTOSKY_V3_DATA_ROOT")
            or os.getenv("TWF_V3_DATA_ROOT", "./data")
        )
    try:
        data_root = Path(str(data_root_raw))
    except Exception:
        return None

    candidate_paths = [
        data_root / "staging" / str(model_id) / run_id / str(var_key) / f"fh{int(fh):03d}.val.cog.tif",
        data_root / "published" / str(model_id) / run_id / str(var_key) / f"fh{int(fh):03d}.val.cog.tif",
    ]
    for candidate in candidate_paths:
        try:
            if not candidate.exists():
                continue
            loaded_data, loaded_crs, loaded_transform = _read_value_cog(candidate)
            loaded_data = (loaded_data / np.float32(scale_divisor)).astype(np.float32, copy=False)
            loaded = (loaded_data, loaded_crs, loaded_transform)
            if ctx is not None:
                cache = getattr(ctx, "kuchera_cumulative_cache", None)
                if not isinstance(cache, dict):
                    cache = {}
                    setattr(ctx, "kuchera_cumulative_cache", cache)
                cache[cache_key] = loaded
            return loaded
        except Exception:
            continue
    return None


def _kuchera_store_cumulative_cache(
    *,
    model_id: str,
    run_date: datetime,
    var_key: str,
    fh: int,
    data: np.ndarray,
    crs: rasterio.crs.CRS,
    transform: rasterio.transform.Affine,
    ctx: FetchContext | None,
) -> None:
    if ctx is None:
        return
    cache = getattr(ctx, "kuchera_cumulative_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(ctx, "kuchera_cumulative_cache", cache)
    cache_key = (str(model_id), _run_id_from_date(run_date), str(var_key), int(fh))
    cache[cache_key] = (data.astype(np.float32, copy=False), crs, transform)


def derive_variable(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None = None,
    model_plugin: Any,
    fetch_ctx: FetchContext | None = None,
    derive_component_target_grid: dict[str, str] | None = None,
    derive_component_resampling: str | None = None,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    """Compute a derived variable field and return source grid metadata."""
    derive_kind = (
        getattr(var_capability, "derive_strategy_id", None)
        or getattr(var_spec_model, "derive", None)
    )
    strategy = DERIVE_STRATEGIES.get(str(derive_kind))
    if strategy is None:
        raise ValueError(f"Unsupported derive strategy: {derive_kind!r}")
    return strategy.execute(
        model_id=model_id,
        var_key=var_key,
        product=product,
        run_date=run_date,
        fh=fh,
        var_spec_model=var_spec_model,
        var_capability=var_capability,
        model_plugin=model_plugin,
        ctx=fetch_ctx,
        derive_component_target_grid=derive_component_target_grid,
        derive_component_resampling=derive_component_resampling,
    )


def _resolve_component_var(model_plugin: Any, var_key: str) -> tuple[str, Any]:
    normalized_key = model_plugin.normalize_var_id(var_key)
    capability = model_plugin.get_var_capability(normalized_key)
    spec = model_plugin.get_var(normalized_key)
    if capability is None and spec is None:
        raise ValueError(
            f"Component var {normalized_key!r} not found in plugin "
            f"{getattr(model_plugin, 'id', '?')!r}"
        )
    selectors = (
        getattr(capability, "selectors", None)
        if capability is not None
        else getattr(spec, "selectors", None)
    )
    if selectors is None or not getattr(selectors, "search", None):
        raise ValueError(f"Component var {normalized_key!r} has no search patterns")
    return normalized_key, selectors


def _selector_fingerprint(selectors: Any) -> str:
    search = tuple(
        " ".join(str(pattern).split())
        for pattern in getattr(selectors, "search", [])
        if str(pattern).strip()
    )
    filter_by_keys = tuple(
        sorted(
            (str(key), str(value))
            for key, value in dict(getattr(selectors, "filter_by_keys", {}) or {}).items()
        )
    )
    hints = tuple(
        sorted(
            (str(key), str(value))
            for key, value in dict(getattr(selectors, "hints", {}) or {}).items()
        )
    )
    return repr((search, filter_by_keys, hints))


def _record_fetch_stat(ctx: FetchContext | None, metric: str) -> None:
    if ctx is None:
        return
    with ctx._lock:
        ctx.stats[metric] = int(ctx.stats.get(metric, 0)) + 1


def _record_warp_stat(ctx: FetchContext | None, metric: str) -> None:
    if ctx is None:
        return
    with ctx._lock:
        ctx.warp_stats[metric] = int(ctx.warp_stats.get(metric, 0)) + 1


def _record_derive_quality(
    ctx: FetchContext | None,
    *,
    var_key: str,
    fh: int,
    quality_flags: list[str],
) -> None:
    if ctx is None:
        return
    deduped_flags = [
        flag for flag in dict.fromkeys(str(item).strip() for item in quality_flags)
        if flag
    ]
    payload = {
        "quality": "degraded" if deduped_flags else "full",
        "quality_flags": deduped_flags,
    }
    with ctx._lock:
        ctx.derive_quality[(str(var_key), int(fh))] = payload


# ---------------------------------------------------------------------------
# Bounded parallel prefetch for cumulative derive strategies
# ---------------------------------------------------------------------------

_PREFETCH_DEFAULT_WORKERS = 6
_PREFETCH_ENV_WORKERS = (
    "CARTOSKY_DERIVE_PREFETCH_WORKERS",
    "CARTOSKY_V3_DERIVE_PREFETCH_WORKERS",
    "TWF_V3_DERIVE_PREFETCH_WORKERS",
)
# If this fraction of prefetch tasks fail, stop launching new ones.
_PREFETCH_FAIL_ABORT_RATIO = 0.5
# Minimum tasks that must have completed before the abort ratio is evaluated.
_PREFETCH_FAIL_ABORT_MIN_COMPLETED = 4
# Brief sleep injected after a failed prefetch to back off upstream sources.
_PREFETCH_BACKOFF_SECONDS = 0.3


def _prefetch_max_workers() -> int:
    """Resolve bounded worker count from env or default."""
    raw = ""
    for env_name in _PREFETCH_ENV_WORKERS:
        raw = os.getenv(env_name, "").strip()
        if raw:
            break
    if raw:
        try:
            return max(1, min(int(raw), 12))
        except ValueError:
            pass
    return _PREFETCH_DEFAULT_WORKERS


@dataclass(frozen=True)
class _PrefetchTask:
    """Describes one GRIB component to pre-warm in the FetchContext cache."""
    model_id: str
    product: str
    run_date: datetime
    fh: int
    model_plugin: Any
    var_key: str
    warped: bool = False
    target_region: str = ""
    target_grid_id: str = ""
    resampling: str = ""

    @property
    def _dedup_key(self) -> tuple:
        if self.warped:
            return (self.model_id, self.product, self.fh, self.var_key,
                    self.target_grid_id, self.resampling)
        return (self.model_id, self.product, self.fh, self.var_key)


def _prefetch_components_parallel(
    tasks: list[_PrefetchTask],
    ctx: FetchContext | None,
    *,
    label: str = "",
) -> int:
    """Prefetch GRIB components with bounded concurrency and backoff.

    Warms the FetchContext cache so the subsequent sequential accumulation
    loop sees near-100% cache hits.  Failures are silently skipped — the
    main loop will attempt its own fetch and handle errors with existing
    error-handling logic.

    Returns the number of successfully prefetched items.
    """
    if not tasks or ctx is None:
        return 0

    # Deduplicate by cache-relevant fields.
    seen: set[tuple] = set()
    unique: list[_PrefetchTask] = []
    for task in tasks:
        key = task._dedup_key
        if key in seen:
            continue
        seen.add(key)
        unique.append(task)

    if not unique:
        return 0

    workers = min(_prefetch_max_workers(), len(unique))

    # For very small task lists, skip the thread-pool overhead entirely.
    if workers <= 1 or len(unique) <= 2:
        return _prefetch_sequential(unique, ctx)

    succeeded = 0
    failed = 0
    lock = threading.Lock()

    def _run_one(task: _PrefetchTask) -> bool:
        # Early abort check: if many tasks have already failed, skip new ones
        # to avoid hammering a struggling upstream source.
        with lock:
            total_done = succeeded + failed
            if (
                total_done >= _PREFETCH_FAIL_ABORT_MIN_COMPLETED
                and failed > total_done * _PREFETCH_FAIL_ABORT_RATIO
            ):
                return False
        try:
            if task.warped:
                _fetch_component_warped(
                    model_id=task.model_id,
                    product=task.product,
                    run_date=task.run_date,
                    fh=task.fh,
                    model_plugin=task.model_plugin,
                    var_key=task.var_key,
                    target_region=task.target_region,
                    target_grid_id=task.target_grid_id,
                    resampling=task.resampling,
                    ctx=ctx,
                )
            else:
                _fetch_component(
                    model_id=task.model_id,
                    product=task.product,
                    run_date=task.run_date,
                    fh=task.fh,
                    model_plugin=task.model_plugin,
                    var_key=task.var_key,
                    ctx=ctx,
                )
            return True
        except Exception:
            # Backoff briefly so concurrent workers don't stampede a failing source.
            time.sleep(_PREFETCH_BACKOFF_SECONDS)
            return False

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_run_one, task): task for task in unique}
        for future in as_completed(futures):
            try:
                ok = future.result()
            except Exception:
                ok = False
            with lock:
                if ok:
                    succeeded += 1
                else:
                    failed += 1

    elapsed_ms = (time.monotonic() - t0) * 1000
    log_label = f" [{label}]" if label else ""
    logger.info(
        "prefetch%s complete: %d/%d ok, %d failed, workers=%d, %.0fms",
        log_label,
        succeeded,
        len(unique),
        failed,
        workers,
        elapsed_ms,
    )
    return succeeded


def _prefetch_sequential(
    tasks: list[_PrefetchTask],
    ctx: FetchContext | None,
) -> int:
    """Fallback: prefetch a small task list without thread-pool overhead."""
    if not tasks or ctx is None:
        return 0
    ok = 0
    for task in tasks:
        try:
            if task.warped:
                _fetch_component_warped(
                    model_id=task.model_id,
                    product=task.product,
                    run_date=task.run_date,
                    fh=task.fh,
                    model_plugin=task.model_plugin,
                    var_key=task.var_key,
                    target_region=task.target_region,
                    target_grid_id=task.target_grid_id,
                    resampling=task.resampling,
                    ctx=ctx,
                )
            else:
                _fetch_component(
                    model_id=task.model_id,
                    product=task.product,
                    run_date=task.run_date,
                    fh=task.fh,
                    model_plugin=task.model_plugin,
                    var_key=task.var_key,
                    ctx=ctx,
                )
            ok += 1
        except Exception:
            pass
    return ok


def _resolve_component_cache_identity(model_plugin: Any, var_key: str) -> tuple[str, str]:
    normalized_var_key, selectors = _resolve_component_var(model_plugin, var_key)
    return normalized_var_key, _selector_fingerprint(selectors)


def _parse_apcp_accum_window_hours(inventory_line: str | None) -> tuple[int, int] | None:
    if not inventory_line:
        return None
    line = str(inventory_line)
    match = _APCP_ACCUM_HOUR_WINDOW_RE.search(line)
    if match is not None:
        try:
            start_hour = int(match.group(1))
            end_hour = int(match.group(2))
        except (TypeError, ValueError):
            return None
    else:
        match = _APCP_ACCUM_DAY_WINDOW_RE.search(line)
        if match is None:
            return None
        try:
            start_hour = int(match.group(1)) * 24
            end_hour = int(match.group(2)) * 24
        except (TypeError, ValueError):
            return None
    if start_hour < 0 or end_hour < 0:
        return None
    return start_hour, end_hour


def _is_probabilistic_apcp_inventory_line(inventory_line: str | None) -> bool:
    line = str(inventory_line or "").strip().lower()
    if not line:
        return False
    return "probability" in line or ":prob " in line


def _apcp_inventory_search_pattern(inventory_line: str | None) -> str:
    line = str(inventory_line or "").strip()
    marker = line.find(":APCP:")
    if marker < 0:
        return ""
    message = line[marker:]
    if message.endswith("$"):
        return message
    return message + "$"


def _classify_apcp_mode_for_kuchera(
    *,
    inventory_line: str | None,
    step_fh: int,
    expected_start_fh: int,
) -> str:
    if _is_probabilistic_apcp_inventory_line(inventory_line):
        return "invalid"
    window = _parse_apcp_accum_window_hours(inventory_line)
    if window is None:
        return "invalid"
    start_hour, end_hour = window
    if end_hour != int(step_fh):
        return "invalid"
    if start_hour > int(expected_start_fh):
        return "invalid"
    if start_hour == int(expected_start_fh):
        return "exact_step"
    if start_hour == 0 and int(expected_start_fh) > 0:
        return "cumulative_from_zero"
    if 0 <= start_hour < int(expected_start_fh):
        return "overlap_window"
    return "invalid"


def _apcp_exact_window_pattern(start_fh: int, end_fh: int) -> str:
    return f":APCP:surface:{int(start_fh)}-{int(end_fh)} hour acc fcst:$"


def _kuchera_primary_herbie_priority() -> str:
    raw = os.getenv("CARTOSKY_HERBIE_PRIORITY") or os.getenv("TWF_HERBIE_PRIORITY", "aws")
    for token in str(raw).split(","):
        candidate = token.strip()
        if candidate:
            return candidate
    return "aws"


def _kuchera_inventory_lines(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    search_pattern: str,
) -> list[str]:
    priority = _kuchera_primary_herbie_priority()
    try:
        return inventory_lines_for_pattern(
            model_id=model_id,
            product=product,
            run_date=run_date,
            fh=int(fh),
            search_pattern=search_pattern,
            herbie_kwargs={"priority": priority},
        )
    except Exception:
        return []


def _kuchera_select_apcp_window_from_inventory(
    *,
    inventory_lines: list[str],
    step_fh: int,
    expected_start_fh: int,
) -> dict[str, Any] | None:
    exact: dict[str, Any] | None = None
    cumulative: dict[str, Any] | None = None
    overlap: dict[str, Any] | None = None
    for line in inventory_lines:
        window = _parse_apcp_accum_window_hours(line)
        if window is None:
            continue
        start_hour, end_hour = window
        mode = _classify_apcp_mode_for_kuchera(
            inventory_line=line,
            step_fh=step_fh,
            expected_start_fh=expected_start_fh,
        )
        if mode == "invalid":
            continue
        candidate = {
            "start_hour": int(start_hour),
            "end_hour": int(end_hour),
            "selected_window": f"{int(start_hour)}-{int(end_hour)}",
            "inventory_line": str(line),
            "search_pattern": _apcp_inventory_search_pattern(line),
            "mode": mode,
        }
        if mode == "exact_step":
            exact = candidate
            break
        if mode == "cumulative_from_zero":
            cumulative = candidate
            continue
        if mode == "overlap_window" and (
            overlap is None or int(start_hour) > int(overlap["start_hour"])
        ):
            overlap = candidate

    if exact is not None:
        return exact
    if cumulative is not None:
        return cumulative
    return overlap


def _normalize_ptype_probability(data: np.ndarray) -> np.ndarray:
    values = np.asarray(data, dtype=np.float32)
    finite = np.isfinite(values)
    max_val = float(np.nanmax(values[finite])) if np.any(finite) else 0.0
    scale = 100.0 if max_val > 1.5 else 1.0
    normalized = values / np.float32(scale)
    normalized = np.clip(normalized, 0.0, 1.0).astype(np.float32, copy=False)
    return normalized


def _apply_kuchera_ptype_gate(apcp_step: np.ndarray, frozen_frac: np.ndarray) -> np.ndarray:
    if apcp_step.shape != frozen_frac.shape:
        raise ValueError(f"kuchera ptype gate shape mismatch: {apcp_step.shape} != {frozen_frac.shape}")
    frozen = np.clip(np.asarray(frozen_frac, dtype=np.float32), 0.0, 1.0).astype(np.float32, copy=False)
    return (np.asarray(apcp_step, dtype=np.float32) * frozen).astype(np.float32, copy=False)


def _log_kuchera_ptype_gate_warning_once(*, model_id: str, var_key: str, step_fh: int, reason: str) -> None:
    global _KUCHERA_PTYPE_GATE_LAST_WARN_TS
    now = time.monotonic()
    should_log = False
    with _KUCHERA_PTYPE_GATE_WARN_LOCK:
        if now - _KUCHERA_PTYPE_GATE_LAST_WARN_TS >= _KUCHERA_PTYPE_GATE_WARN_INTERVAL_SECONDS:
            _KUCHERA_PTYPE_GATE_LAST_WARN_TS = now
            should_log = True
    if should_log:
        logger.warning(
            "kuchera_ptype_gate fallback=ones model=%s var=%s step_fh=%03d reason=%s",
            model_id,
            var_key,
            int(step_fh),
            reason,
        )


def _kuchera_frozen_fraction_for_step(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    step_fh: int,
    model_plugin: Any,
    use_warped: bool,
    target_region: str,
    target_grid_id: str,
    resampling: str,
    ctx: FetchContext | None,
    expected_shape: tuple[int, ...],
) -> tuple[np.ndarray, bool, int]:
    component_keys = ("csnow", "crain", "cicep", "cfrzr")
    fetched: dict[str, np.ndarray] = {}
    fetch_count = 0
    try:
        for key in component_keys:
            component_data, _, _ = _fetch_step_component(
                model_id=model_id,
                product=product,
                run_date=run_date,
                step_fh=step_fh,
                model_plugin=model_plugin,
                var_key=key,
                use_warped=use_warped,
                target_region=target_region,
                target_grid_id=target_grid_id,
                resampling=resampling,
                ctx=ctx,
            )
            fetch_count += 1
            component_clean = np.asarray(component_data, dtype=np.float32)
            if component_clean.shape != expected_shape:
                raise ValueError(
                    f"kuchera ptype component shape mismatch for {key}: "
                    f"{component_clean.shape} != {expected_shape}"
                )
            fetched[key] = component_clean
    except Exception as exc:
        _log_kuchera_ptype_gate_warning_once(
            model_id=model_id,
            var_key=var_key,
            step_fh=step_fh,
            reason=str(exc),
        )
        return np.ones(expected_shape, dtype=np.float32), True, fetch_count

    csnow_prob = _normalize_ptype_probability(fetched["csnow"])
    _ = _normalize_ptype_probability(fetched["crain"])
    cicep_prob = _normalize_ptype_probability(fetched["cicep"])
    _ = _normalize_ptype_probability(fetched["cfrzr"])
    frozen_frac = np.clip(csnow_prob + cicep_prob, 0.0, 1.0).astype(np.float32, copy=False)
    return frozen_frac, False, fetch_count


@overload
def _fetch_component(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    model_plugin: Any,
    var_key: str,
    ctx: FetchContext | None = ...,
    return_meta: Literal[False] = ...,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]: ...


@overload
def _fetch_component(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    model_plugin: Any,
    var_key: str,
    ctx: FetchContext | None = ...,
    return_meta: Literal[True],
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine, dict[str, Any]]: ...


def _fetch_component(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    model_plugin: Any,
    var_key: str,
    ctx: FetchContext | None = None,
    return_meta: bool = False,
) -> (
    tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]
    | tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine, dict[str, Any]]
):
    normalized_var_key, selectors = _resolve_component_var(model_plugin, var_key)
    run_date_utc = run_date.astimezone(timezone.utc) if run_date.tzinfo else run_date.replace(tzinfo=timezone.utc)
    cache_key = (
        str(model_id),
        str(product),
        run_date_utc.isoformat(),
        int(fh),
        str(normalized_var_key),
        _selector_fingerprint(selectors),
        str(getattr(ctx, "coverage", "") if ctx is not None else ""),
        str(getattr(model_plugin, "coverage", "")),
    )
    if ctx is not None and cache_key in ctx.fetch_cache:
        _record_fetch_stat(ctx, "hits")
        cached = ctx.fetch_cache[cache_key]
        if return_meta:
            cached_meta = dict(ctx.fetch_meta_cache.get(cache_key, {}))
            return cached[0], cached[1], cached[2], cached_meta
        return cached

    last_exc: Exception | None = None
    for search_pattern in selectors.search:
        try:
            fetch_kwargs: dict[str, Any] = {}
            if ctx is not None and getattr(ctx, "bundle_fetch_cache", None) is not None:
                fetch_kwargs["bundle_fetch_cache"] = getattr(ctx, "bundle_fetch_cache")
            fetch_result = fetch_variable(
                model_id=model_id,
                product=product,
                search_pattern=search_pattern,
                run_date=run_date,
                fh=fh,
                **fetch_kwargs,
                return_meta=True,
            )
            data, crs, transform, meta = fetch_result
            resolved = data.astype(np.float32, copy=False), crs, transform
            if ctx is not None:
                ctx.fetch_cache[cache_key] = resolved
                ctx.fetch_meta_cache[cache_key] = dict(meta)
                _record_fetch_stat(ctx, "misses")
            if return_meta:
                return resolved[0], resolved[1], resolved[2], dict(meta)
            return resolved
        except (HerbieTransientUnavailableError, RuntimeError) as exc:
            last_exc = exc
            continue
    if last_exc is not None:
        raise last_exc
    raise ValueError(f"Component var {normalized_var_key!r} has no usable search patterns")


def _fetch_component_warped(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    model_plugin: Any,
    var_key: str,
    target_region: str,
    target_grid_id: str,
    resampling: str,
    ctx: FetchContext | None = None,
    return_meta: bool = False,
) -> (
    tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]
    | tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine, dict[str, Any]]
):
    normalized_var_key, selector_fingerprint = _resolve_component_cache_identity(model_plugin, var_key)
    run_date_utc = run_date.astimezone(timezone.utc) if run_date.tzinfo else run_date.replace(tzinfo=timezone.utc)
    cache_key = (
        str(model_id),
        str(product),
        run_date_utc.isoformat(),
        int(fh),
        str(normalized_var_key),
        str(selector_fingerprint),
        str(target_grid_id),
        str(resampling),
    )
    if ctx is not None and cache_key in ctx.warp_cache:
        _record_warp_stat(ctx, "hits")
        cached = ctx.warp_cache[cache_key]
        if return_meta:
            cached_meta = dict(ctx.warp_meta_cache.get(cache_key, {}))
            return cached[0], cached[1], cached[2], cached_meta
        return cached

    raw_data, raw_crs, raw_transform, raw_meta = _fetch_component(
        model_id=model_id,
        product=product,
        run_date=run_date,
        fh=fh,
        model_plugin=model_plugin,
        var_key=normalized_var_key,
        ctx=ctx,
        return_meta=True,
    )
    warped_data, dst_transform = warp_to_target_grid(
        raw_data,
        raw_crs,
        raw_transform,
        model=model_id,
        region=target_region,
        resampling=resampling,
        src_nodata=None,
        dst_nodata=float("nan"),
    )
    resolved = (
        warped_data.astype(np.float32, copy=False),
        rasterio.crs.CRS.from_epsg(3857),
        dst_transform,
    )
    if ctx is not None:
        ctx.warp_cache[cache_key] = resolved
        ctx.warp_meta_cache[cache_key] = dict(raw_meta)
        _record_warp_stat(ctx, "misses")
    if return_meta:
        return resolved[0], resolved[1], resolved[2], dict(raw_meta)
    return resolved


def _cadence_hint_suffix(hints: dict[str, Any]) -> str:
    parts: list[str] = []
    step_hours = hints.get("step_hours")
    transition = hints.get("step_transition_fh")
    after = hints.get("step_hours_after_fh")
    if step_hours is not None and str(step_hours).strip():
        parts.append(f"step_hours={step_hours}")
    if transition is not None and str(transition).strip():
        parts.append(f"transition={transition}")
    if after is not None and str(after).strip():
        parts.append(f"after={after}")
    return f" {' '.join(parts)}" if parts else ""


def _derive_uses_warped_components(
    derive_component_target_grid: dict[str, str] | None,
    derive_component_resampling: str | None,
) -> bool:
    if derive_component_target_grid is None:
        return False
    region = str(derive_component_target_grid.get("region", "")).strip()
    if not region:
        return False
    return isinstance(derive_component_resampling, str) and bool(derive_component_resampling.strip())


def _resolve_cumulative_step_fhs(
    *,
    hints: dict[str, Any],
    fh: int,
    run_date: datetime | None = None,
    default_step_hours: int = 6,
) -> list[int]:
    step_hours_raw = hints.get("step_hours", str(default_step_hours))
    step_transition_fh_raw = hints.get("step_transition_fh")
    step_hours_after_fh_raw = hints.get("step_hours_after_fh")

    try:
        step_hours = max(1, int(step_hours_raw))
    except (TypeError, ValueError):
        step_hours = default_step_hours
    try:
        step_transition_fh = int(step_transition_fh_raw) if step_transition_fh_raw is not None else None
    except (TypeError, ValueError):
        step_transition_fh = None
    try:
        step_hours_after_fh = int(step_hours_after_fh_raw) if step_hours_after_fh_raw is not None else None
    except (TypeError, ValueError):
        step_hours_after_fh = None
    if step_hours_after_fh is not None:
        step_hours_after_fh = max(1, step_hours_after_fh)
    align_after_transition_to_cycle = _parse_hint_bool(
        hints.get("step_hours_after_fh_align_to_cycle"),
        default=False,
    )

    if (
        step_transition_fh is not None
        and step_transition_fh > 0
        and step_hours_after_fh is not None
        and step_hours_after_fh > 0
    ):
        before_end = min(fh, step_transition_fh)
        step_fhs = list(range(step_hours, before_end + 1, step_hours))
        if fh > step_transition_fh:
            after_start = step_transition_fh + step_hours_after_fh
            if align_after_transition_to_cycle and run_date is not None:
                cycle_hour = int(run_date.hour)
                transition_mod = int(step_transition_fh) % int(step_hours_after_fh)
                cycle_mod = cycle_hour % int(step_hours_after_fh)
                offset = (cycle_mod - transition_mod) % int(step_hours_after_fh)
                after_start = int(step_transition_fh) + offset
                if after_start <= int(step_transition_fh):
                    after_start += int(step_hours_after_fh)
            step_fhs.extend(range(after_start, fh + 1, step_hours_after_fh))
        return step_fhs

    return list(range(step_hours, fh + 1, step_hours))


# ---------------------------------------------------------------------------
# Shared infrastructure for cumulative APCP strategies
# ---------------------------------------------------------------------------


def _resolve_warped_state(
    derive_component_target_grid: dict[str, str] | None,
    derive_component_resampling: str | None,
    model_id: str,
) -> tuple[bool, str, str, str]:
    """Resolve warped component state.

    Returns ``(use_warped, target_region, target_grid_id, resampling)``.
    """
    use_warped = _derive_uses_warped_components(
        derive_component_target_grid, derive_component_resampling,
    )
    target_region = (
        str((derive_component_target_grid or {}).get("region", "")).strip()
        if use_warped else ""
    )
    target_grid_id = (
        str((derive_component_target_grid or {}).get("id", "")).strip()
        if use_warped else ""
    )
    if use_warped and not target_grid_id:
        target_grid_id = f"{model_id}:{target_region}"
    resampling = str(derive_component_resampling).strip() if use_warped else ""
    return use_warped, target_region, target_grid_id, resampling


@overload
def _fetch_step_component(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    step_fh: int,
    model_plugin: Any,
    var_key: str,
    use_warped: bool,
    target_region: str,
    target_grid_id: str,
    resampling: str,
    ctx: FetchContext | None,
    return_meta: Literal[False] = ...,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]: ...


@overload
def _fetch_step_component(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    step_fh: int,
    model_plugin: Any,
    var_key: str,
    use_warped: bool,
    target_region: str,
    target_grid_id: str,
    resampling: str,
    ctx: FetchContext | None,
    return_meta: Literal[True],
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine, dict[str, Any]]: ...


def _fetch_step_component(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    step_fh: int,
    model_plugin: Any,
    var_key: str,
    use_warped: bool,
    target_region: str,
    target_grid_id: str,
    resampling: str,
    ctx: FetchContext | None,
    return_meta: bool = False,
) -> (
    tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]
    | tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine, dict[str, Any]]
):
    """Fetch a component for a step, branching warped vs raw."""
    if use_warped:
        return _fetch_component_warped(
            model_id=model_id, product=product, run_date=run_date, fh=step_fh,
            model_plugin=model_plugin, var_key=var_key,
            target_region=target_region, target_grid_id=target_grid_id,
            resampling=resampling, ctx=ctx, return_meta=return_meta,
        )
    return _fetch_component(
        model_id=model_id, product=product, run_date=run_date, fh=step_fh,
        model_plugin=model_plugin, var_key=var_key, ctx=ctx,
        return_meta=return_meta,
    )


def _is_valid_apcp_exact_result(data: Any, meta: dict[str, Any] | None) -> bool:
    """Check whether an inventory-selected APCP fetch returned usable data."""
    if not isinstance(data, np.ndarray):
        return False
    if data.size <= 0:
        return False
    if not np.isfinite(data).any():
        return False
    inventory_line = str((meta or {}).get("inventory_line", "")).strip()
    if not inventory_line:
        return False
    if _is_probabilistic_apcp_inventory_line(inventory_line):
        return False
    return True


@dataclass
class _ApcpCumDiffState:
    """Mutable state for cumulative-to-step APCP differencing across the loop."""
    consumed_sum: np.ndarray | None = None
    consumed_sum_valid: np.ndarray | None = None
    consumed_sum_crs: rasterio.crs.CRS | None = None
    consumed_sum_transform: rasterio.transform.Affine | None = None
    consumed_through_fh: int = 0
    bucket_start_fh: int | None = None
    bucket_cumulative_sum: np.ndarray | None = None
    bucket_cumulative_valid: np.ndarray | None = None
    bucket_cumulative_crs: rasterio.crs.CRS | None = None
    bucket_cumulative_transform: rasterio.transform.Affine | None = None
    bucket_through_fh: int = 0
    recent_exact_steps: dict[
        int,
        tuple[np.ndarray, np.ndarray, rasterio.crs.CRS | None, rasterio.transform.Affine | None],
    ] = field(default_factory=dict)


def _reconstruct_overlap_prior_sum(
    *,
    cum_diff_state: _ApcpCumDiffState,
    start_fh: int,
    through_fh: int,
) -> tuple[np.ndarray, np.ndarray, rasterio.crs.CRS | None, rasterio.transform.Affine | None] | None:
    if through_fh <= start_fh:
        return None

    reconstructed_sum: np.ndarray | None = None
    reconstructed_valid: np.ndarray | None = None
    reconstructed_crs: rasterio.crs.CRS | None = None
    reconstructed_transform: rasterio.transform.Affine | None = None

    for prior_fh in range(int(start_fh) + 1, int(through_fh) + 1):
        cached = cum_diff_state.recent_exact_steps.get(int(prior_fh))
        if cached is None:
            return None
        prior_data, prior_valid, prior_crs, prior_transform = cached
        if reconstructed_sum is None:
            reconstructed_sum = prior_data.copy()
            reconstructed_valid = prior_valid.copy()
            reconstructed_crs = prior_crs
            reconstructed_transform = prior_transform
            continue
        if prior_data.shape != reconstructed_sum.shape:
            return None
        if prior_crs != reconstructed_crs or prior_transform != reconstructed_transform:
            return None
        reconstructed_sum = (reconstructed_sum + prior_data).astype(np.float32, copy=False)
        assert reconstructed_valid is not None
        reconstructed_valid = reconstructed_valid & prior_valid

    if reconstructed_sum is None or reconstructed_valid is None:
        return None
    return reconstructed_sum, reconstructed_valid, reconstructed_crs, reconstructed_transform


def _resolve_apcp_step_data(
    *,
    step_fh: int,
    step_index: int,
    step_fhs: list[int],
    model_id: str,
    product: str,
    run_date: datetime,
    model_plugin: Any,
    ctx: FetchContext | None,
    apcp_component: str,
    apcp_product: str | None,
    use_warped: bool,
    target_region: str,
    target_grid_id: str,
    resampling: str,
    cum_diff_state: _ApcpCumDiffState,
    expected_start_fh_override: int | None = None,
) -> tuple[np.ndarray, np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine, str]:
    """Resolve per-step APCP data with inventory-driven window selection.

    Tries, in order:
      1. FetchContext cache hit
      2. Exact-guess window in Herbie inventory
      3. Best available window ending at step_fh
      4. Component selector regex fallback

    Detects cumulative (0-N hour) windows and differences against the
    previous step's cumulative value (tracked in *cum_diff_state*).

    Returns ``(step_clean, apcp_valid, crs, transform, apcp_mode)``
    where
    *step_clean* is the cleaned per-step increment (>= 0, invalid → 0)
    and *apcp_valid* is the boolean validity mask.
    """
    if expected_start_fh_override is not None and step_index == 0:
        expected_start_fh = int(expected_start_fh_override)
    else:
        expected_start_fh = 0 if step_index == 0 else int(step_fhs[step_index - 1])
    resolved_apcp_product = str(apcp_product or product)
    apcp_search_pattern = _apcp_exact_window_pattern(expected_start_fh, step_fh)
    apcp_step: np.ndarray | None = None
    step_crs: rasterio.crs.CRS | None = None
    step_transform: rasterio.transform.Affine | None = None
    apcp_meta: dict[str, Any] = {}
    exact_guess_used = False
    inventory_selected = False
    selected_window = "none"
    selector_fallback_used = False
    selector_reason = "none"
    apcp_fetch_resolved = False
    selected_mode = "invalid"
    inventory_choice_mode = "invalid"

    # 1. Check FetchContext cache.
    if ctx is not None:
        run_date_utc = (
            run_date.astimezone(timezone.utc)
            if run_date.tzinfo else run_date.replace(tzinfo=timezone.utc)
        )
        try:
            apcp_cache_var_key, apcp_selector_fingerprint = _resolve_component_cache_identity(
                model_plugin, apcp_component,
            )
        except Exception:
            apcp_cache_var_key = None
            apcp_selector_fingerprint = None

        if apcp_cache_var_key is not None and apcp_selector_fingerprint is not None:
            if use_warped:
                warped_cache_key = (
                    str(model_id),
                    str(resolved_apcp_product),
                    run_date_utc.isoformat(),
                    int(step_fh),
                    str(apcp_cache_var_key),
                    str(apcp_selector_fingerprint),
                    str(target_grid_id),
                    str(resampling),
                )
                cached = ctx.warp_cache.get(warped_cache_key)
                if cached is not None:
                    cached_meta = dict(ctx.warp_meta_cache.get(warped_cache_key, {}))
                    cached_line = str(cached_meta.get("inventory_line", "")).strip()
                    cached_mode = _classify_apcp_mode_for_kuchera(
                        inventory_line=cached_line,
                        step_fh=step_fh,
                        expected_start_fh=expected_start_fh,
                    )
                    if cached_mode != "invalid":
                        _record_warp_stat(ctx, "hits")
                        apcp_step, step_crs, step_transform = cached
                        apcp_meta = cached_meta
                        apcp_search_pattern = str((apcp_meta or {}).get("search_pattern", "")).strip() or apcp_search_pattern
                        selector_fallback_used = True
                        selector_reason = "cache_hit"
                        selected_mode = cached_mode
                        apcp_fetch_resolved = True
            else:
                fetch_cache_key = (
                    str(model_id),
                    str(resolved_apcp_product),
                    run_date_utc.isoformat(),
                    int(step_fh),
                    str(apcp_cache_var_key),
                    str(apcp_selector_fingerprint),
                    str(getattr(ctx, "coverage", "")),
                    str(getattr(model_plugin, "coverage", "")),
                )
                cached = ctx.fetch_cache.get(fetch_cache_key)
                if cached is not None:
                    cached_meta = dict(ctx.fetch_meta_cache.get(fetch_cache_key, {}))
                    cached_line = str(cached_meta.get("inventory_line", "")).strip()
                    cached_mode = _classify_apcp_mode_for_kuchera(
                        inventory_line=cached_line,
                        step_fh=step_fh,
                        expected_start_fh=expected_start_fh,
                    )
                    if cached_mode != "invalid":
                        _record_fetch_stat(ctx, "hits")
                        apcp_step, step_crs, step_transform = cached
                        apcp_meta = cached_meta
                        apcp_search_pattern = str((apcp_meta or {}).get("search_pattern", "")).strip() or apcp_search_pattern
                        selector_fallback_used = True
                        selector_reason = "cache_hit"
                        selected_mode = cached_mode
                        apcp_fetch_resolved = True

    # 2. Inventory-driven APCP selection.
    if not apcp_fetch_resolved:
        inventory_lines = _kuchera_inventory_lines(
            model_id=model_id,
            product=resolved_apcp_product,
            run_date=run_date,
            fh=step_fh,
            search_pattern=":APCP:surface:",
        )
        if not inventory_lines:
            selector_fallback_used = True
            selector_reason = "inventory_empty"
        else:
            inventory_choice = _kuchera_select_apcp_window_from_inventory(
                inventory_lines=inventory_lines,
                step_fh=step_fh,
                expected_start_fh=expected_start_fh,
            )
            if inventory_choice is not None:
                apcp_search_pattern = str(inventory_choice.get("search_pattern") or apcp_search_pattern)
                selected_window = str(inventory_choice.get("selected_window") or selected_window)
                inventory_choice_mode = str(inventory_choice.get("mode") or inventory_choice_mode)
                exact_guess_used = inventory_choice_mode == "exact_step"
                inventory_selected = inventory_choice_mode != "exact_step"
                selector_reason = (
                    "inventory_exact_match"
                    if inventory_choice_mode == "exact_step"
                    else "inventory_best_window"
                )
            else:
                selector_fallback_used = True
                selector_reason = "inventory_no_matching_window"

        if not apcp_fetch_resolved and (inventory_selected or exact_guess_used):
            try:
                fetch_kwargs: dict[str, Any] = {}
                if ctx is not None and getattr(ctx, "bundle_fetch_cache", None) is not None:
                    fetch_kwargs["bundle_fetch_cache"] = getattr(ctx, "bundle_fetch_cache")
                selected_data, selected_crs, selected_transform, selected_meta = fetch_variable(
                    model_id=model_id,
                    product=resolved_apcp_product,
                    search_pattern=apcp_search_pattern,
                    run_date=run_date,
                    fh=step_fh,
                    **fetch_kwargs,
                    return_meta=True,
                )
                selected_data = selected_data.astype(np.float32, copy=False)
                selected_meta = dict(selected_meta)

                if use_warped:
                    warped_data, warped_transform = warp_to_target_grid(
                        selected_data,
                        selected_crs,
                        selected_transform,
                        model=model_id,
                        region=target_region,
                        resampling=resampling,
                        src_nodata=None,
                        dst_nodata=float("nan"),
                    )
                    selected_data = warped_data.astype(np.float32, copy=False)
                    selected_crs = rasterio.crs.CRS.from_epsg(3857)
                    selected_transform = warped_transform

                if _is_valid_apcp_exact_result(selected_data, selected_meta):
                    apcp_step = selected_data
                    step_crs = selected_crs
                    step_transform = selected_transform
                    apcp_meta = selected_meta
                    selected_mode = inventory_choice_mode
                    apcp_fetch_resolved = True
                else:
                    selector_fallback_used = True
                    selector_reason = f"{selector_reason}_invalid_result"
            except Exception as exc:
                selector_fallback_used = True
                selector_reason = f"{selector_reason}_error:{exc.__class__.__name__}"

    # 3. Fallback to component selector regex.
    if not apcp_fetch_resolved:
        apcp_step, step_crs, step_transform, apcp_meta = _fetch_step_component(
            model_id=model_id,
            product=resolved_apcp_product,
            run_date=run_date,
            step_fh=step_fh,
            model_plugin=model_plugin,
            var_key=apcp_component,
            use_warped=use_warped,
            target_region=target_region,
            target_grid_id=target_grid_id,
            resampling=resampling,
            ctx=ctx,
            return_meta=True,
        )
        apcp_meta = dict(apcp_meta)
        apcp_search_pattern = str((apcp_meta or {}).get("search_pattern", "")).strip() or apcp_search_pattern
        selector_fallback_used = True
        if selector_reason == "none":
            selector_reason = "selector_regex_fallback"

    # 4. Classify mode and apply cumulative differencing.
    assert apcp_step is not None  # guaranteed set by steps 1/2/3 above
    apcp_valid_raw = np.isfinite(apcp_step) & (apcp_step >= 0.0)
    apcp_cum_clean = np.where(apcp_valid_raw, apcp_step, 0.0).astype(np.float32, copy=False)

    apcp_inventory_line = str((apcp_meta or {}).get("inventory_line", "")).strip()
    apcp_mode = _classify_apcp_mode_for_kuchera(
        inventory_line=apcp_inventory_line,
        step_fh=step_fh,
        expected_start_fh=expected_start_fh,
    )
    if apcp_mode == "invalid" and selected_mode != "invalid":
        apcp_mode = selected_mode

    step_apcp_data = apcp_cum_clean
    apcp_valid = apcp_valid_raw
    fallback_differencing_applied = False
    window = _parse_apcp_accum_window_hours(apcp_inventory_line)

    if apcp_mode == "cumulative_from_zero" and cum_diff_state.consumed_sum is not None:
        same_shape = apcp_cum_clean.shape == cum_diff_state.consumed_sum.shape
        same_crs = step_crs == cum_diff_state.consumed_sum_crs
        same_transform = step_transform == cum_diff_state.consumed_sum_transform
        if not (same_shape and same_crs and same_transform):
            raise ValueError(
                f"APCP_STEP_RESOLUTION cumulative grid mismatch for fh{step_fh:03d}: "
                f"shape_match={same_shape} crs_match={same_crs} transform_match={same_transform}"
            )
        step_apcp_data = np.clip(
            apcp_cum_clean - cum_diff_state.consumed_sum, 0.0, None,
        ).astype(np.float32, copy=False)
        if cum_diff_state.consumed_sum_valid is not None:
            apcp_valid = apcp_valid_raw & cum_diff_state.consumed_sum_valid
        fallback_differencing_applied = True
        logger.info(
            'APCP_STEP_FALLBACK step_fh=%d prev_fh=%d reason="cumulative 0-%d"',
            step_fh,
            cum_diff_state.consumed_through_fh,
            step_fh,
        )
    elif apcp_mode == "overlap_window":
        assert window is not None
        bucket_state_available = (
            cum_diff_state.bucket_start_fh is not None
            and int(cum_diff_state.bucket_start_fh) == int(window[0])
            and int(cum_diff_state.bucket_through_fh) == int(expected_start_fh)
            and cum_diff_state.bucket_cumulative_sum is not None
        )
        if bucket_state_available:
            same_shape = apcp_cum_clean.shape == cum_diff_state.bucket_cumulative_sum.shape
            same_crs = step_crs == cum_diff_state.bucket_cumulative_crs
            same_transform = step_transform == cum_diff_state.bucket_cumulative_transform
            if not (same_shape and same_crs and same_transform):
                raise ValueError(
                    f"APCP_STEP_RESOLUTION overlap grid mismatch for fh{step_fh:03d}: "
                    f"shape_match={same_shape} crs_match={same_crs} transform_match={same_transform}"
                )
            step_apcp_data = np.clip(
                apcp_cum_clean - cum_diff_state.bucket_cumulative_sum,
                0.0,
                None,
            ).astype(np.float32, copy=False)
            if cum_diff_state.bucket_cumulative_valid is not None:
                apcp_valid = apcp_valid_raw & cum_diff_state.bucket_cumulative_valid
            fallback_differencing_applied = True
            logger.info(
                'APCP_STEP_FALLBACK step_fh=%d prev_fh=%d reason="overlap %s"',
                step_fh,
                cum_diff_state.bucket_through_fh,
                selected_window,
            )
        else:
            reconstructed = _reconstruct_overlap_prior_sum(
                cum_diff_state=cum_diff_state,
                start_fh=int(window[0]),
                through_fh=int(expected_start_fh),
            )
            if reconstructed is not None:
                prior_sum, prior_valid, prior_crs, prior_transform = reconstructed
                same_shape = apcp_cum_clean.shape == prior_sum.shape
                same_crs = step_crs == prior_crs
                same_transform = step_transform == prior_transform
                if not (same_shape and same_crs and same_transform):
                    raise ValueError(
                        f"APCP_STEP_RESOLUTION reconstructed overlap grid mismatch for fh{step_fh:03d}: "
                        f"shape_match={same_shape} crs_match={same_crs} transform_match={same_transform}"
                    )
                step_apcp_data = np.clip(
                    apcp_cum_clean - prior_sum,
                    0.0,
                    None,
                ).astype(np.float32, copy=False)
                apcp_valid = apcp_valid_raw & prior_valid
                fallback_differencing_applied = True
                logger.info(
                    'APCP_STEP_FALLBACK step_fh=%d prev_fh=%d reason="overlap_reconstructed %s"',
                    step_fh,
                    expected_start_fh,
                    selected_window,
                )
            elif cum_diff_state.consumed_through_fh <= 0:
                selector_reason = "history_gap_overlap_rebuild"
            else:
                raise ValueError(
                    f"APCP_STEP_RESOLUTION overlap state missing for fh{step_fh:03d}: "
                    f"expected_start={expected_start_fh} selected_window={selected_window}"
                )

    log_mode = {
        "exact_step": "step",
        "cumulative_from_zero": "cumulative",
        "overlap_window": "overlap",
        "invalid": "invalid",
    }.get(apcp_mode, apcp_mode)

    logger.info(
        'APCP_STEP_RESOLUTION step_fh=%d product=%s inv="%s" mode=%s fallback=%s '
        'exact_guess_used=%s inventory_selected=%s selected_window="%s" selector_fallback=%s '
        'reason="%s" pattern="%s"',
        step_fh,
        apcp_product or product,
        apcp_inventory_line.replace('"', "'"),
        log_mode,
        "true" if fallback_differencing_applied else "false",
        "true" if exact_guess_used else "false",
        "true" if inventory_selected else "false",
        selected_window,
        "true" if selector_fallback_used else "false",
        selector_reason.replace('"', "'"),
        apcp_search_pattern.replace('"', "'"),
    )

    # 5. Advance consumed-sum tracking for all modes.
    increment_for_sum = np.where(apcp_valid, step_apcp_data, 0.0).astype(np.float32, copy=False)
    if cum_diff_state.consumed_sum is None:
        cum_diff_state.consumed_sum = increment_for_sum.copy()
        cum_diff_state.consumed_sum_valid = apcp_valid.copy()
        cum_diff_state.consumed_sum_crs = step_crs
        cum_diff_state.consumed_sum_transform = step_transform
    else:
        same_shape = increment_for_sum.shape == cum_diff_state.consumed_sum.shape
        same_crs = step_crs == cum_diff_state.consumed_sum_crs
        same_transform = step_transform == cum_diff_state.consumed_sum_transform
        if not (same_shape and same_crs and same_transform):
            raise ValueError(
                f"APCP_STEP_RESOLUTION consumed-sum grid mismatch for fh{step_fh:03d}: "
                f"shape_match={same_shape} crs_match={same_crs} transform_match={same_transform}"
            )
        cum_diff_state.consumed_sum = (
            cum_diff_state.consumed_sum + increment_for_sum
        ).astype(np.float32, copy=False)
        if cum_diff_state.consumed_sum_valid is not None:
            cum_diff_state.consumed_sum_valid = cum_diff_state.consumed_sum_valid & apcp_valid
        else:
            cum_diff_state.consumed_sum_valid = apcp_valid.copy()

    if window is not None and int(window[0]) > 0 and apcp_mode in {"exact_step", "overlap_window"}:
        start_hour = int(window[0])
        cum_diff_state.bucket_start_fh = start_hour
        cum_diff_state.bucket_cumulative_sum = apcp_cum_clean.copy()
        cum_diff_state.bucket_cumulative_valid = apcp_valid_raw.copy()
        cum_diff_state.bucket_cumulative_crs = step_crs
        cum_diff_state.bucket_cumulative_transform = step_transform
        cum_diff_state.bucket_through_fh = int(step_fh)
    elif apcp_mode == "cumulative_from_zero":
        cum_diff_state.bucket_start_fh = None
        cum_diff_state.bucket_cumulative_sum = None
        cum_diff_state.bucket_cumulative_valid = None
        cum_diff_state.bucket_cumulative_crs = None
        cum_diff_state.bucket_cumulative_transform = None
        cum_diff_state.bucket_through_fh = int(step_fh)

    if apcp_mode in {"exact_step", "overlap_window"}:
        cum_diff_state.recent_exact_steps[int(step_fh)] = (
            increment_for_sum.copy(),
            apcp_valid.copy(),
            step_crs,
            step_transform,
        )
        prune_before_fh = int(step_fh) - 12
        for prior_fh in list(cum_diff_state.recent_exact_steps.keys()):
            if int(prior_fh) < prune_before_fh:
                cum_diff_state.recent_exact_steps.pop(int(prior_fh), None)

    cum_diff_state.consumed_through_fh = int(step_fh)
    assert step_crs is not None  # guaranteed set by steps 1/2/3 above
    assert step_transform is not None  # guaranteed set by steps 1/2/3 above
    return step_apcp_data, apcp_valid, step_crs, step_transform, apcp_mode


def _cumulative_apcp_loop(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    step_fhs: list[int],
    model_plugin: Any,
    ctx: FetchContext | None,
    apcp_component: str,
    apcp_product: str | None,
    use_warped: bool,
    target_region: str,
    target_grid_id: str,
    resampling: str,
    use_inventory_resolution: bool,
    process_step: Callable[
        [int, np.ndarray, "np.ndarray | None", rasterio.crs.CRS, rasterio.transform.Affine],
        tuple[np.ndarray, np.ndarray],
    ],
    error_label: str,
    first_step_expected_start_fh: int | None = None,
    initial_apcp_cumulative: tuple[
        np.ndarray,
        np.ndarray,
        rasterio.crs.CRS,
        rasterio.transform.Affine,
        int,
    ] | None = None,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine, bool]:
    """Shared cumulative APCP accumulation loop.

    For each forecast step:
      1. Fetch APCP via simple fetch or inventory-driven resolution.
      2. Call *process_step(step_fh, step_data, apcp_valid, crs, transform)*
         which returns ``(contribution, step_valid)``.
      3. Accumulate *contribution*, merge *step_valid*.

    *process_step* receives ``apcp_valid=None`` for the simple fetch path
    (the callback determines validity from raw data) and a boolean mask for
    the inventory path (pre-cleaned, post-differencing).

    Returns ``(cumulative, crs, transform, cumulative_fallback_used)``
    with NaN at invalid pixels.
    """
    cum_diff_state = _ApcpCumDiffState() if use_inventory_resolution else None
    if use_inventory_resolution and cum_diff_state is not None and initial_apcp_cumulative is not None:
        (
            seed_data,
            seed_valid,
            seed_crs,
            seed_transform,
            seed_fh,
        ) = initial_apcp_cumulative
        cum_diff_state.consumed_sum = np.asarray(seed_data, dtype=np.float32)
        cum_diff_state.consumed_sum_valid = np.asarray(seed_valid, dtype=bool)
        cum_diff_state.consumed_sum_crs = seed_crs
        cum_diff_state.consumed_sum_transform = seed_transform
        cum_diff_state.consumed_through_fh = int(seed_fh)

    cumulative: np.ndarray | None = None
    valid_mask: np.ndarray | None = None
    src_crs: rasterio.crs.CRS | None = None
    src_transform: rasterio.transform.Affine | None = None
    cumulative_fallback_used = False

    for step_index, step_fh in enumerate(step_fhs):
        if use_inventory_resolution and cum_diff_state is not None:
            step_data, apcp_valid, step_crs, step_transform, step_apcp_mode = _resolve_apcp_step_data(
                step_fh=step_fh,
                step_index=step_index,
                step_fhs=step_fhs,
                model_id=model_id,
                product=product,
                run_date=run_date,
                model_plugin=model_plugin,
                ctx=ctx,
                apcp_component=apcp_component,
                apcp_product=apcp_product,
                use_warped=use_warped,
                target_region=target_region,
                target_grid_id=target_grid_id,
                resampling=resampling,
                cum_diff_state=cum_diff_state,
                expected_start_fh_override=(first_step_expected_start_fh if step_index == 0 else None),
            )
            cumulative_fallback_used = cumulative_fallback_used or step_apcp_mode != "exact_step"
        else:
            step_data, step_crs, step_transform = _fetch_step_component(
                model_id=model_id,
                product=str(apcp_product or product),
                run_date=run_date,
                step_fh=step_fh,
                model_plugin=model_plugin,
                var_key=apcp_component,
                use_warped=use_warped,
                target_region=target_region,
                target_grid_id=target_grid_id,
                resampling=resampling,
                ctx=ctx,
            )
            apcp_valid = None

        contribution, step_valid = process_step(
            step_fh, step_data, apcp_valid, step_crs, step_transform,
        )

        if cumulative is None:
            cumulative = contribution
            valid_mask = step_valid
            src_crs = step_crs
            src_transform = step_transform
            continue

        if contribution.shape != cumulative.shape:
            raise ValueError(
                f"{error_label} shape mismatch at fh{step_fh:03d}: "
                f"{contribution.shape} != {cumulative.shape}"
            )

        cumulative = cumulative + contribution
        valid_mask = np.logical_or(valid_mask, step_valid)  # type: ignore[arg-type]

    if cumulative is None or valid_mask is None or src_crs is None or src_transform is None:
        raise ValueError(error_label)

    cumulative = np.where(valid_mask, cumulative, np.nan).astype(np.float32)
    return cumulative, src_crs, src_transform, cumulative_fallback_used


def _interval_sample_fhs(step_fh: int, step_len: int, *, sample_mode: str = "auto") -> list[int]:
    if step_len <= 0:
        raise ValueError(f"Invalid cumulative step length={step_len} for fh={step_fh}")
    start_fh = step_fh - step_len
    normalized_sample_mode = str(sample_mode).strip().lower()
    if normalized_sample_mode == "three_point":
        mid_offset = max(1, step_len // 2)
        mid_fh = start_fh + mid_offset
        candidates = [start_fh, mid_fh, step_fh]
    elif step_len == 3:
        candidates = [start_fh, step_fh]
    else:
        mid_fh = step_fh - (step_len // 2)
        candidates = [start_fh, mid_fh, step_fh]

    sample_fhs: list[int] = []
    for sample_fh in candidates:
        if sample_fh in sample_fhs:
            continue
        sample_fhs.append(sample_fh)
    return sample_fhs


def _filter_sample_fhs_to_available_steps(
    sample_fhs: list[int],
    *,
    available_fhs: set[int] | None,
) -> list[int]:
    if not available_fhs:
        return sample_fhs

    filtered: list[int] = []
    for sample_fh in sample_fhs:
        if sample_fh not in available_fhs:
            continue
        if sample_fh in filtered:
            continue
        filtered.append(sample_fh)

    return filtered if filtered else sample_fhs


def _log_missing_csnow_sample(
    *,
    model_id: str,
    var_key: str,
    step_fh: int,
    sample_fh: int,
    exc: Exception,
) -> None:
    global _MISSING_CSNOW_SAMPLE_LOG_COUNT
    _MISSING_CSNOW_SAMPLE_LOG_COUNT += 1
    count = _MISSING_CSNOW_SAMPLE_LOG_COUNT
    if count <= 5 or count % 25 == 0:
        logger.debug(
            "Skipping unavailable csnow sample for %s/%s at step fh%03d sample fh%03d (%s); missing_count=%d",
            model_id,
            var_key,
            step_fh,
            sample_fh,
            exc.__class__.__name__,
            count,
        )


def _neighbor_count_3x3(mask: np.ndarray) -> np.ndarray:
    """Return count of True values in each 3x3 neighborhood (including center)."""
    padded = np.pad(mask.astype(np.uint8, copy=False), 1, mode="constant", constant_values=0)
    return (
        padded[:-2, :-2]
        + padded[:-2, 1:-1]
        + padded[:-2, 2:]
        + padded[1:-1, :-2]
        + padded[1:-1, 1:-1]
        + padded[1:-1, 2:]
        + padded[2:, :-2]
        + padded[2:, 1:-1]
        + padded[2:, 2:]
    ).astype(np.uint8, copy=False)


def _derive_wspd10m(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None,
    model_plugin: Any,
    ctx: FetchContext | None = None,
    derive_component_target_grid: dict[str, str] | None = None,
    derive_component_resampling: str | None = None,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    del derive_component_target_grid, derive_component_resampling
    hints = getattr(getattr(var_spec_model, "selectors", None), "hints", {})
    u_component = hints.get("u_component", "10u")
    v_component = hints.get("v_component", "10v")
    speed_component = hints.get("speed_component")

    # Prefer a direct wind-speed field when available.
    if speed_component:
        try:
            logger.info(
                "wspd10m derive path (model=%s): trying direct speed component=%s",
                model_id,
                speed_component,
            )
            speed_data, src_crs, src_transform = _fetch_component(
                model_id=model_id,
                product=product,
                run_date=run_date,
                fh=fh,
                model_plugin=model_plugin,
                var_key=str(speed_component),
                ctx=ctx,
            )
            wspd = convert_units(
                speed_data.astype(np.float32, copy=False),
                var_key=var_key,
                model_id=model_id,
                var_capability=var_capability,
            )
            return wspd.astype(np.float32, copy=False), src_crs, src_transform
        except (HerbieTransientUnavailableError, RuntimeError, ValueError):
            # Fall back to vector magnitude from 10u/10v.
            pass

    try:
        u_data, src_crs, src_transform = _fetch_component(
            model_id=model_id,
            product=product,
            run_date=run_date,
            fh=fh,
            model_plugin=model_plugin,
            var_key=u_component,
            ctx=ctx,
        )
        v_data, _, _ = _fetch_component(
            model_id=model_id,
            product=product,
            run_date=run_date,
            fh=fh,
            model_plugin=model_plugin,
            var_key=v_component,
            ctx=ctx,
        )
    except (HerbieTransientUnavailableError, RuntimeError, ValueError):
        if not speed_component:
            raise
        raise

    wspd_ms = np.hypot(u_data, v_data, dtype=np.float32)
    wspd = convert_units(
        wspd_ms,
        var_key=var_key,
        model_id=model_id,
        var_capability=var_capability,
    )
    return wspd.astype(np.float32, copy=False), src_crs, src_transform


def _derive_radar_ptype_combo(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None,
    model_plugin: Any,
    ctx: FetchContext | None = None,
    derive_component_target_grid: dict[str, str] | None = None,
    derive_component_resampling: str | None = None,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    del var_key, var_capability, derive_component_target_grid, derive_component_resampling
    hints = getattr(getattr(var_spec_model, "selectors", None), "hints", {})
    try:
        min_visible_dbz = float(hints.get("min_visible_dbz", "10.0"))
    except (TypeError, ValueError):
        min_visible_dbz = 10.0
    try:
        min_mask_value = float(hints.get("min_mask_value", "0.0"))
    except (TypeError, ValueError):
        min_mask_value = 0.0
    try:
        despeckle_min_neighbors = int(hints.get("despeckle_min_neighbors", "1"))
    except (TypeError, ValueError):
        despeckle_min_neighbors = 1
    despeckle_min_neighbors = min(max(despeckle_min_neighbors, 1), 9)

    refl_id = hints.get("refl_component", "refc")
    rain_id = hints.get("rain_component", "crain")
    snow_id = hints.get("snow_component", "csnow")
    sleet_id = hints.get("sleet_component", "cicep")
    frzr_id = hints.get("frzr_component", "cfrzr")

    refl, src_crs, src_transform = _fetch_component(
        model_id=model_id,
        product=product,
        run_date=run_date,
        fh=fh,
        model_plugin=model_plugin,
        var_key=refl_id,
        ctx=ctx,
    )
    rain, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=rain_id, ctx=ctx)
    snow, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=snow_id, ctx=ctx)
    sleet, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=sleet_id, ctx=ctx)
    frzr, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=frzr_id, ctx=ctx)

    mask_stack = np.stack([rain, snow, sleet, frzr], axis=0).astype(np.float32, copy=False)
    mask_max = np.nanmax(mask_stack, axis=0)
    ptype_idx = np.argmax(mask_stack, axis=0).astype(np.int32)
    ptype_codes = np.array(RADAR_PTYPE_ORDER)
    ptype = ptype_codes[ptype_idx]

    rain_mask = mask_stack[0]
    snow_mask = mask_stack[1]
    frzr_transition = (ptype == "frzr") & ((rain_mask > 0) | (snow_mask > 0))
    if np.any(frzr_transition):
        prefer_rain = rain_mask >= snow_mask
        ptype[frzr_transition & prefer_rain] = "rain"
        ptype[frzr_transition & ~prefer_rain] = "snow"

    refl_safe = np.where(np.isfinite(refl), np.maximum(refl, 0.0), np.nan)
    bins_per_type = {k: int(v["count"]) for k, v in RADAR_PTYPE_BREAKS.items()}
    normalized = np.clip(refl_safe / 70.0, 0.0, 1.0)

    indexed = np.full(refl.shape, np.nan, dtype=np.float32)
    for code in RADAR_PTYPE_ORDER:
        breaks = RADAR_PTYPE_BREAKS[code]
        offset = int(breaks["offset"])
        count = bins_per_type[code]
        local_bin = np.clip(np.rint(normalized * (count - 1)), 0, count - 1).astype(np.int32)
        selector = (
            (ptype == code)
            & np.isfinite(refl_safe)
            & (mask_max >= min_mask_value)
            & (refl_safe >= min_visible_dbz)
        )
        indexed[selector] = (offset + local_bin[selector]).astype(np.float32)

    if despeckle_min_neighbors > 1:
        valid = np.isfinite(indexed)
        if np.any(valid):
            neighbor_count = _neighbor_count_3x3(valid)
            indexed = np.where(neighbor_count >= despeckle_min_neighbors, indexed, np.nan).astype(np.float32, copy=False)

    return indexed, src_crs, src_transform


def _derive_precip_ptype_blend(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None,
    model_plugin: Any,
    ctx: FetchContext | None = None,
    derive_component_target_grid: dict[str, str] | None = None,
    derive_component_resampling: str | None = None,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    del var_key, var_capability, derive_component_target_grid, derive_component_resampling
    hints = getattr(getattr(var_spec_model, "selectors", None), "hints", {})
    prate_id = hints.get("prate_component", "prate")
    rain_id = hints.get("rain_component", "crain")
    snow_id = hints.get("snow_component", "csnow")
    sleet_id = hints.get("sleet_component", "cicep")
    frzr_id = hints.get("frzr_component", "cfrzr")

    prate, src_crs, src_transform = _fetch_component(
        model_id=model_id,
        product=product,
        run_date=run_date,
        fh=fh,
        model_plugin=model_plugin,
        var_key=prate_id,
        ctx=ctx,
    )
    rain, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=rain_id, ctx=ctx)
    snow, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=snow_id, ctx=ctx)
    sleet, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=sleet_id, ctx=ctx)
    frzr, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=frzr_id, ctx=ctx)

    # GFS PRATE is typically kg m^-2 s^-1 (equivalent to mm/s) → in/hr.
    prate_inhr = np.where(
        np.isfinite(prate),
        np.maximum(prate, 0.0) * 3600.0 * 0.03937007874015748,
        np.nan,
    ).astype(np.float32)

    # Breaks order is authoritative for flattened palette indexing.
    mask_by_code = {
        "rain": rain,
        "snow": snow,
        "sleet": sleet,
        "frzr": frzr,
    }
    stack_for_pick = np.stack([mask_by_code[c] for c in PRECIP_PTYPE_ORDER], axis=0).astype(np.float32, copy=False)
    mask_max = np.nanmax(stack_for_pick, axis=0)
    ptype_idx = np.argmax(stack_for_pick, axis=0).astype(np.int32)
    ptype_codes = np.array(PRECIP_PTYPE_ORDER)
    ptype = ptype_codes[ptype_idx]

    range_min, range_max = float(PRECIP_PTYPE_RANGE[0]), float(PRECIP_PTYPE_RANGE[1])
    normalized = np.clip((prate_inhr - range_min) / max(range_max - range_min, 1e-6), 0.0, 1.0)

    indexed = np.full(prate_inhr.shape, np.nan, dtype=np.float32)
    for code in PRECIP_PTYPE_ORDER:
        breaks = PRECIP_PTYPE_BREAKS[code]
        offset = int(breaks["offset"])
        count = int(breaks["count"])
        # Keep bins anchored to the configured per-type count.
        if count != PRECIP_PTYPE_BINS_PER_TYPE:
            count = PRECIP_PTYPE_BINS_PER_TYPE
        local_bin = np.clip(np.rint(normalized * (count - 1)), 0, count - 1).astype(np.int32)
        selector = (ptype == code) & np.isfinite(prate_inhr) & (prate_inhr > 0.0) & (mask_max > 0)
        indexed[selector] = (offset + local_bin[selector]).astype(np.float32)

    return indexed, src_crs, src_transform


def _derive_precip_total_cumulative(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None,
    model_plugin: Any,
    ctx: FetchContext | None = None,
    derive_component_target_grid: dict[str, str] | None = None,
    derive_component_resampling: str | None = None,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    hints = getattr(getattr(var_spec_model, "selectors", None), "hints", {})
    apcp_component = hints.get("apcp_component", "apcp_step")
    use_inventory_resolution = (
        str(var_key).strip().lower() == "precip_total"
        and str(apcp_component).strip() == "apcp_step"
    )
    step_fhs = _resolve_cumulative_step_fhs(hints=hints, fh=fh, run_date=run_date, default_step_hours=6)
    cadence_hint = _cadence_hint_suffix(hints)
    logger.info("derive %s fh%03d apcp_steps=%d%s", var_key, fh, len(step_fhs), cadence_hint)
    logger.debug("derive %s fh%03d apcp_steps=%s", var_key, fh, step_fhs)

    use_warped, target_region, target_grid_id, resampling = _resolve_warped_state(
        derive_component_target_grid, derive_component_resampling, model_id,
    )

    if not use_inventory_resolution:
        _prefetch_components_parallel(
            [
                _PrefetchTask(
                    model_id=model_id, product=product, run_date=run_date,
                    fh=sfh, model_plugin=model_plugin, var_key=apcp_component,
                    warped=use_warped, target_region=target_region,
                    target_grid_id=target_grid_id, resampling=resampling,
                )
                for sfh in step_fhs
            ],
            ctx,
            label=f"precip_total fh{fh:03d}",
        )

    def _process_step(
        step_fh: int,
        step_data: np.ndarray,
        apcp_valid_hint: np.ndarray | None,
        step_crs: rasterio.crs.CRS,
        step_transform: rasterio.transform.Affine,
    ) -> tuple[np.ndarray, np.ndarray]:
        del step_fh, step_crs, step_transform
        step_clean = np.where(
            np.isfinite(step_data), np.maximum(step_data, 0.0), 0.0,
        ).astype(np.float32)
        if apcp_valid_hint is None:
            step_valid = np.isfinite(step_data)
        else:
            step_valid = np.asarray(apcp_valid_hint, dtype=bool)
        return step_clean, step_valid

    cumulative_kgm2, src_crs, src_transform, _ = _cumulative_apcp_loop(
        model_id=model_id,
        var_key=var_key,
        product=product,
        run_date=run_date,
        fh=fh,
        step_fhs=step_fhs,
        model_plugin=model_plugin,
        ctx=ctx,
        apcp_component=apcp_component,
        apcp_product=None,
        use_warped=use_warped,
        target_region=target_region,
        target_grid_id=target_grid_id,
        resampling=resampling,
        use_inventory_resolution=use_inventory_resolution,
        process_step=_process_step,
        error_label=f"No cumulative APCP source steps resolved for {model_id}/{var_key} fh{fh:03d}",
    )

    cumulative_inches = convert_units(
        cumulative_kgm2,
        var_key=var_key,
        model_id=model_id,
        var_capability=var_capability,
    )
    return cumulative_inches.astype(np.float32, copy=False), src_crs, src_transform


def _derive_snowfall_total_10to1_cumulative(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None,
    model_plugin: Any,
    ctx: FetchContext | None = None,
    derive_component_target_grid: dict[str, str] | None = None,
    derive_component_resampling: str | None = None,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    del var_capability
    frame_start = time.perf_counter()
    hints = getattr(getattr(var_spec_model, "selectors", None), "hints", {})
    apcp_component = hints.get("apcp_component", "apcp_step")
    snow_component = hints.get("snow_component", "csnow")
    slr_raw = hints.get("slr", "10")
    snow_mask_threshold_raw = hints.get("snow_mask_threshold")
    snow_interval_sample_mode = str(hints.get("snow_interval_sample_mode", "auto")).strip().lower() or "auto"
    min_step_lwe_raw = hints.get("min_step_lwe_kgm2", "0.01")

    try:
        slr = float(slr_raw)
    except (TypeError, ValueError):
        slr = 10.0
    if slr <= 0.0:
        slr = 10.0

    snow_mask_threshold: float | None = None
    if snow_mask_threshold_raw is not None:
        try:
            parsed_threshold = float(snow_mask_threshold_raw)
        except (TypeError, ValueError):
            parsed_threshold = 0.5
        snow_mask_threshold = min(max(parsed_threshold, 0.0), 1.0)

    try:
        min_step_lwe = float(min_step_lwe_raw)
    except (TypeError, ValueError):
        min_step_lwe = 0.01
    min_step_lwe = max(min_step_lwe, 0.0)

    use_inventory_resolution = (
        str(model_id).strip().lower() in {"gfs", "nam"}
        and str(apcp_component).strip() == "apcp_step"
    )
    cadence_sample_fhs: set[int] | None = None
    snow_inches_scale = 0.03937007874015748 * slr

    step_fhs = _resolve_cumulative_step_fhs(hints=hints, fh=fh, run_date=run_date, default_step_hours=6)
    if str(model_id).strip().lower() == "gfs" and snow_interval_sample_mode == "three_point":
        cadence_sample_fhs = {0, *[int(step_fh) for step_fh in step_fhs]}
    # Build interval plan: step_fh → (step_len, sample_fhs).
    interval_plan: dict[int, tuple[int, list[int]]] = {}
    snow_step_fhs: list[int] = []
    prev_step_fh = 0
    for step_fh in step_fhs:
        step_len = step_fh - prev_step_fh
        prev_step_fh = step_fh
        if step_len <= 0:
            raise ValueError(
                f"Non-increasing cumulative snowfall step sequence for {model_id}/{var_key}: "
                f"step_len={step_len} at fh{step_fh:03d}"
            )
        sample_fhs = [
            sf
            for sf in _interval_sample_fhs(
                step_fh,
                step_len,
                sample_mode=snow_interval_sample_mode,
            )
            if sf >= 0
        ]
        sample_fhs = _filter_sample_fhs_to_available_steps(
            sample_fhs,
            available_fhs=cadence_sample_fhs,
        )
        interval_plan[step_fh] = (step_len, sample_fhs)
        for sf in sample_fhs:
            if sf not in snow_step_fhs:
                snow_step_fhs.append(sf)

    active_step_fhs = list(step_fhs)
    active_snow_step_fhs = list(snow_step_fhs)
    reused_prev_cumulative = False
    base_fh: int | None = None
    base_cumulative_kgm2: np.ndarray | None = None
    base_crs: rasterio.crs.CRS | None = None
    base_transform: rasterio.transform.Affine | None = None
    first_step_expected_start_fh: int | None = None
    initial_apcp_cumulative: tuple[
        np.ndarray,
        np.ndarray,
        rasterio.crs.CRS,
        rasterio.transform.Affine,
        int,
    ] | None = None
    current_step_fetch_counts: dict[str, int] = {"apcp": 0, "csnow": 0}

    if len(step_fhs) >= 2:
        prev_fh = int(step_fhs[-2])
        prior_snowfall = _kuchera_load_prior_cumulative(
            model_id=model_id,
            run_date=run_date,
            var_key=var_key,
            fh=prev_fh,
            ctx=ctx,
            scale_divisor=snow_inches_scale,
        )
        prior_precip = _kuchera_load_prior_cumulative(
            model_id=model_id,
            run_date=run_date,
            var_key="precip_total",
            fh=prev_fh,
            ctx=ctx,
            scale_divisor=0.03937007874015748,
        )
        if prior_snowfall is not None and prior_precip is not None:
            prior_snowfall_data, prior_snowfall_crs, prior_snowfall_transform = prior_snowfall
            prior_precip_data, prior_precip_crs, prior_precip_transform = prior_precip
            same_shape = prior_snowfall_data.shape == prior_precip_data.shape
            same_crs = prior_snowfall_crs == prior_precip_crs
            same_transform = prior_snowfall_transform == prior_precip_transform
            if same_shape and same_crs and same_transform:
                active_step_fhs = [int(step_fhs[-1])]
                active_snow_step_fhs = list(interval_plan[int(step_fhs[-1])][1])
                reused_prev_cumulative = True
                base_fh = prev_fh
                base_cumulative_kgm2 = prior_snowfall_data.astype(np.float32, copy=False)
                base_crs = prior_snowfall_crs
                base_transform = prior_snowfall_transform
                first_step_expected_start_fh = prev_fh
                initial_apcp_cumulative = (
                    prior_precip_data.astype(np.float32, copy=False),
                    np.isfinite(prior_precip_data),
                    prior_precip_crs,
                    prior_precip_transform,
                    prev_fh,
                )

    logger.info("snow_ratio method=10to1 fh=%d", fh)
    logger.info(
        "derive %s fh%03d apcp_steps=%d snow_steps=%d%s",
        var_key, fh, len(step_fhs), len(snow_step_fhs),
        _cadence_hint_suffix(hints),
    )
    logger.debug("derive %s fh%03d apcp_steps=%s snow_steps=%s", var_key, fh, step_fhs, snow_step_fhs)

    use_warped, target_region, target_grid_id, resampling = _resolve_warped_state(
        derive_component_target_grid, derive_component_resampling, model_id,
    )

    # Prefetch APCP + csnow in parallel.
    _prefetch_tasks: list[_PrefetchTask] = []
    if not use_inventory_resolution:
        for _pf_fh in active_step_fhs:
            _prefetch_tasks.append(_PrefetchTask(
                model_id=model_id, product=product, run_date=run_date,
                fh=_pf_fh, model_plugin=model_plugin, var_key=apcp_component,
                warped=use_warped, target_region=target_region,
                target_grid_id=target_grid_id, resampling=resampling,
            ))
    for _pf_fh in active_snow_step_fhs:
        _prefetch_tasks.append(_PrefetchTask(
            model_id=model_id, product=product, run_date=run_date,
            fh=_pf_fh, model_plugin=model_plugin, var_key=snow_component,
            warped=use_warped, target_region=target_region,
            target_grid_id=target_grid_id, resampling=resampling,
        ))
    _prefetch_components_parallel(_prefetch_tasks, ctx, label=f"snow10to1 fh{fh:03d}")
    del _prefetch_tasks

    def _process_step(
        step_fh: int,
        step_data: np.ndarray,
        apcp_valid_hint: np.ndarray | None,
        step_crs: rasterio.crs.CRS,
        step_transform: rasterio.transform.Affine,
    ) -> tuple[np.ndarray, np.ndarray]:
        if int(step_fh) == int(fh):
            current_step_fetch_counts["apcp"] = int(current_step_fetch_counts.get("apcp", 0)) + 1
        if apcp_valid_hint is None:
            apcp_valid = np.isfinite(step_data) & (step_data >= 0.0)
        else:
            apcp_valid = np.asarray(apcp_valid_hint, dtype=bool)
        step_apcp_clean = np.where(apcp_valid, step_data, 0.0).astype(np.float32, copy=False)
        if min_step_lwe > 0.0:
            step_apcp_clean = np.where(
                step_apcp_clean >= min_step_lwe, step_apcp_clean, 0.0,
            ).astype(np.float32, copy=False)

        _step_len, sample_fhs = interval_plan[step_fh]
        sample_masks: list[np.ndarray] = []
        for sample_fh in sample_fhs:
            if int(step_fh) == int(fh):
                current_step_fetch_counts["csnow"] = int(current_step_fetch_counts.get("csnow", 0)) + 1
            try:
                snow_mask, _, _ = _fetch_step_component(
                    model_id=model_id, product=product, run_date=run_date,
                    step_fh=sample_fh, model_plugin=model_plugin,
                    var_key=snow_component,
                    use_warped=use_warped, target_region=target_region,
                    target_grid_id=target_grid_id, resampling=resampling,
                    ctx=ctx,
                )
            except (HerbieTransientUnavailableError, RuntimeError, ValueError) as exc:
                _log_missing_csnow_sample(
                    model_id=model_id, var_key=var_key,
                    step_fh=step_fh, sample_fh=sample_fh, exc=exc,
                )
                continue

            if snow_mask.shape != step_apcp_clean.shape:
                raise ValueError(
                    f"Snowfall mask shape mismatch for {model_id}/{var_key} at fh{sample_fh:03d}: "
                    f"{snow_mask.shape} != {step_apcp_clean.shape}"
                )
            snow_valid = np.isfinite(snow_mask) & (snow_mask >= 0.0) & (snow_mask <= 1.0)
            sample_masks.append(
                np.where(snow_valid, snow_mask, np.nan).astype(np.float32, copy=False)
            )

        if sample_masks:
            sample_stack = np.stack(sample_masks, axis=0).astype(np.float32, copy=False)
            sample_valid_counts = np.sum(np.isfinite(sample_stack), axis=0).astype(np.int32, copy=False)
            sample_sum = np.nansum(sample_stack, axis=0).astype(np.float32, copy=False)
            interval_mask = np.zeros(step_apcp_clean.shape, dtype=np.float32)
            np.divide(
                sample_sum,
                sample_valid_counts.astype(np.float32, copy=False),
                out=interval_mask,
                where=sample_valid_counts > 0,
            )
            interval_mask = np.clip(interval_mask, 0.0, 1.0).astype(np.float32, copy=False)
            if snow_mask_threshold is not None:
                interval_mask = np.where(
                    interval_mask >= np.float32(snow_mask_threshold),
                    np.float32(1.0),
                    np.float32(0.0),
                ).astype(np.float32, copy=False)
            csnow_valid = sample_valid_counts > 0
        else:
            interval_mask = np.zeros(step_apcp_clean.shape, dtype=np.float32)
            csnow_valid = np.zeros(step_apcp_clean.shape, dtype=bool)

        step_snow_kgm2 = (step_apcp_clean * interval_mask).astype(np.float32, copy=False)
        step_valid = apcp_valid & csnow_valid
        return step_snow_kgm2, step_valid

    cumulative_kgm2, src_crs, src_transform, _ = _cumulative_apcp_loop(
        model_id=model_id,
        var_key=var_key,
        product=product,
        run_date=run_date,
        fh=fh,
        step_fhs=active_step_fhs,
        model_plugin=model_plugin,
        ctx=ctx,
        apcp_component=apcp_component,
        apcp_product=None,
        use_warped=use_warped,
        target_region=target_region,
        target_grid_id=target_grid_id,
        resampling=resampling,
        use_inventory_resolution=use_inventory_resolution,
        process_step=_process_step,
        error_label=f"No cumulative snowfall source steps resolved for {model_id}/{var_key} fh{fh:03d}",
        first_step_expected_start_fh=first_step_expected_start_fh,
        initial_apcp_cumulative=initial_apcp_cumulative,
    )

    if base_cumulative_kgm2 is not None and base_crs is not None and base_transform is not None:
        shape_match = base_cumulative_kgm2.shape == cumulative_kgm2.shape
        crs_match = base_crs == src_crs
        transform_match = base_transform == src_transform
        if not (shape_match and crs_match and transform_match):
            raise ValueError(
                f"Snowfall incremental base-grid mismatch for {model_id}/{var_key} fh{fh:03d}: "
                f"shape_match={shape_match} crs_match={crs_match} transform_match={transform_match}"
            )
        base_valid = np.isfinite(base_cumulative_kgm2)
        base_clean = np.where(base_valid, base_cumulative_kgm2, 0.0).astype(np.float32, copy=False)
        current_valid = np.isfinite(cumulative_kgm2)
        current_clean = np.where(current_valid, cumulative_kgm2, 0.0).astype(np.float32, copy=False)
        cumulative_kgm2 = (base_clean + current_clean).astype(np.float32, copy=False)
        cumulative_kgm2 = np.where(base_valid | current_valid, cumulative_kgm2, np.nan).astype(np.float32, copy=False)

    # 1 kg/m^2 == 1 mm LWE. Convert to inches liquid then apply fixed 10:1 SLR.
    cumulative_snow_inches = cumulative_kgm2 * 0.03937007874015748 * slr
    logger.info(
        "snow10to1_incremental model=%s run=%s fh=%03d total_steps=%d computed_steps=%d reused_prev_cumulative=%s "
        "base_fh=%s final_step_samples=%d current_step_fetches=%s compute_ms=%d",
        model_id,
        _run_id_from_date(run_date),
        fh,
        len(step_fhs),
        len(active_step_fhs),
        "true" if reused_prev_cumulative else "false",
        f"{base_fh:03d}" if base_fh is not None else "none",
        len(interval_plan.get(int(fh), (0, []))[1]),
        current_step_fetch_counts,
        int((time.perf_counter() - frame_start) * 1000),
    )
    return cumulative_snow_inches.astype(np.float32, copy=False), src_crs, src_transform


def _derive_snowfall_kuchera_total_cumulative(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None,
    model_plugin: Any,
    ctx: FetchContext | None = None,
    derive_component_target_grid: dict[str, str] | None = None,
    derive_component_resampling: str | None = None,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    del var_capability
    frame_start = time.perf_counter()
    hints = getattr(getattr(var_spec_model, "selectors", None), "hints", {})
    apcp_component = str(hints.get("apcp_component", "apcp_step"))
    apcp_product_raw = str(hints.get("kuchera_apcp_product", "")).strip()
    apcp_product = apcp_product_raw or None
    profile_product_raw = str(hints.get("kuchera_profile_product", "")).strip()
    profile_product = profile_product_raw or None
    ptype_product_raw = str(hints.get("kuchera_ptype_product", "")).strip()
    ptype_product = ptype_product_raw or apcp_product or product
    configured_levels_hpa = _parse_kuchera_levels_hpa(hints.get("kuchera_levels_hpa"))
    profile_mode_hint = str(hints.get("kuchera_profile_mode", "")).strip().lower()
    use_simplified_profile = (
        profile_mode_hint in {"simplified", "ops", "operational"}
        or (not profile_mode_hint and str(model_id).lower() == "hrrr")
    )
    profile_levels_hpa = _kuchera_select_profile_levels(configured_levels_hpa, simplified=use_simplified_profile)
    if not profile_levels_hpa:
        raise ValueError(f"No Kuchera profile levels configured for {model_id}/{var_key} fh{fh:03d}")
    use_ptype_gate = _parse_hint_bool(
        hints.get("kuchera_use_ptype_gate"),
        default=False,
    )
    use_sfc_pressure_mask = _parse_hint_bool(
        hints.get("kuchera_use_sfc_pressure_mask"),
        default=False,
    )
    sfc_pressure_product_raw = str(hints.get("kuchera_sfc_pressure_product", "")).strip()
    resolved_sfc_pressure_product = sfc_pressure_product_raw or product
    sfc_pressure_margin_pa_raw = hints.get("kuchera_sfc_pressure_margin_pa")
    try:
        sfc_pressure_margin_pa = np.float32(float(sfc_pressure_margin_pa_raw))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        sfc_pressure_margin_pa = _KUCHERA_SFC_PRESSURE_MARGIN_PA_DEFAULT
    sfc_pressure_margin_pa = np.float32(max(0.0, float(sfc_pressure_margin_pa)))
    min_step_lwe_raw = hints.get("min_step_lwe_kgm2", "0.01")
    try:
        min_step_lwe = float(min_step_lwe_raw)
    except (TypeError, ValueError):
        min_step_lwe = 0.01
    min_step_lwe = max(min_step_lwe, 0.0)
    rebuild_window_steps = _parse_hint_int(
        hints.get("kuchera_incremental_rebuild_window_steps"),
        default=_KUCHERA_INCREMENTAL_WINDOW_DEFAULT,
        minimum=1,
    )

    step_fhs = _resolve_cumulative_step_fhs(hints=hints, fh=fh, run_date=run_date, default_step_hours=6)
    if not step_fhs:
        raise ValueError(f"No cumulative Kuchera source steps resolved for {model_id}/{var_key} fh{fh:03d}")
    logger.info(
        "derive %s fh%03d apcp_steps=%d profile_levels=%s profile_mode=%s apcp_product=%s profile_product=%s%s",
        var_key,
        fh,
        len(step_fhs),
        profile_levels_hpa,
        "simplified" if use_simplified_profile else "full",
        apcp_product or product,
        profile_product or product,
        _cadence_hint_suffix(hints),
    )
    logger.debug("derive %s fh%03d apcp_steps=%s", var_key, fh, step_fhs)

    if use_simplified_profile:
        logger.info(
            "kuchera_profile_mode=simplified model=%s fh=%03d levels=%s",
            model_id,
            fh,
            profile_levels_hpa,
        )

    use_warped, target_region, target_grid_id, resampling = _resolve_warped_state(
        derive_component_target_grid, derive_component_resampling, model_id,
    )

    resolved_profile_product = str(profile_product or product)
    sfc_pressure_mask_logged = False
    sfc_pressure_fetch_failed_logged = False
    fallback_used = False
    fallback_profile_logged = False
    missing_level_warning_logged = False
    sparse_level_warning_logged = False
    ptype_stats: dict[str, float] = {
        "frozen_min": float("inf"),
        "frozen_max": float("-inf"),
        "frozen_sum": 0.0,
        "frozen_count": 0.0,
        "apcp_min": float("inf"),
        "apcp_max": float("-inf"),
        "apcp_frozen_min": float("inf"),
        "apcp_frozen_max": float("-inf"),
    }
    ptype_any_precip_pixels = False
    ptype_any_reduced_pixels = False
    kuchera_maxt_stats: dict[str, float] = {
        "max_t_min": float("inf"),
        "max_t_max": float("-inf"),
        "max_t_sum": 0.0,
        "max_t_count": 0.0,
        "ratio_min": float("inf"),
        "ratio_max": float("-inf"),
        "ratio_sum": 0.0,
        "ratio_count": 0.0,
        "ratio_clamp_max_count": 0.0,
    }
    apcp_cumulative_fallback_used = False
    current_step_fetch_counts: dict[str, int] = {"apcp": 0, "profile_temp": 0, "ptype": 0}

    reused_prev_cumulative = False
    base_fh: int | None = None
    base_cumulative: np.ndarray | None = None
    base_crs: rasterio.crs.CRS | None = None
    base_transform: rasterio.transform.Affine | None = None
    start_index = max(0, len(step_fhs) - rebuild_window_steps)

    if len(step_fhs) >= 2:
        prev_fh = int(step_fhs[-2])
        prior = _kuchera_load_prior_cumulative(
            model_id=model_id,
            run_date=run_date,
            var_key=var_key,
            fh=prev_fh,
            ctx=ctx,
        )
        if prior is not None:
            base_cumulative, base_crs, base_transform = prior
            base_fh = prev_fh
            start_index = len(step_fhs) - 1
            reused_prev_cumulative = True

    if base_cumulative is None and start_index > 0:
        anchor_fh = int(step_fhs[start_index - 1])
        prior = _kuchera_load_prior_cumulative(
            model_id=model_id,
            run_date=run_date,
            var_key=var_key,
            fh=anchor_fh,
            ctx=ctx,
        )
        if prior is None:
            start_index = 0
            base_fh = None
        else:
            base_cumulative, base_crs, base_transform = prior
            base_fh = anchor_fh

    steps_processed = 0
    while True:
        subset_step_fhs = step_fhs[start_index:]
        if not subset_step_fhs:
            raise ValueError(f"No incremental Kuchera steps selected for {model_id}/{var_key} fh{fh:03d}")

        if start_index > 0:
            anchor_fh = int(step_fhs[start_index - 1])
            if base_fh != anchor_fh or base_cumulative is None:
                prior = _kuchera_load_prior_cumulative(
                    model_id=model_id,
                    run_date=run_date,
                    var_key=var_key,
                    fh=anchor_fh,
                    ctx=ctx,
                )
                if prior is None:
                    start_index = 0
                    base_cumulative = None
                    base_crs = None
                    base_transform = None
                    base_fh = None
                    reused_prev_cumulative = False
                    continue
                base_cumulative, base_crs, base_transform = prior
                base_fh = anchor_fh

        cum_diff_state = _ApcpCumDiffState()
        subset_cumulative: np.ndarray | None = None
        subset_valid_mask: np.ndarray | None = None
        subset_crs: rasterio.crs.CRS | None = None
        subset_transform: rasterio.transform.Affine | None = None
        requires_full_history_rebuild = False
        rebuild_trigger_step_fh: int | None = None
        steps_processed = 0

        for local_step_index, step_fh in enumerate(subset_step_fhs):
            step_apcp_data, apcp_valid, step_crs, step_transform, step_apcp_mode = _resolve_apcp_step_data(
                step_fh=step_fh,
                step_index=start_index + local_step_index,
                step_fhs=step_fhs,
                model_id=model_id,
                product=product,
                run_date=run_date,
                model_plugin=model_plugin,
                ctx=ctx,
                apcp_component=apcp_component,
                apcp_product=apcp_product,
                use_warped=use_warped,
                target_region=target_region,
                target_grid_id=target_grid_id,
                resampling=resampling,
                cum_diff_state=cum_diff_state,
            )
            apcp_cumulative_fallback_used = apcp_cumulative_fallback_used or step_apcp_mode != "exact_step"
            steps_processed += 1
            if int(step_fh) == int(fh):
                current_step_fetch_counts["apcp"] = current_step_fetch_counts.get("apcp", 0) + 1

            # Incremental reuse only has APCP differencing state for the current
            # subset. Any history-dependent APCP window within that subset
            # needs a full history rebuild to avoid subtracting against a stale
            # baseline or missing intra-bucket state.
            if step_apcp_mode != "exact_step" and start_index > 0:
                requires_full_history_rebuild = True
                rebuild_trigger_step_fh = int(step_fh)
                break

            assert apcp_valid is not None
            step_apcp_clean = step_apcp_data
            if min_step_lwe > 0.0:
                step_apcp_clean = np.where(
                    step_apcp_clean >= min_step_lwe,
                    step_apcp_clean,
                    0.0,
                ).astype(np.float32, copy=False)

            step_apcp_for_snow = step_apcp_clean
            if use_ptype_gate:
                frozen_frac, _ptype_fallback_used, ptype_fetch_count = _kuchera_frozen_fraction_for_step(
                    model_id=model_id,
                    var_key=var_key,
                    product=str(ptype_product),
                    run_date=run_date,
                    step_fh=step_fh,
                    model_plugin=model_plugin,
                    use_warped=use_warped,
                    target_region=target_region,
                    target_grid_id=target_grid_id,
                    resampling=resampling,
                    ctx=ctx,
                    expected_shape=step_apcp_clean.shape,
                )
                if int(step_fh) == int(fh):
                    current_step_fetch_counts["ptype"] = current_step_fetch_counts.get("ptype", 0) + int(ptype_fetch_count)
                step_apcp_for_snow = _apply_kuchera_ptype_gate(step_apcp_clean, frozen_frac)

                finite_frozen = np.isfinite(frozen_frac)
                if np.any(finite_frozen):
                    frozen_values = frozen_frac[finite_frozen]
                    ptype_stats["frozen_min"] = min(ptype_stats["frozen_min"], float(np.min(frozen_values)))
                    ptype_stats["frozen_max"] = max(ptype_stats["frozen_max"], float(np.max(frozen_values)))
                    ptype_stats["frozen_sum"] += float(np.sum(frozen_values, dtype=np.float64))
                    ptype_stats["frozen_count"] += float(frozen_values.size)
                finite_apcp = np.isfinite(step_apcp_clean)
                if np.any(finite_apcp):
                    apcp_values = step_apcp_clean[finite_apcp]
                    ptype_stats["apcp_min"] = min(ptype_stats["apcp_min"], float(np.min(apcp_values)))
                    ptype_stats["apcp_max"] = max(ptype_stats["apcp_max"], float(np.max(apcp_values)))
                finite_apcp_frozen = np.isfinite(step_apcp_for_snow)
                if np.any(finite_apcp_frozen):
                    apcp_frozen_values = step_apcp_for_snow[finite_apcp_frozen]
                    ptype_stats["apcp_frozen_min"] = min(ptype_stats["apcp_frozen_min"], float(np.min(apcp_frozen_values)))
                    ptype_stats["apcp_frozen_max"] = max(ptype_stats["apcp_frozen_max"], float(np.max(apcp_frozen_values)))

                precip_mask = apcp_valid & np.isfinite(step_apcp_clean) & (step_apcp_clean > 0.0) & np.isfinite(frozen_frac)
                if np.any(precip_mask):
                    ptype_any_precip_pixels = True
                    if np.any(frozen_frac[precip_mask] < 0.999):
                        ptype_any_reduced_pixels = True

            step_levels: list[int] = []
            step_temps: list[np.ndarray] = []

            step_sfc_pressure: np.ndarray | None = None
            if use_sfc_pressure_mask:
                try:
                    sfc_pres_data, _, _ = _fetch_step_component(
                        model_id=model_id,
                        product=resolved_sfc_pressure_product,
                        run_date=run_date,
                        step_fh=step_fh,
                        model_plugin=model_plugin,
                        var_key="pres_sfc",
                        use_warped=use_warped,
                        target_region=target_region,
                        target_grid_id=target_grid_id,
                        resampling=resampling,
                        ctx=ctx,
                    )
                    step_sfc_pressure = sfc_pres_data.astype(np.float32, copy=False)
                except (HerbieTransientUnavailableError, RuntimeError, ValueError) as exc:
                    if not sfc_pressure_fetch_failed_logged:
                        logger.warning(
                            "kuchera_sfc_pressure_mask fetch_failed step_fh=%03d reason=%s; "
                            "proceeding without below-ground filtering",
                            step_fh,
                            exc,
                        )
                        sfc_pressure_fetch_failed_logged = True

            for level_hpa in profile_levels_hpa:
                try:
                    step_temp, _, _ = _fetch_step_component(
                        model_id=model_id,
                        product=resolved_profile_product,
                        run_date=run_date,
                        step_fh=step_fh,
                        model_plugin=model_plugin,
                        var_key=f"tmp{int(level_hpa)}",
                        use_warped=use_warped,
                        target_region=target_region,
                        target_grid_id=target_grid_id,
                        resampling=resampling,
                        ctx=ctx,
                    )
                    if int(step_fh) == int(fh):
                        current_step_fetch_counts["profile_temp"] = current_step_fetch_counts.get("profile_temp", 0) + 1
                except (HerbieTransientUnavailableError, RuntimeError, ValueError):
                    continue
                if step_temp.shape != step_apcp_clean.shape:
                    raise ValueError(
                        f"Kuchera temp shape mismatch for {model_id}/{var_key} at fh{step_fh:03d} "
                        f"level={level_hpa}: {step_temp.shape} != {step_apcp_clean.shape}"
                    )
                step_levels.append(int(level_hpa))
                step_temp_clean = step_temp.astype(np.float32, copy=False)
                if step_sfc_pressure is not None and step_temp_clean.shape == step_sfc_pressure.shape:
                    level_pa = np.float32(int(level_hpa) * 100)
                    below_ground = np.isfinite(step_sfc_pressure) & (level_pa > step_sfc_pressure + sfc_pressure_margin_pa)
                    masked_count = int(np.count_nonzero(below_ground))
                    if masked_count > 0:
                        step_temp_clean = np.where(below_ground, np.nan, step_temp_clean).astype(np.float32, copy=False)
                        if not sfc_pressure_mask_logged:
                            logger.info(
                                "kuchera_sfc_pressure_mask active step_fh=%03d level=%d "
                                "masked_pixels=%d/%d",
                                step_fh,
                                level_hpa,
                                masked_count,
                                step_temp_clean.size,
                            )
                            sfc_pressure_mask_logged = True
                step_temps.append(step_temp_clean)

            step_max_t_k = np.full(step_apcp_clean.shape, np.nan, dtype=np.float32)
            if not step_levels:
                if not fallback_profile_logged:
                    logger.info(
                        "kuchera_profile insufficient_levels=0/%d fallback=10to1",
                        len(profile_levels_hpa),
                    )
                    fallback_profile_logged = True
                fallback_used = True
                step_slr = np.full(step_apcp_clean.shape, 10.0, dtype=np.float32)
            else:
                if len(step_levels) < len(profile_levels_hpa) and not missing_level_warning_logged:
                    missing_levels = sorted(level for level in profile_levels_hpa if level not in set(step_levels))
                    logger.warning(
                        "kuchera_maxt_low500 missing_levels available=%d/%d step_fh=%03d missing=%s",
                        len(step_levels),
                        len(profile_levels_hpa),
                        step_fh,
                        missing_levels,
                    )
                    missing_level_warning_logged = True
                if len(step_levels) < min(2, len(profile_levels_hpa)) and not sparse_level_warning_logged:
                    logger.warning(
                        "kuchera_maxt_low500 sparse_levels available=%d/%d step_fh=%03d using_warmest_available=true",
                        len(step_levels),
                        len(profile_levels_hpa),
                        step_fh,
                    )
                    sparse_level_warning_logged = True
                step_max_t_k = _kuchera_maxt_low500_from_temp_stack_k(step_temps)
                step_slr = _kuchera_ratio_from_maxt_low500_k(step_max_t_k)
                step_slr = np.where(np.isfinite(step_slr), step_slr, 10.0).astype(np.float32, copy=False)

            valid_precip_ratio = (
                apcp_valid
                & np.isfinite(step_apcp_for_snow)
                & (step_apcp_for_snow > 0.0)
                & np.isfinite(step_slr)
            )
            if np.any(valid_precip_ratio):
                ratio_values = step_slr[valid_precip_ratio]
                kuchera_maxt_stats["ratio_min"] = min(kuchera_maxt_stats["ratio_min"], float(np.min(ratio_values)))
                kuchera_maxt_stats["ratio_max"] = max(kuchera_maxt_stats["ratio_max"], float(np.max(ratio_values)))
                kuchera_maxt_stats["ratio_sum"] += float(np.sum(ratio_values, dtype=np.float64))
                kuchera_maxt_stats["ratio_count"] += float(ratio_values.size)
                kuchera_maxt_stats["ratio_clamp_max_count"] += float(
                    np.count_nonzero(ratio_values >= (_KUCHERA_RATIO_CLAMP_MAX - np.float32(1e-6)))
                )

                valid_max_t = valid_precip_ratio & np.isfinite(step_max_t_k)
                if np.any(valid_max_t):
                    max_t_values = step_max_t_k[valid_max_t]
                    kuchera_maxt_stats["max_t_min"] = min(kuchera_maxt_stats["max_t_min"], float(np.min(max_t_values)))
                    kuchera_maxt_stats["max_t_max"] = max(kuchera_maxt_stats["max_t_max"], float(np.max(max_t_values)))
                    kuchera_maxt_stats["max_t_sum"] += float(np.sum(max_t_values, dtype=np.float64))
                    kuchera_maxt_stats["max_t_count"] += float(max_t_values.size)

            contribution = (step_apcp_for_snow * step_slr).astype(np.float32, copy=False)
            step_valid = apcp_valid & np.isfinite(step_slr)

            if subset_cumulative is None:
                subset_cumulative = contribution
                subset_valid_mask = step_valid
                subset_crs = step_crs
                subset_transform = step_transform
            else:
                if contribution.shape != subset_cumulative.shape:
                    raise ValueError(
                        f"Kuchera contribution shape mismatch at fh{step_fh:03d}: "
                        f"{contribution.shape} != {subset_cumulative.shape}"
                    )
                subset_cumulative = (subset_cumulative + contribution).astype(np.float32, copy=False)
                subset_valid_mask = np.logical_or(subset_valid_mask, step_valid)

        if requires_full_history_rebuild and start_index > 0:
            logger.info(
                "kuchera_incremental cumulative_apcp_requires_full_rebuild fh=%03d step_fh=%03d start_index=%d",
                fh,
                rebuild_trigger_step_fh if rebuild_trigger_step_fh is not None else -1,
                start_index,
            )
            start_index = 0
            base_cumulative = None
            base_crs = None
            base_transform = None
            base_fh = None
            reused_prev_cumulative = False
            continue

        if subset_cumulative is None or subset_valid_mask is None or subset_crs is None or subset_transform is None:
            raise ValueError(f"No cumulative Kuchera source steps resolved for {model_id}/{var_key} fh{fh:03d}")

        if base_cumulative is not None and base_crs is not None and base_transform is not None:
            base_data = np.asarray(base_cumulative, dtype=np.float32)
            shape_match = base_data.shape == subset_cumulative.shape
            crs_match = base_crs == subset_crs
            transform_match = base_transform == subset_transform
            if not (shape_match and crs_match and transform_match):
                if start_index > 0:
                    logger.warning(
                        "kuchera_incremental base-grid mismatch at fh=%03d; retrying full rebuild "
                        "(shape_match=%s crs_match=%s transform_match=%s)",
                        fh,
                        shape_match,
                        crs_match,
                        transform_match,
                    )
                    start_index = 0
                    base_cumulative = None
                    base_crs = None
                    base_transform = None
                    base_fh = None
                    reused_prev_cumulative = False
                    continue
                raise ValueError(
                    f"Kuchera incremental base-grid mismatch for {model_id}/{var_key} fh{fh:03d}"
                )

            base_valid = np.isfinite(base_data)
            base_clean = np.where(base_valid, base_data, 0.0).astype(np.float32, copy=False)
            subset_clean = np.where(subset_valid_mask, subset_cumulative, 0.0).astype(np.float32, copy=False)
            cumulative_kgm2 = (base_clean + subset_clean).astype(np.float32, copy=False)
            valid_mask = base_valid | subset_valid_mask
            src_crs = base_crs
            src_transform = base_transform
        else:
            cumulative_kgm2 = subset_cumulative.astype(np.float32, copy=False)
            valid_mask = subset_valid_mask
            src_crs = subset_crs
            src_transform = subset_transform
        break

    cumulative_kgm2 = np.where(valid_mask, cumulative_kgm2, np.nan).astype(np.float32, copy=False)
    cumulative_snow_inches = cumulative_kgm2 * 0.03937007874015748
    if use_ptype_gate and ptype_stats["frozen_count"] > 0:
        frozen_mean = ptype_stats["frozen_sum"] / ptype_stats["frozen_count"]
        logger.info(
            "kuchera_ptype_gate fh=%03d frozen_frac_min=%.3f frozen_frac_max=%.3f "
            "frozen_frac_mean=%.3f apcp_step_min=%.3f apcp_step_max=%.3f "
            "apcp_frozen_min=%.3f apcp_frozen_max=%.3f",
            fh,
            ptype_stats["frozen_min"],
            ptype_stats["frozen_max"],
            frozen_mean,
            ptype_stats["apcp_min"],
            ptype_stats["apcp_max"],
            ptype_stats["apcp_frozen_min"],
            ptype_stats["apcp_frozen_max"],
        )
        if ptype_any_precip_pixels and not ptype_any_reduced_pixels:
            logger.warning("ptype gate ineffective")

    ratio_count = kuchera_maxt_stats["ratio_count"]
    ratio_mean = kuchera_maxt_stats["ratio_sum"] / ratio_count if ratio_count > 0 else float("nan")
    max_t_count = kuchera_maxt_stats["max_t_count"]
    max_t_mean = kuchera_maxt_stats["max_t_sum"] / max_t_count if max_t_count > 0 else float("nan")
    logger.info(
        "kuchera_maxt_low500 fh=%03d maxT_k_min=%.2f maxT_k_max=%.2f maxT_k_mean=%.2f "
        "ratio_min=%.2f ratio_max=%.2f ratio_mean=%.2f valid_precip_px=%d",
        fh,
        kuchera_maxt_stats["max_t_min"] if max_t_count > 0 else float("nan"),
        kuchera_maxt_stats["max_t_max"] if max_t_count > 0 else float("nan"),
        max_t_mean,
        kuchera_maxt_stats["ratio_min"] if ratio_count > 0 else float("nan"),
        kuchera_maxt_stats["ratio_max"] if ratio_count > 0 else float("nan"),
        ratio_mean,
        int(ratio_count),
    )
    if ratio_count > 0:
        clamp_max_fraction = kuchera_maxt_stats["ratio_clamp_max_count"] / ratio_count
        if clamp_max_fraction > 0.2:
            logger.warning(
                "kuchera_maxt_low500 high_clamp_max_fraction=%.3f threshold=0.200 valid_precip_px=%d",
                clamp_max_fraction,
                int(ratio_count),
            )

    quality_flags: list[str] = []
    if fallback_used:
        quality_flags.append("slr_fallback_10to1")
    if apcp_cumulative_fallback_used:
        quality_flags.append("apcp_cumulative_fallback")
    _record_derive_quality(
        ctx,
        var_key=var_key,
        fh=fh,
        quality_flags=quality_flags,
    )
    logger.info(
        "snow_ratio method=kuchera fh=%d levels=%s fallback=%s",
        fh, profile_levels_hpa, "10to1" if fallback_used else "none",
    )
    logger.info(
        "kuchera_incremental run=%s fh=%03d total_steps=%d computed_steps=%d reused_prev_cumulative=%s "
        "base_fh=%s current_step_fetches=%s compute_ms=%d",
        _run_id_from_date(run_date),
        fh,
        len(step_fhs),
        steps_processed,
        "true" if reused_prev_cumulative else "false",
        f"{base_fh:03d}" if base_fh is not None else "none",
        current_step_fetch_counts,
        int((time.perf_counter() - frame_start) * 1000),
    )

    _kuchera_store_cumulative_cache(
        model_id=model_id,
        run_date=run_date,
        var_key=var_key,
        fh=fh,
        data=cumulative_kgm2,
        crs=src_crs,
        transform=src_transform,
        ctx=ctx,
    )
    return cumulative_snow_inches.astype(np.float32, copy=False), src_crs, src_transform


DERIVE_STRATEGIES: dict[str, DeriveStrategy] = {
    "wspd10m": DeriveStrategy(
        id="wspd10m",
        required_inputs=("10u", "10v"),
        output_var_key="wspd10m",
        execute=_derive_wspd10m,
    ),
    "radar_ptype_combo": DeriveStrategy(
        id="radar_ptype_combo",
        required_inputs=("refc", "crain", "csnow", "cicep", "cfrzr"),
        output_var_key="radar_ptype",
        execute=_derive_radar_ptype_combo,
    ),
    "precip_ptype_blend": DeriveStrategy(
        id="precip_ptype_blend",
        required_inputs=("prate", "crain", "csnow", "cicep", "cfrzr"),
        output_var_key="precip_ptype",
        execute=_derive_precip_ptype_blend,
    ),
    "precip_total_cumulative": DeriveStrategy(
        id="precip_total_cumulative",
        required_inputs=("apcp_step",),
        output_var_key="precip_total",
        execute=_derive_precip_total_cumulative,
    ),
    "snowfall_total_10to1_cumulative": DeriveStrategy(
        id="snowfall_total_10to1_cumulative",
        required_inputs=("apcp_step", "csnow"),
        output_var_key="snowfall_total",
        execute=_derive_snowfall_total_10to1_cumulative,
    ),
    "snowfall_kuchera_total_cumulative": DeriveStrategy(
        id="snowfall_kuchera_total_cumulative",
        required_inputs=("apcp_step", "tmp850", "tmp700"),
        output_var_key="snowfall_kuchera_total",
        execute=_derive_snowfall_kuchera_total_cumulative,
    ),
}

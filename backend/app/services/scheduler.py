from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import re
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import rasterio
from PIL import Image
from rasterio.enums import Resampling

from app.models.registry import MODEL_REGISTRY
from app.services.builder.pipeline import build_frame

logger = logging.getLogger(__name__)

RUN_ID_RE = re.compile(r"^(?P<day>\d{8})_(?P<hour>\d{2})z$")
DEFAULT_DATA_ROOT = Path("/opt/twf_v3/data/v3")
DEFAULT_PRIMARY_VAR = "tmp2m"
DEFAULT_VARS = "tmp2m,tmp850,precip_total,snowfall_total,wspd10m,refc,radar_ptype"
DEFAULT_POLL_SECONDS = 300
INCOMPLETE_RUN_POLL_SECONDS = 60
DEFAULT_PROMOTION_FHS = (0, 1, 2)
DEFAULT_PROBE_VAR = "tmp2m"
CANONICAL_COVERAGE = "conus"
DEFAULT_HRRR_PROBE_ATTEMPTS = 4
MAX_HRRR_PROBE_ATTEMPTS = 6
ENV_DEFAULT_VARS = "TWF_V3_SCHEDULER_VARS"
ENV_DEFAULT_PRIMARY_VARS = "TWF_V3_SCHEDULER_PRIMARY_VARS"
ENV_DEFAULT_POLL_SECONDS = "TWF_V3_SCHEDULER_POLL_SECONDS"
ENV_DEFAULT_KEEP_RUNS = "TWF_V3_SCHEDULER_KEEP_RUNS"
ENV_PROBE_VAR = "TWF_V3_SCHEDULER_PROBE_VAR"
ENV_HERBIE_PRIORITY = "TWF_HERBIE_PRIORITY"
ENV_LOOP_PREGENERATE_ENABLED = "TWF_V3_LOOP_PREGENERATE_ENABLED"
ENV_LOOP_CACHE_ROOT = "TWF_V3_LOOP_CACHE_ROOT"
ENV_LOOP_PREGENERATE_WORKERS = "TWF_V3_LOOP_PREGENERATE_WORKERS"
ENV_LOOP_WEBP_QUALITY = "TWF_V3_LOOP_WEBP_QUALITY"
ENV_LOOP_WEBP_MAX_DIM = "TWF_V3_LOOP_WEBP_MAX_DIM"

DEFAULT_LOOP_PREGENERATE_ENABLED = True
DEFAULT_LOOP_CACHE_ROOT = Path("/tmp/twf_v3_loop_webp_cache")
DEFAULT_LOOP_PREGENERATE_WORKERS = 4
DEFAULT_LOOP_WEBP_QUALITY = 82
DEFAULT_LOOP_WEBP_MAX_DIM = 1600


class SchedulerConfigError(RuntimeError):
    pass


def _parse_run_id_datetime(run_id: str) -> datetime | None:
    match = RUN_ID_RE.match(run_id)
    if not match:
        return None
    try:
        day = match.group("day")
        hour = int(match.group("hour"))
        if not (0 <= hour <= 23):
            return None
        year = int(day[0:4])
        month = int(day[4:6])
        day_num = int(day[6:8])
        return datetime(year, month, day_num, hour, tzinfo=timezone.utc)
    except ValueError:
        return None


def _run_id_from_dt(run_dt: datetime) -> str:
    return run_dt.strftime("%Y%m%d_%Hz")


def _parse_vars(value: str) -> list[str]:
    vars_list = [item.strip().lower() for item in value.split(",") if item.strip()]
    if not vars_list:
        raise SchedulerConfigError("--vars cannot be empty")
    return vars_list


def _dedupe_preserve_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _data_root(cli_data_root: str | None) -> Path:
    if cli_data_root:
        return Path(cli_data_root).resolve()
    return Path(os.getenv("TWF_V3_DATA_ROOT", str(DEFAULT_DATA_ROOT))).resolve()


def _workers(cli_workers: int | None) -> int:
    if cli_workers is not None and cli_workers > 0:
        return cli_workers
    raw = os.getenv("TWF_V3_WORKERS", "4").strip()
    try:
        value = int(raw)
    except ValueError:
        return 4
    return value if value > 0 else 4


def _int_from_env(env_name: str, fallback: int, *, min_value: int) -> int:
    raw = os.getenv(env_name, "").strip()
    if not raw:
        return fallback
    try:
        parsed = int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using fallback=%d", env_name, raw, fallback)
        return fallback
    return parsed if parsed >= min_value else fallback


def _bool_from_env(env_name: str, fallback: bool) -> bool:
    raw = os.getenv(env_name, "").strip().lower()
    if not raw:
        return fallback
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    logger.warning("Invalid %s=%r; using fallback=%s", env_name, raw, fallback)
    return fallback


def _resolve_model(model_id: str):
    plugin = MODEL_REGISTRY.get(model_id)
    if plugin is None:
        raise SchedulerConfigError(f"Unknown model: {model_id}")
    return plugin


def _resolve_vars_to_schedule(plugin, requested: list[str]) -> list[str]:
    resolved: list[str] = []

    if requested:
        for raw in requested:
            normalized = plugin.normalize_var_id(raw)
            spec = plugin.get_var(normalized)
            if spec is None:
                logger.warning("Skipping unknown var for model=%s: %s", plugin.id, raw)
                continue
            if not (bool(getattr(spec, "primary", False)) or bool(getattr(spec, "derived", False))):
                logger.info("Skipping component-only var: %s", normalized)
                continue
            resolved.append(normalized)
        return _dedupe_preserve_order(resolved)

    for var_id, spec in plugin.vars.items():
        normalized = plugin.normalize_var_id(var_id)
        if plugin.get_var(normalized) is None:
            continue
        if bool(getattr(spec, "primary", False)) or bool(getattr(spec, "derived", False)):
            resolved.append(normalized)
    return _dedupe_preserve_order(resolved)


def _probe_search_pattern(plugin: Any, probe_var: str) -> str:
    probe_var_id = plugin.normalize_var_id(probe_var)
    probe_spec = plugin.get_var(probe_var_id)
    if probe_spec is None:
        raise SchedulerConfigError(f"Probe var {probe_var!r} not found for model={plugin.id}")

    selectors = getattr(probe_spec, "selectors", None)
    searches = getattr(selectors, "search", None) if selectors is not None else None
    if not searches:
        raise SchedulerConfigError(
            f"Probe var {probe_var_id!r} has no search pattern and cannot be used for run probing"
        )
    return str(searches[0])


def _probe_run_exists(*, plugin: Any, run_dt: datetime, probe_var: str) -> bool:
    from herbie.core import Herbie

    search_pattern = _probe_search_pattern(plugin, probe_var)
    priority_raw = os.getenv(ENV_HERBIE_PRIORITY, "aws,nomads,google,azure,pando,pando2")
    priorities = [item.strip().lower() for item in priority_raw.split(",") if item.strip()]
    if not priorities:
        priorities = ["aws", "nomads", "google", "azure", "pando", "pando2"]

    herbie_date = run_dt.replace(tzinfo=None) if run_dt.tzinfo else run_dt
    probe_var_id = plugin.normalize_var_id(probe_var)
    last_exc: Exception | None = None
    for priority in priorities:
        try:
            H = Herbie(
                herbie_date,
                model=plugin.id,
                product=getattr(plugin, "product", "sfc"),
                fxx=0,
                priority=priority,
            )
            inventory = H.inventory(search_pattern)
            if inventory is not None and len(inventory) > 0:
                logger.info(
                    "Run probe success: model=%s run=%s probe_var=%s priority=%s",
                    plugin.id,
                    _run_id_from_dt(run_dt),
                    probe_var_id,
                    priority,
                )
                return True
        except Exception as exc:
            last_exc = exc
            continue

    logger.info(
        "Run probe miss: model=%s run=%s probe_var=%s priorities=%s (%s)",
        plugin.id,
        _run_id_from_dt(run_dt),
        probe_var_id,
        priorities,
        last_exc,
    )
    return False


def _resolve_latest_run_dt(model_id: str, *, plugin: Any, probe_var: str) -> datetime:
    now = datetime.now(timezone.utc)
    if model_id == "hrrr":
        base = now.replace(minute=0, second=0, microsecond=0)
        attempts = min(max(DEFAULT_HRRR_PROBE_ATTEMPTS, 1), MAX_HRRR_PROBE_ATTEMPTS)
        for offset in range(attempts):
            candidate = base - timedelta(hours=offset)
            if _probe_run_exists(plugin=plugin, run_dt=candidate, probe_var=probe_var):
                return candidate
        target = now - timedelta(hours=2)
        fallback = target.replace(minute=0, second=0, microsecond=0)
        logger.warning(
            "HRRR probe failed after %d attempts; falling back to heuristic run=%s",
            attempts,
            _run_id_from_dt(fallback),
        )
        return fallback
    if model_id == "gfs":
        target = now - timedelta(hours=5)
        cycle_hour = (target.hour // 6) * 6
        return target.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)
    target = now - timedelta(hours=3)
    return target.replace(minute=0, second=0, microsecond=0)


def _resolve_run_dt(run_arg: str | None, model_id: str, *, plugin: Any, probe_var: str) -> datetime:
    if run_arg:
        parsed = _parse_run_id_datetime(run_arg)
        if parsed is None:
            raise SchedulerConfigError(
                f"Invalid --run value {run_arg!r}. Expected YYYYMMDD_HHz (e.g. 20260217_06z)."
            )
        return parsed
    return _resolve_latest_run_dt(model_id, plugin=plugin, probe_var=probe_var)


def _scheduled_targets_for_cycle(plugin, vars_to_build: list[str], cycle_hour: int) -> list[tuple[str, int]]:
    fhs = list(plugin.target_fhs(cycle_hour))
    targets: list[tuple[str, int]] = []
    for var_id in vars_to_build:
        min_fh = 6 if plugin.id == "gfs" and var_id == "qpf6h" else 0
        for fh in fhs:
            if fh < min_fh:
                continue
            targets.append((var_id, int(fh)))
    return targets


def _frame_sidecar_path(data_root: Path, model: str, run_id: str, var_id: str, fh: int) -> Path:
    return data_root / "staging" / model / run_id / var_id / f"fh{fh:03d}.json"


def _frame_rgba_path(data_root: Path, model: str, run_id: str, var_id: str, fh: int) -> Path:
    return data_root / "staging" / model / run_id / var_id / f"fh{fh:03d}.rgba.cog.tif"


def _frame_value_path(data_root: Path, model: str, run_id: str, var_id: str, fh: int) -> Path:
    return data_root / "staging" / model / run_id / var_id / f"fh{fh:03d}.val.cog.tif"


def _frame_artifacts_exist(
    data_root: Path,
    model: str,
    run_id: str,
    var_id: str,
    fh: int,
) -> bool:
    rgba = _frame_rgba_path(data_root, model, run_id, var_id, fh)
    val = _frame_value_path(data_root, model, run_id, var_id, fh)
    side = _frame_sidecar_path(data_root, model, run_id, var_id, fh)

    def _safe_exists(path: Path) -> bool:
        try:
            return path.exists()
        except PermissionError:
            logger.warning("Permission denied while checking artifact path: %s", path)
            return False

    return _safe_exists(rgba) and _safe_exists(val) and _safe_exists(side)


def _build_one(
    *,
    model_id: str,
    var_id: str,
    fh: int,
    run_dt: datetime,
    data_root: Path,
    plugin,
) -> tuple[str, int, bool]:
    result = build_frame(
        model=model_id,
        region=CANONICAL_COVERAGE,
        var_id=var_id,
        fh=fh,
        run_date=run_dt,
        data_root=data_root,
        product=getattr(plugin, "product", "sfc"),
        model_plugin=plugin,
    )
    return var_id, fh, result is not None


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")
    tmp.replace(path)


def _write_latest_pointer(data_root: Path, model: str, run_id: str) -> None:
    run_dt = _parse_run_id_datetime(run_id)
    if run_dt is None:
        raise SchedulerConfigError(f"Cannot write LATEST.json for invalid run_id={run_id!r}")
    payload = {
        "run_id": run_id,
        "cycle_utc": run_dt.strftime("%Y-%m-%dT%H:00:00Z"),
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "scheduler_v3",
    }
    latest_path = data_root / "published" / model / "LATEST.json"
    _write_json_atomic(latest_path, payload)


def _should_promote(
    data_root: Path,
    model: str,
    run_id: str,
    primary_vars: list[str],
    promotion_fhs: Iterable[int],
) -> bool:
    for var_id in primary_vars:
        for fh in promotion_fhs:
            rgba = _frame_rgba_path(data_root, model, run_id, var_id, int(fh))
            val = _frame_value_path(data_root, model, run_id, var_id, int(fh))
            side = _frame_sidecar_path(data_root, model, run_id, var_id, int(fh))
            if rgba.exists() and val.exists() and side.exists():
                return True
    return False


def _promote_run(data_root: Path, model: str, run_id: str) -> None:
    stage_run = data_root / "staging" / model / run_id
    if not stage_run.is_dir():
        raise SchedulerConfigError(f"Cannot promote missing staging run dir: {stage_run}")

    published_model = data_root / "published" / model
    published_model.mkdir(parents=True, exist_ok=True)

    published_run = published_model / run_id
    tmp_run = published_model / f".{run_id}.tmp"

    if tmp_run.exists():
        shutil.rmtree(tmp_run, ignore_errors=True)
    if tmp_run.exists():
        raise SchedulerConfigError(f"Cannot clear temporary promotion dir: {tmp_run}")

    shutil.copytree(stage_run, tmp_run)

    if published_run.exists():
        shutil.rmtree(published_run, ignore_errors=True)
    if published_run.exists():
        raise SchedulerConfigError(f"Cannot clear existing published run dir: {published_run}")

    shutil.move(str(tmp_run), str(published_run))


def _write_run_manifest(
    *,
    data_root: Path,
    model: str,
    run_id: str,
    targets: list[tuple[str, int]],
) -> None:
    run_dt = _parse_run_id_datetime(run_id)
    if run_dt is None:
        raise SchedulerConfigError(f"Invalid run id for manifest: {run_id}")

    expected_by_var: dict[str, list[int]] = {}
    for var_id, fh in targets:
        expected_by_var.setdefault(var_id, []).append(int(fh))

    variables: dict[str, dict] = {}
    for var_id, fhs in sorted(expected_by_var.items()):
        expected_fhs = sorted(set(fhs))
        frames: list[dict] = []
        units = ""
        kind = ""

        for fh in expected_fhs:
            sidecar_path = _frame_sidecar_path(data_root, model, run_id, var_id, fh)
            if not sidecar_path.exists():
                continue
            try:
                meta = json.loads(sidecar_path.read_text())
            except (OSError, json.JSONDecodeError):
                continue

            if not units:
                units = str(meta.get("units", ""))
            if not kind:
                kind = str(meta.get("kind", ""))

            valid_time = meta.get("valid_time")
            frame_entry: dict[str, Any] = {"fh": fh}
            if isinstance(valid_time, str) and valid_time:
                frame_entry["valid_time"] = valid_time
            frames.append(frame_entry)

        variables[var_id] = {
            "kind": kind,
            "units": units,
            "expected_frames": len(expected_fhs),
            "available_frames": len(frames),
            "frames": sorted(frames, key=lambda item: item["fh"]),
        }

    payload = {
        "contract_version": "3.0",
        "model": model,
        "run": run_id,
        "variables": variables,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    manifest_path = data_root / "manifests" / model / f"{run_id}.json"
    _write_json_atomic(manifest_path, payload)


def _enforce_run_retention(root: Path, keep_runs: int) -> None:
    if keep_runs < 1 or not root.is_dir():
        return

    runs: list[tuple[datetime, Path]] = []
    for child in root.iterdir():
        if not child.is_dir() or child.name.startswith("."):
            continue
        run_dt = _parse_run_id_datetime(child.name)
        if run_dt is None:
            continue
        runs.append((run_dt, child))

    if len(runs) <= keep_runs:
        return

    runs.sort(key=lambda pair: pair[0], reverse=True)
    for _, old_run_dir in runs[keep_runs:]:
        logger.info("Removing old run dir: %s", old_run_dir)
        shutil.rmtree(old_run_dir, ignore_errors=True)


def _convert_rgba_cog_to_loop_webp(
    *,
    cog_path: Path,
    out_path: Path,
    quality: int,
    max_dim: int,
) -> bool:
    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(cog_path) as ds:
            src_h = int(ds.height)
            src_w = int(ds.width)
            max_side = max(src_h, src_w)
            if max_side <= 0:
                return False

            scale = min(1.0, float(max_dim) / float(max_side))
            out_h = max(1, int(round(src_h * scale)))
            out_w = max(1, int(round(src_w * scale)))

            data = ds.read(
                indexes=(1, 2, 3, 4),
                out_shape=(4, out_h, out_w),
                resampling=Resampling.bilinear,
            )

        rgba = np.moveaxis(data, 0, -1)
        image = Image.fromarray(rgba, mode="RGBA")
        image.save(out_path, format="WEBP", quality=quality, method=6)
        return True
    except Exception:
        logger.exception("Loop WebP conversion failed: %s -> %s", cog_path, out_path)
        return False


def _pregenerate_loop_webp_for_run(
    *,
    data_root: Path,
    model: str,
    run_id: str,
    loop_cache_root: Path,
    workers: int,
    quality: int,
    max_dim: int,
) -> tuple[int, int]:
    published_run = data_root / "published" / model / run_id
    if not published_run.is_dir():
        return 0, 0

    jobs: list[tuple[Path, Path]] = []
    for var_dir in sorted([p for p in published_run.iterdir() if p.is_dir()]):
        variable = var_dir.name
        for cog_path in sorted(var_dir.glob("fh*.rgba.cog.tif")):
            fh = cog_path.name.split(".")[0]
            out_path = loop_cache_root / model / run_id / variable / f"{fh}.loop.webp"
            if out_path.is_file():
                continue
            jobs.append((cog_path, out_path))

    if not jobs:
        return 0, 0

    logger.info(
        "Loop pre-generate start: model=%s run=%s jobs=%d workers=%d quality=%d max_dim=%d root=%s",
        model,
        run_id,
        len(jobs),
        workers,
        quality,
        max_dim,
        loop_cache_root,
    )

    ok = 0
    fail = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [
            pool.submit(
                _convert_rgba_cog_to_loop_webp,
                cog_path=cog_path,
                out_path=out_path,
                quality=quality,
                max_dim=max_dim,
            )
            for cog_path, out_path in jobs
        ]
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                ok += 1
            else:
                fail += 1

    logger.info(
        "Loop pre-generate done: model=%s run=%s success=%d failed=%d",
        model,
        run_id,
        ok,
        fail,
    )
    return ok, fail


def _process_run(
    *,
    plugin,
    model_id: str,
    vars_to_build: list[str],
    primary_vars: list[str],
    run_dt: datetime,
    data_root: Path,
    workers: int,
    keep_runs: int,
    loop_pregenerate_enabled: bool,
    loop_cache_root: Path,
    loop_workers: int,
    loop_quality: int,
    loop_max_dim: int,
) -> tuple[str, int, int]:
    run_id = _run_id_from_dt(run_dt)
    cycle_hour = run_dt.hour
    targets = _scheduled_targets_for_cycle(plugin, vars_to_build, cycle_hour)

    # Build targets sequentially per variable: if the next FH is missing,
    # do not attempt later FHs for that variable in this poll cycle.
    # Upstream publication is typically hour-by-hour, so this avoids repeated
    # futile fetches for future FHs and significantly reduces log spam.
    fhs_by_var: dict[str, list[int]] = {}
    for var_id, fh in targets:
        fhs_by_var.setdefault(var_id, []).append(int(fh))

    missing: list[tuple[str, int]] = []
    for var_id, fhs in fhs_by_var.items():
        for fh in sorted(set(fhs)):
            if _frame_artifacts_exist(data_root, model_id, run_id, var_id, fh):
                continue
            missing.append((var_id, fh))
            break

    total = len(targets)
    logger.info(
        "Run=%s model=%s coverage=%s targets=%d next_missing=%d",
        run_id,
        model_id,
        CANONICAL_COVERAGE,
        total,
        len(missing),
    )

    built_ok = 0
    if missing:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(
                    _build_one,
                    model_id=model_id,
                    var_id=var_id,
                    fh=fh,
                    run_dt=run_dt,
                    data_root=data_root,
                    plugin=plugin,
                )
                for var_id, fh in missing
            ]
            for future in concurrent.futures.as_completed(futures):
                var_id, fh, ok = future.result()
                if ok:
                    built_ok += 1
                    logger.info("Build success: %s %s fh%03d", run_id, var_id, fh)
                else:
                    logger.warning("Build skipped/failed: %s %s fh%03d", run_id, var_id, fh)

    if _should_promote(data_root, model_id, run_id, primary_vars, DEFAULT_PROMOTION_FHS):
        _promote_run(data_root, model_id, run_id)
        _write_run_manifest(
            data_root=data_root,
            model=model_id,
            run_id=run_id,
            targets=targets,
        )
        _write_latest_pointer(data_root, model_id, run_id)
        if loop_pregenerate_enabled:
            _pregenerate_loop_webp_for_run(
                data_root=data_root,
                model=model_id,
                run_id=run_id,
                loop_cache_root=loop_cache_root,
                workers=loop_workers,
                quality=loop_quality,
                max_dim=loop_max_dim,
            )

    _enforce_run_retention(data_root / "staging" / model_id, keep_runs)
    _enforce_run_retention(data_root / "published" / model_id, keep_runs)
    _enforce_run_retention(loop_cache_root / model_id, keep_runs)

    available = 0
    for var_id, fh in targets:
        if _frame_artifacts_exist(data_root, model_id, run_id, var_id, fh):
            available += 1
    return run_id, available, total


def run_scheduler(
    *,
    model: str,
    vars_to_build: list[str],
    primary_vars: list[str],
    data_root: Path,
    workers: int,
    keep_runs: int,
    poll_seconds: int,
    run_arg: str | None,
    once: bool,
    probe_var: str,
    loop_pregenerate_enabled: bool,
    loop_cache_root: Path,
    loop_workers: int,
    loop_quality: int,
    loop_max_dim: int,
) -> int:
    plugin = _resolve_model(model)
    if plugin.get_region(CANONICAL_COVERAGE) is None:
        raise SchedulerConfigError(
            f"Model {model!r} does not define canonical coverage {CANONICAL_COVERAGE!r}"
        )

    normalized_vars = _resolve_vars_to_schedule(plugin, vars_to_build)
    if not normalized_vars:
        raise SchedulerConfigError("No schedulable vars resolved")

    resolved_primary: list[str] = []
    for item in primary_vars:
        normalized = plugin.normalize_var_id(item)
        if plugin.get_var(normalized) is not None:
            resolved_primary.append(normalized)
    resolved_primary = _dedupe_preserve_order(resolved_primary)
    if not resolved_primary:
        fallback = plugin.normalize_var_id(DEFAULT_PRIMARY_VAR)
        if plugin.get_var(fallback) is not None:
            resolved_primary = [fallback]
        else:
            resolved_primary = [normalized_vars[0]]

    logger.info(
        "Scheduler starting model=%s coverage=%s vars=%s primary=%s probe_var=%s data_root=%s workers=%d poll_incomplete=%ds poll_complete=%ds",
        model,
        CANONICAL_COVERAGE,
        normalized_vars,
        resolved_primary,
        plugin.normalize_var_id(probe_var),
        data_root,
        workers,
        INCOMPLETE_RUN_POLL_SECONDS,
        poll_seconds,
    )

    last_run_id: str | None = None
    last_run_available: int = 0
    last_run_total: int = 0
    while True:
        run_dt = _resolve_run_dt(run_arg, model, plugin=plugin, probe_var=probe_var)
        run_id = _run_id_from_dt(run_dt)

        run_complete = last_run_total > 0 and last_run_available >= last_run_total
        if last_run_id == run_id and not run_arg and run_complete:
            logger.info("No new run yet (latest=%s complete); sleeping %ss", run_id, poll_seconds)
            time.sleep(poll_seconds)
            continue

        processed_run_id, available, total = _process_run(
            plugin=plugin,
            model_id=model,
            vars_to_build=normalized_vars,
            primary_vars=resolved_primary,
            run_dt=run_dt,
            data_root=data_root,
            workers=workers,
            keep_runs=keep_runs,
            loop_pregenerate_enabled=loop_pregenerate_enabled,
            loop_cache_root=loop_cache_root,
            loop_workers=loop_workers,
            loop_quality=loop_quality,
            loop_max_dim=loop_max_dim,
        )
        last_run_id = processed_run_id
        last_run_available = available
        last_run_total = total
        logger.info("Run summary: %s available=%d/%d", processed_run_id, available, total)

        if once or run_arg:
            return 0

        run_complete_now = total > 0 and available >= total
        next_poll_seconds = poll_seconds if run_complete_now else INCOMPLETE_RUN_POLL_SECONDS
        logger.info(
            "Next poll in %ss (run=%s complete=%s)",
            next_poll_seconds,
            processed_run_id,
            run_complete_now,
        )
        time.sleep(next_poll_seconds)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the V3 model scheduler.")
    parser.add_argument("--model", required=True, help="Model id (e.g. hrrr, gfs)")
    parser.add_argument("--vars", default=None, help="Comma-separated vars to build")
    parser.add_argument("--primary-vars", default=None, help="Comma-separated primary vars for promotion")
    parser.add_argument("--data-root", default=None, help="Override TWF_V3_DATA_ROOT")
    parser.add_argument("--workers", type=int, default=None, help="Parallel frame workers")
    parser.add_argument("--keep-runs", type=int, default=None, help="Retention count for staging/published runs")
    parser.add_argument("--poll-seconds", type=int, default=None, help="Poll interval in loop mode")
    parser.add_argument("--probe-var", default=None, help="Var id used to probe run availability (default: tmp2m)")
    parser.add_argument("--run", default=None, help="Explicit run id YYYYMMDD_HHz; implies one-shot")
    parser.add_argument("--once", action="store_true", help="Build one cycle then exit")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(argv)

    data_root = _data_root(args.data_root)
    workers = _workers(args.workers)
    vars_raw = args.vars if isinstance(args.vars, str) and args.vars.strip() else os.getenv(ENV_DEFAULT_VARS, DEFAULT_VARS)
    primary_raw = (
        args.primary_vars
        if isinstance(args.primary_vars, str) and args.primary_vars.strip()
        else os.getenv(ENV_DEFAULT_PRIMARY_VARS, DEFAULT_PRIMARY_VAR)
    )
    poll_seconds = (
        int(args.poll_seconds)
        if args.poll_seconds is not None
        else _int_from_env(ENV_DEFAULT_POLL_SECONDS, DEFAULT_POLL_SECONDS, min_value=15)
    )
    keep_runs = (
        int(args.keep_runs)
        if args.keep_runs is not None
        else _int_from_env(ENV_DEFAULT_KEEP_RUNS, 2, min_value=1)
    )
    probe_var = (
        args.probe_var
        if isinstance(args.probe_var, str) and args.probe_var.strip()
        else os.getenv(ENV_PROBE_VAR, DEFAULT_PROBE_VAR)
    )
    loop_pregenerate_enabled = _bool_from_env(ENV_LOOP_PREGENERATE_ENABLED, DEFAULT_LOOP_PREGENERATE_ENABLED)
    loop_cache_root = Path(os.getenv(ENV_LOOP_CACHE_ROOT, str(DEFAULT_LOOP_CACHE_ROOT))).resolve()
    loop_workers = _int_from_env(
        ENV_LOOP_PREGENERATE_WORKERS,
        DEFAULT_LOOP_PREGENERATE_WORKERS,
        min_value=1,
    )
    loop_quality = _int_from_env(ENV_LOOP_WEBP_QUALITY, DEFAULT_LOOP_WEBP_QUALITY, min_value=1)
    loop_quality = max(1, min(100, loop_quality))
    loop_max_dim = _int_from_env(ENV_LOOP_WEBP_MAX_DIM, DEFAULT_LOOP_WEBP_MAX_DIM, min_value=64)

    vars_list = _parse_vars(vars_raw)
    primary_list = _parse_vars(primary_raw)

    try:
        return run_scheduler(
            model=args.model.strip().lower(),
            vars_to_build=vars_list,
            primary_vars=primary_list,
            data_root=data_root,
            workers=workers,
            keep_runs=max(1, keep_runs),
            poll_seconds=max(15, poll_seconds),
            run_arg=args.run.strip().lower() if isinstance(args.run, str) and args.run.strip() else None,
            once=bool(args.once),
            probe_var=probe_var,
            loop_pregenerate_enabled=loop_pregenerate_enabled,
            loop_cache_root=loop_cache_root,
            loop_workers=loop_workers,
            loop_quality=loop_quality,
            loop_max_dim=loop_max_dim,
        )
    except SchedulerConfigError as exc:
        logger.error("Scheduler configuration error: %s", exc)
        return 1
    except KeyboardInterrupt:
        logger.info("Scheduler shutdown requested")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())

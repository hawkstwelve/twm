"""GRIB acquisition via Herbie.

Downloads GRIB data for a given model/variable/forecast-hour and returns
the raw numpy array along with its source CRS and affine transform.

Phase 1 scope: single-variable "simple" fetch (e.g. tmp2m, refc).
Phase 2 adds multi-component fetch for derived variables (wspd, radar_ptype).

Usage
-----
    from app.services.builder.fetch import fetch_variable

    data, crs, transform = fetch_variable(
        model_id="hrrr", product="sfc",
        search_pattern=":TMP:2 m above ground:",
        run_date=datetime(2026, 2, 17, 6),
        fh=0,
    )
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
import re
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
import rasterio.crs
import rasterio.transform
import requests

logger = logging.getLogger(__name__)

DEFAULT_HERBIE_PRIORITY = ["aws", "nomads", "google", "azure", "pando", "pando2"]
ENV_HERBIE_PRIORITY = "TWF_HERBIE_PRIORITY"
ENV_HERBIE_RETRIES = "TWF_HERBIE_SUBSET_RETRIES"
ENV_HERBIE_RETRY_SLEEP = "TWF_HERBIE_RETRY_SLEEP_SECONDS"
ENV_HERBIE_IDX_NEGATIVE_CACHE_INITIAL_TTL = "TWF_HERBIE_IDX_NEGATIVE_CACHE_INITIAL_TTL_SECONDS"
ENV_HERBIE_IDX_NEGATIVE_CACHE_MAX_TTL = "TWF_HERBIE_IDX_NEGATIVE_CACHE_MAX_TTL_SECONDS"
ENV_HERBIE_INVENTORY_CACHE_TTL = "TWF_HERBIE_INVENTORY_CACHE_TTL_SECONDS"
ENV_GRIB_DISK_CACHE_LOCK = "TWF_V3_GRIB_DISK_CACHE_LOCK"
DEFAULT_GRIB_DISK_LOCK_TIMEOUT_SECONDS = 8.0
DEFAULT_GRIB_DISK_LOCK_POLL_SECONDS = 0.1
DEFAULT_IDX_NEGATIVE_INITIAL_TTL_SECONDS = 60.0
DEFAULT_IDX_NEGATIVE_MAX_TTL_SECONDS = 300.0
DEFAULT_INVENTORY_CACHE_TTL_SECONDS = 600.0
_GRIB_DISK_CACHE_LOCK_WAITS = 0

_MISSING_VALUE_TAG_KEYS = (
    "missing_value",
    "_FillValue",
    "GRIB_missingValue",
    "GRIB_NODATA",
    "GRIB_noDataValue",
    "NODATA",
)

_INVENTORY_SEARCH_COLUMNS = (
    "search_this",
    "line",
    "inventory_line",
    "grib_message",
    "message",
)


class HerbieTransientUnavailableError(RuntimeError):
    """Raised when all Herbie attempts fail due to transient source/index availability."""


@dataclass
class _IdxNegativeCacheEntry:
    expires_at: float
    ttl_seconds: float
    updated_at: float


@dataclass
class _InventoryCacheEntry:
    data: Any
    expires_at: float
    updated_at: float


@dataclass
class _TimerAggregate:
    count: int = 0
    total_ms: float = 0.0
    max_ms: float = 0.0


@dataclass
class _InventorySearchResult:
    inventory: Any | None
    reason: str
    idx_key: str = ""


_IDX_NEGATIVE_CACHE: dict[tuple[str, str, str, int, str], _IdxNegativeCacheEntry] = {}
_IDX_NEGATIVE_CACHE_LOCK = threading.Lock()
_IDX_NEGATIVE_LOG_SUPPRESS: dict[tuple[str, str, str, int], float] = {}

_INVENTORY_CACHE: dict[str, _InventoryCacheEntry] = {}
_INVENTORY_CACHE_LOCK = threading.Lock()
_INVENTORY_INFLIGHT: dict[str, threading.Event] = {}

_FETCH_RUNTIME_COUNTERS: dict[str, int] = {}
_FETCH_RUNTIME_TIMERS_MS: dict[str, _TimerAggregate] = {}
_FETCH_RUNTIME_METRICS_LOCK = threading.Lock()


def _priority_candidates(herbie_kwargs: dict[str, Any] | None) -> list[str]:
    if herbie_kwargs and herbie_kwargs.get("priority"):
        return [str(herbie_kwargs["priority"]).strip()]

    raw = os.getenv(ENV_HERBIE_PRIORITY, "")
    if raw.strip():
        parsed = [item.strip().lower() for item in raw.split(",") if item.strip()]
        if parsed:
            return parsed
    return list(DEFAULT_HERBIE_PRIORITY)


def _priority_normalized(priority: str) -> str:
    return str(priority).strip().lower()


def _is_prs_aws_priority(*, priority: str, product: str) -> bool:
    return _priority_normalized(priority) == "aws" and str(product).strip().lower() == "prs"


def _is_idx_lag_reason(reason: str) -> bool:
    return str(reason).strip().lower() in {
        "idx_missing",
        "idx_missing_cached",
        "idx_empty",
        "idx_unparseable",
        "pattern_missing",
        "no_inventory",
    }


def _fallback_to_nomads_sequence(priority_sequence: list[str], *, current_index: int) -> list[str]:
    if current_index < 0:
        return ["nomads"]
    return list(priority_sequence[: current_index + 1]) + ["nomads"]


def _log_source_fallback(
    *,
    from_source: str,
    to_source: str,
    reason: str,
    model_id: str,
    run_date: datetime,
    fh: int,
    var_pattern: str,
) -> None:
    logger.warning(
        "SOURCE_FALLBACK from=%s to=%s reason=%s model=%s run=%s fh=%03d var=%s",
        from_source,
        to_source,
        reason,
        model_id,
        _run_id_from_date(run_date),
        int(fh),
        var_pattern,
    )


def _retry_count() -> int:
    raw = os.getenv(ENV_HERBIE_RETRIES, "2").strip()
    try:
        count = int(raw)
    except ValueError:
        return 2
    return max(1, count)


def _retry_sleep_seconds() -> float:
    raw = os.getenv(ENV_HERBIE_RETRY_SLEEP, "0.6").strip()
    try:
        value = float(raw)
    except ValueError:
        return 0.6
    return max(0.0, value)


def _float_from_env(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return max(minimum, default)
    try:
        parsed = float(raw)
    except ValueError:
        return max(minimum, default)
    return max(minimum, parsed)


def _idx_negative_initial_ttl_seconds() -> float:
    return _float_from_env(
        ENV_HERBIE_IDX_NEGATIVE_CACHE_INITIAL_TTL,
        DEFAULT_IDX_NEGATIVE_INITIAL_TTL_SECONDS,
        minimum=1.0,
    )


def _idx_negative_max_ttl_seconds() -> float:
    default_max = max(DEFAULT_IDX_NEGATIVE_MAX_TTL_SECONDS, _idx_negative_initial_ttl_seconds())
    return _float_from_env(
        ENV_HERBIE_IDX_NEGATIVE_CACHE_MAX_TTL,
        default_max,
        minimum=_idx_negative_initial_ttl_seconds(),
    )


def _inventory_cache_ttl_seconds() -> float:
    return _float_from_env(
        ENV_HERBIE_INVENTORY_CACHE_TTL,
        DEFAULT_INVENTORY_CACHE_TTL_SECONDS,
        minimum=1.0,
    )


def _metric_increment(name: str, amount: int = 1) -> None:
    metric_name = str(name).strip()
    if not metric_name:
        return
    with _FETCH_RUNTIME_METRICS_LOCK:
        _FETCH_RUNTIME_COUNTERS[metric_name] = int(_FETCH_RUNTIME_COUNTERS.get(metric_name, 0)) + int(amount)


def _metric_observe_ms(name: str, elapsed_ms: float) -> None:
    metric_name = str(name).strip()
    if not metric_name:
        return
    elapsed = max(0.0, float(elapsed_ms))
    with _FETCH_RUNTIME_METRICS_LOCK:
        aggregate = _FETCH_RUNTIME_TIMERS_MS.get(metric_name)
        if aggregate is None:
            aggregate = _TimerAggregate()
            _FETCH_RUNTIME_TIMERS_MS[metric_name] = aggregate
        aggregate.count += 1
        aggregate.total_ms += elapsed
        aggregate.max_ms = max(aggregate.max_ms, elapsed)


def get_herbie_runtime_metrics_for_tests() -> dict[str, Any]:
    """Return process-local Herbie fetch metrics (tests only)."""
    with _FETCH_RUNTIME_METRICS_LOCK:
        counters = {key: int(value) for key, value in _FETCH_RUNTIME_COUNTERS.items()}
        timers = {
            key: {
                "count": int(value.count),
                "sum_ms": float(value.total_ms),
                "avg_ms": float(value.total_ms / value.count) if value.count > 0 else 0.0,
                "max_ms": float(value.max_ms),
            }
            for key, value in _FETCH_RUNTIME_TIMERS_MS.items()
        }
    return {"counters": counters, "timers_ms": timers}


def _run_id_from_date(run_date: datetime) -> str:
    return run_date.strftime("%Y%m%d_%Hz")


def _idx_negative_key(
    *,
    model_id: str,
    run_date: datetime,
    product: str,
    fh: int,
    priority: str,
) -> tuple[str, str, str, int, str]:
    run_id = _run_id_from_date(run_date)
    return (
        str(model_id).strip().lower(),
        run_id,
        str(product).strip().lower(),
        int(fh),
        str(priority).strip().lower(),
    )


def _idx_negative_log_key(
    *,
    model_id: str,
    run_date: datetime,
    product: str,
    fh: int,
) -> tuple[str, str, str, int]:
    run_id = _run_id_from_date(run_date)
    return (
        str(model_id).strip().lower(),
        run_id,
        str(product).strip().lower(),
        int(fh),
    )


def _idx_negative_cache_remaining(cache_key: tuple[str, str, str, int, str]) -> float:
    now = time.monotonic()
    with _IDX_NEGATIVE_CACHE_LOCK:
        entry = _IDX_NEGATIVE_CACHE.get(cache_key)
        if entry is None:
            return 0.0
        if now >= entry.expires_at:
            _IDX_NEGATIVE_CACHE.pop(cache_key, None)
            return 0.0
        return max(0.0, entry.expires_at - now)


def _record_idx_negative_cache(cache_key: tuple[str, str, str, int, str]) -> float:
    now = time.monotonic()
    initial_ttl = _idx_negative_initial_ttl_seconds()
    max_ttl = _idx_negative_max_ttl_seconds()
    with _IDX_NEGATIVE_CACHE_LOCK:
        previous = _IDX_NEGATIVE_CACHE.get(cache_key)
        if previous is not None and now < previous.expires_at:
            ttl = min(max_ttl, max(initial_ttl, previous.ttl_seconds * 2.0))
        else:
            ttl = initial_ttl
        _IDX_NEGATIVE_CACHE[cache_key] = _IdxNegativeCacheEntry(
            expires_at=now + ttl,
            ttl_seconds=ttl,
            updated_at=now,
        )
    return ttl


def _log_idx_missing_once(
    *,
    model_id: str,
    run_date: datetime,
    product: str,
    fh: int,
    priority: str,
    search_pattern: str,
    ttl_seconds: float,
    source: str,
) -> None:
    now = time.monotonic()
    log_key = _idx_negative_log_key(
        model_id=model_id,
        run_date=run_date,
        product=product,
        fh=fh,
    )
    should_log = False
    with _IDX_NEGATIVE_CACHE_LOCK:
        suppress_until = _IDX_NEGATIVE_LOG_SUPPRESS.get(log_key, 0.0)
        if now >= suppress_until:
            _IDX_NEGATIVE_LOG_SUPPRESS[log_key] = now + max(1.0, ttl_seconds)
            should_log = True
    if should_log:
        logger.warning(
            "Herbie precheck unavailable (%s %s %s fh%03d; priority=%s; pattern=%s): no idx (%s; suppress=%ds)",
            model_id,
            _run_id_from_date(run_date),
            product,
            int(fh),
            priority,
            search_pattern,
            source,
            int(max(1.0, ttl_seconds)),
        )


def _record_and_log_idx_missing(
    *,
    model_id: str,
    run_date: datetime,
    product: str,
    fh: int,
    priority: str,
    search_pattern: str,
    source: str,
) -> float:
    cache_key = _idx_negative_key(
        model_id=model_id,
        run_date=run_date,
        product=product,
        fh=fh,
        priority=priority,
    )
    ttl = _record_idx_negative_cache(cache_key)
    _log_idx_missing_once(
        model_id=model_id,
        run_date=run_date,
        product=product,
        fh=fh,
        priority=priority,
        search_pattern=search_pattern,
        ttl_seconds=ttl,
        source=source,
    )
    return ttl


def _inventory_cache_key_from_idx(
    idx_ref: Any,
    *,
    priority: str = "",
    model_id: str = "",
    run_date: datetime | None = None,
    product: str = "",
    fh: int | None = None,
    grib_ref: Any = None,
) -> str:
    idx_url = str(idx_ref).strip()
    if not idx_url:
        return ""
    run_id = "-"
    if isinstance(run_date, datetime):
        run_id = _run_id_from_date(run_date)
    fh_token = "-"
    if fh is not None:
        try:
            fh_token = f"{int(fh):03d}"
        except Exception:
            fh_token = str(fh).strip() or "-"
    grib_token = str(grib_ref).strip() if grib_ref is not None else ""
    return "|".join(
        [
            _priority_normalized(priority) or "-",
            str(model_id).strip().lower() or "-",
            run_id,
            str(product).strip().lower() or "-",
            fh_token,
            idx_url,
            grib_token or "-",
        ]
    )


def _inventory_cache_get(key: str) -> Any | None:
    now = time.monotonic()
    with _INVENTORY_CACHE_LOCK:
        entry = _INVENTORY_CACHE.get(key)
        if entry is None:
            return None
        if now >= entry.expires_at:
            _INVENTORY_CACHE.pop(key, None)
            return None
        return entry.data


def _inventory_cache_set(key: str, data: Any, ttl_seconds: float) -> None:
    now = time.monotonic()
    with _INVENTORY_CACHE_LOCK:
        _INVENTORY_CACHE[key] = _InventoryCacheEntry(
            data=data,
            expires_at=now + max(1.0, ttl_seconds),
            updated_at=now,
        )


def _inventory_index_dataframe(
    H: Any,
    *,
    idx_key: str,
) -> Any | None:
    cached = _inventory_cache_get(idx_key)
    if cached is not None:
        _metric_increment("idx_cache_hit")
        return cached

    downloader = False
    inflight_event: threading.Event
    now = time.monotonic()
    with _INVENTORY_CACHE_LOCK:
        entry = _INVENTORY_CACHE.get(idx_key)
        if entry is not None and now < entry.expires_at:
            _metric_increment("idx_cache_hit")
            return entry.data
        if entry is not None and now >= entry.expires_at:
            _INVENTORY_CACHE.pop(idx_key, None)
        existing = _INVENTORY_INFLIGHT.get(idx_key)
        if existing is None:
            inflight_event = threading.Event()
            _INVENTORY_INFLIGHT[idx_key] = inflight_event
            downloader = True
            _metric_increment("idx_cache_miss")
        else:
            inflight_event = existing

    if not downloader:
        inflight_event.wait(timeout=max(5.0, _inventory_cache_ttl_seconds()))
        reused = _inventory_cache_get(idx_key)
        if reused is not None:
            _metric_increment("idx_cache_hit")
            return reused
        _metric_increment("idx_cache_miss")
        return None

    fetch_start = time.monotonic()
    try:
        dataframe = H.index_as_dataframe
        _metric_observe_ms("idx_fetch_ms", (time.monotonic() - fetch_start) * 1000.0)
        if dataframe is None:
            _metric_increment("idx_cache_error")
            return None
        try:
            dataframe_len = len(dataframe)
        except Exception:
            _metric_increment("idx_cache_error")
            raise
        if dataframe_len > 0:
            _inventory_cache_set(idx_key, dataframe, _inventory_cache_ttl_seconds())
            _metric_increment("idx_cache_store")
        return dataframe
    except Exception:
        _metric_observe_ms("idx_fetch_ms", (time.monotonic() - fetch_start) * 1000.0)
        _metric_increment("idx_cache_error")
        raise
    finally:
        with _INVENTORY_CACHE_LOCK:
            event = _INVENTORY_INFLIGHT.pop(idx_key, None)
            if event is not None:
                event.set()


def _inventory_filter(index_df: Any, search_pattern: str) -> Any | None:
    if index_df is None:
        return None
    try:
        if len(index_df) == 0:
            return index_df
    except Exception:
        return None

    pattern = str(search_pattern)
    regex_mode = True
    try:
        re.compile(pattern)
    except re.error:
        regex_mode = False

    try:
        for col in _INVENTORY_SEARCH_COLUMNS:
            if col in index_df.columns:
                series = index_df[col].astype(str)
                if regex_mode:
                    mask = series.str.contains(pattern, regex=True, na=False)
                else:
                    mask = series.str.contains(pattern, regex=False, na=False)
                subset = index_df.loc[mask]
                if len(subset) > 0:
                    return subset
        return index_df.iloc[0:0]
    except Exception:
        return None


def _inventory_search(
    H: Any,
    *,
    search_pattern: str,
    priority: str = "",
    model_id: str = "",
    run_date: datetime | None = None,
    product: str = "",
    fh: int | None = None,
) -> _InventorySearchResult:
    idx_ref: Any
    try:
        idx_ref = getattr(H, "idx", None)
    except Exception as exc:
        if _is_missing_index_error(exc):
            return _InventorySearchResult(inventory=None, reason="idx_missing")
        return _InventorySearchResult(inventory=None, reason="idx_unparseable")
    try:
        grib_ref = getattr(H, "grib", None)
    except Exception:
        grib_ref = None
    priority_token = str(priority).strip() or str(getattr(H, "priority", "") or "")
    model_token = str(model_id).strip() or str(getattr(H, "model", "") or "")
    run_token = run_date if isinstance(run_date, datetime) else getattr(H, "date", None)
    product_token = str(product).strip() or str(getattr(H, "product", "") or "")
    fh_token = fh if fh is not None else getattr(H, "fxx", None)
    idx_key = _inventory_cache_key_from_idx(
        idx_ref,
        priority=priority_token,
        model_id=model_token,
        run_date=run_token,
        product=product_token,
        fh=fh_token,
        grib_ref=grib_ref,
    )
    if not idx_key:
        return _InventorySearchResult(inventory=None, reason="idx_missing")

    try:
        index_df = _inventory_index_dataframe(H, idx_key=idx_key)
    except Exception:
        return _InventorySearchResult(inventory=None, reason="idx_unparseable", idx_key=idx_key)
    if index_df is None:
        return _InventorySearchResult(inventory=None, reason="idx_empty", idx_key=idx_key)

    try:
        if len(index_df) == 0:
            return _InventorySearchResult(inventory=index_df, reason="idx_empty", idx_key=idx_key)
    except Exception:
        return _InventorySearchResult(inventory=None, reason="idx_unparseable", idx_key=idx_key)

    parse_start = time.monotonic()
    filtered = _inventory_filter(index_df, search_pattern)
    _metric_observe_ms("idx_parse_ms", (time.monotonic() - parse_start) * 1000.0)
    if filtered is None:
        return _InventorySearchResult(inventory=None, reason="idx_unparseable", idx_key=idx_key)

    try:
        if len(filtered) == 0:
            return _InventorySearchResult(inventory=filtered, reason="pattern_missing", idx_key=idx_key)
    except Exception:
        return _InventorySearchResult(inventory=None, reason="idx_unparseable", idx_key=idx_key)
    return _InventorySearchResult(inventory=filtered, reason="ok", idx_key=idx_key)


def _inventory_lines_from_rows(inventory: Any) -> list[str]:
    if inventory is None:
        return []
    try:
        if len(inventory) == 0:
            return []
    except Exception:
        return []

    lines: list[str] = []
    for row_index in range(len(inventory)):
        try:
            row = inventory.iloc[row_index]
        except Exception:
            continue
        line = _inventory_line_from_row(row)
        if line:
            lines.append(line)
    return lines


def reset_herbie_runtime_caches_for_tests() -> None:
    """Reset process-local Herbie availability caches (tests only)."""
    with _IDX_NEGATIVE_CACHE_LOCK:
        _IDX_NEGATIVE_CACHE.clear()
        _IDX_NEGATIVE_LOG_SUPPRESS.clear()
    with _INVENTORY_CACHE_LOCK:
        _INVENTORY_CACHE.clear()
        _INVENTORY_INFLIGHT.clear()
    with _FETCH_RUNTIME_METRICS_LOCK:
        _FETCH_RUNTIME_COUNTERS.clear()
        _FETCH_RUNTIME_TIMERS_MS.clear()


def inventory_lines_for_pattern(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    search_pattern: str,
    herbie_kwargs: dict[str, Any] | None = None,
) -> list[str]:
    """Return inventory lines for a pattern with process-local cache/dedupe."""
    from herbie.core import Herbie

    kwargs = {
        "model": model_id,
        "product": product,
        "fxx": fh,
    }
    if herbie_kwargs:
        kwargs.update(herbie_kwargs)

    priority_list = [_priority_normalized(item) for item in _priority_candidates(herbie_kwargs) if str(item).strip()]
    herbie_date = run_date.replace(tzinfo=None) if run_date.tzinfo else run_date
    priority_sequence = list(priority_list)
    priority_idx = 0
    while priority_idx < len(priority_sequence):
        priority = priority_sequence[priority_idx]
        cache_key = _idx_negative_key(
            model_id=model_id,
            run_date=run_date,
            product=product,
            fh=fh,
            priority=priority,
        )
        if _idx_negative_cache_remaining(cache_key) > 0:
            priority_idx += 1
            continue
        run_kwargs = dict(kwargs)
        run_kwargs["priority"] = priority
        inv_reason = "unknown"
        try:
            H = Herbie(herbie_date, **run_kwargs)
            idx_ref = getattr(H, "idx", None)
            if not idx_ref:
                inv_reason = "idx_missing"
                _record_and_log_idx_missing(
                    model_id=model_id,
                    run_date=run_date,
                    product=product,
                    fh=fh,
                    priority=priority,
                    search_pattern=search_pattern,
                    source="inventory_lines",
                )
            else:
                inv_result = _inventory_search(
                    H,
                    search_pattern=search_pattern,
                    priority=priority,
                    model_id=model_id,
                    run_date=run_date,
                    product=product,
                    fh=fh,
                )
                inventory = inv_result.inventory
                inv_reason = inv_result.reason
                lines = _inventory_lines_from_rows(inventory)
                if lines:
                    return lines
        except Exception as exc:
            if _is_missing_index_error(exc):
                inv_reason = "idx_missing"
                _record_and_log_idx_missing(
                    model_id=model_id,
                    run_date=run_date,
                    product=product,
                    fh=fh,
                    priority=priority,
                    search_pattern=search_pattern,
                    source="inventory_lines_exception",
                )
            else:
                inv_reason = "idx_unparseable"

        if _is_prs_aws_priority(priority=priority, product=product) and _is_idx_lag_reason(inv_reason):
            _metric_increment("prs_idx_lag_count")
            _metric_increment("source_switch_count")
            _log_source_fallback(
                from_source="prs",
                to_source="nomads",
                reason="idx_lag",
                model_id=model_id,
                run_date=run_date,
                fh=fh,
                var_pattern=search_pattern,
            )
            priority_sequence = _fallback_to_nomads_sequence(priority_sequence, current_index=priority_idx)

        priority_idx += 1
    return []


def product_hour_has_any_idx(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    herbie_kwargs: dict[str, Any] | None = None,
) -> bool:
    """Cheap run-hour readiness probe using only IDX availability."""
    from herbie.core import Herbie

    kwargs = {
        "model": model_id,
        "product": product,
        "fxx": fh,
    }
    if herbie_kwargs:
        kwargs.update(herbie_kwargs)

    priority_list = _priority_candidates(herbie_kwargs)
    herbie_date = run_date.replace(tzinfo=None) if run_date.tzinfo else run_date
    all_cached_missing = True
    for priority in priority_list:
        cache_key = _idx_negative_key(
            model_id=model_id,
            run_date=run_date,
            product=product,
            fh=fh,
            priority=priority,
        )
        if _idx_negative_cache_remaining(cache_key) > 0:
            continue
        all_cached_missing = False
        run_kwargs = dict(kwargs)
        run_kwargs["priority"] = priority
        try:
            H = Herbie(herbie_date, **run_kwargs)
            idx_ref = getattr(H, "idx", None)
        except Exception as exc:
            if _is_missing_index_error(exc):
                _record_and_log_idx_missing(
                    model_id=model_id,
                    run_date=run_date,
                    product=product,
                    fh=fh,
                    priority=priority,
                    search_pattern="(readiness_probe)",
                    source="readiness_probe_exception",
                )
                continue
            logger.debug(
                "Herbie readiness probe failed (%s %s fh%03d; priority=%s): %s",
                model_id,
                product,
                int(fh),
                priority,
                exc,
            )
            return True
        if not idx_ref:
            _record_and_log_idx_missing(
                model_id=model_id,
                run_date=run_date,
                product=product,
                fh=fh,
                priority=priority,
                search_pattern="(readiness_probe)",
                source="readiness_probe",
            )
            continue
        return True
    if all_cached_missing:
        logger.warning(
            "Herbie readiness probe short-circuited (%s %s fh%03d): all priorities cached idx-missing",
            model_id,
            product,
            int(fh),
        )
    return False


def _is_missing_index_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "no index file was found for none" in text


def _is_missing_file_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "no such file or directory" in text


def _is_grib_not_found_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "grib2 file not found" in text


def _parse_float_tag(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(parsed):
        return None
    return parsed


def _bool_from_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _grib_disk_cache_lock_enabled() -> bool:
    return _bool_from_env(ENV_GRIB_DISK_CACHE_LOCK, False)


def _log_disk_lock_wait_event() -> None:
    global _GRIB_DISK_CACHE_LOCK_WAITS
    _GRIB_DISK_CACHE_LOCK_WAITS += 1
    waits = _GRIB_DISK_CACHE_LOCK_WAITS
    if waits <= 5 or waits % 25 == 0:
        logger.info("grib_disk_cache lock_waits=%d", waits)


def _subset_file_status(path: Path) -> tuple[bool, int]:
    size = 0
    try:
        if path.is_file():
            size = int(path.stat().st_size)
            return size > 0, size
    except OSError:
        pass
    return False, size


@contextmanager
def _subset_download_lock(path: Path):
    if not _grib_disk_cache_lock_enabled():
        yield
        return

    try:
        import fcntl
    except ImportError:
        logger.warning("GRIB disk-cache lock requested but fcntl is unavailable; proceeding unlocked")
        yield
        return

    lock_path = Path(f"{path}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_file = open(lock_path, "a+")
    waited = False
    deadline = time.monotonic() + DEFAULT_GRIB_DISK_LOCK_TIMEOUT_SECONDS
    try:
        while True:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                if waited:
                    _log_disk_lock_wait_event()
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out waiting for GRIB subset lock: {lock_path}")
                waited = True
                time.sleep(DEFAULT_GRIB_DISK_LOCK_POLL_SECONDS)
        yield
    finally:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        lock_file.close()


def _precheck_subset_available(
    H: Any,
    *,
    model_id: str,
    run_date: datetime,
    product: str,
    fh: int,
    search_pattern: str,
    priority: str,
    attempt_idx: int,
    retries: int,
) -> tuple[bool, str]:
    cache_key = _idx_negative_key(
        model_id=model_id,
        run_date=run_date,
        product=product,
        fh=fh,
        priority=priority,
    )
    cache_remaining = _idx_negative_cache_remaining(cache_key)
    if cache_remaining > 0.0:
        return False, "idx_missing_cached"

    try:
        idx_ref = getattr(H, "idx", None)
    except Exception as exc:
        if _is_missing_index_error(exc):
            _record_and_log_idx_missing(
                model_id=model_id,
                run_date=run_date,
                product=product,
                fh=fh,
                priority=priority,
                search_pattern=search_pattern,
                source="precheck_idx_exception",
            )
            return False, "idx_missing"
        logger.debug(
            "Herbie precheck idx introspection failed (%s fh%03d %s; priority=%s): %s",
            model_id,
            fh,
            search_pattern,
            priority,
            exc,
        )
        return True, "ok"

    if not idx_ref:
        _record_and_log_idx_missing(
            model_id=model_id,
            run_date=run_date,
            product=product,
            fh=fh,
            priority=priority,
            search_pattern=search_pattern,
            source="precheck_no_idx",
        )
        return False, "idx_missing"

    try:
        inv_result = _inventory_search(
            H,
            search_pattern=search_pattern,
            priority=priority,
            model_id=model_id,
            run_date=run_date,
            product=product,
            fh=fh,
        )
        if inv_result.reason == "ok":
            return True, "ok"
        if inv_result.reason == "idx_missing":
            _record_and_log_idx_missing(
                model_id=model_id,
                run_date=run_date,
                product=product,
                fh=fh,
                priority=priority,
                search_pattern=search_pattern,
                source="precheck_inventory_idx_missing",
            )
            return False, "idx_missing"
        if inv_result.reason == "pattern_missing":
            logger.warning(
                "Herbie precheck unavailable (%s fh%03d %s; priority=%s; attempt=%d/%d): inventory missing pattern",
                model_id,
                fh,
                search_pattern,
                priority,
                attempt_idx,
                retries,
            )
            return False, "pattern_missing"
        if inv_result.reason == "idx_empty":
            logger.warning(
                "Herbie precheck unavailable (%s fh%03d %s; priority=%s; attempt=%d/%d): empty idx",
                model_id,
                fh,
                search_pattern,
                priority,
                attempt_idx,
                retries,
            )
            return False, "idx_empty"
        if inv_result.reason == "idx_unparseable":
            logger.warning(
                "Herbie precheck unavailable (%s fh%03d %s; priority=%s; attempt=%d/%d): idx unparseable",
                model_id,
                fh,
                search_pattern,
                priority,
                attempt_idx,
                retries,
            )
            return False, "idx_unparseable"
        logger.warning(
            "Herbie precheck unavailable (%s fh%03d %s; priority=%s; attempt=%d/%d): no inventory match",
            model_id,
            fh,
            search_pattern,
            priority,
            attempt_idx,
            retries,
        )
        return False, "no_inventory"
    except Exception as exc:
        if _is_missing_index_error(exc):
            _record_and_log_idx_missing(
                model_id=model_id,
                run_date=run_date,
                product=product,
                fh=fh,
                priority=priority,
                search_pattern=search_pattern,
                source="precheck_inventory_exception",
            )
            return False, "idx_missing"
        logger.debug(
            "Herbie precheck inventory check failed; continuing with subset download (%s fh%03d %s; priority=%s): %s",
            model_id,
            fh,
            search_pattern,
            priority,
            exc,
        )
        return True, "ok"


def _inventory_line_from_row(row: Any) -> str:
    preferred_keys = (
        "search_this",
        "line",
        "inventory_line",
        "grib_message",
        "message",
    )
    for key in preferred_keys:
        try:
            value = row.get(key)
        except Exception:
            value = None
        if value is None:
            continue
        text = " ".join(str(value).split()).strip()
        if text:
            return text

    try:
        if hasattr(row, "to_dict"):
            row_dict = row.to_dict()
            pieces = [
                " ".join(str(value).split()).strip()
                for value in row_dict.values()
                if str(value).strip()
            ]
            joined = " | ".join(piece for piece in pieces if piece)
            if joined:
                return joined
    except Exception:
        pass
    return ""


def _inventory_meta_from_herbie(
    H: Any,
    *,
    search_pattern: str,
    fh: int,
    product: str,
    model_id: str = "",
    run_date: datetime | None = None,
    priority: str = "",
) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "inventory_line": "",
        "search_pattern": str(search_pattern),
        "fh": int(fh),
        "product": str(product),
    }
    inv_result = _inventory_search(
        H,
        search_pattern=search_pattern,
        priority=priority,
        model_id=model_id,
        run_date=run_date,
        product=product,
        fh=fh,
    )
    inventory = inv_result.inventory
    if inv_result.reason != "ok" or inventory is None or len(inventory) == 0:
        return meta

    try:
        row = inventory.iloc[0]
    except Exception:
        return meta

    meta["inventory_line"] = _inventory_line_from_row(row)
    return meta


def _manual_subset_download_with_corrected_range(
    H: Any,
    *,
    search_pattern: str,
    out_path: Path,
    model_id: str,
    run_date: datetime,
    product: str,
    fh: int,
    priority: str,
) -> Path | None:
    """Fallback subset fetch for edge-case index rows with duplicate start bytes.

    Some upstream IDX inventories contain duplicate `start_byte` rows (for example
    NAM 10m vector components). In those cases the first row can end up with an
    invalid computed range in Herbie's subset path and produce 0-byte output.
    This fallback computes `end_byte` from the next distinct start byte.
    """
    try:
        inv_result = _inventory_search(
            H,
            search_pattern=search_pattern,
            priority=priority,
            model_id=model_id,
            run_date=run_date,
            product=product,
            fh=fh,
        )
        inv = inv_result.inventory
    except Exception:
        inv = None

    if inv is None or len(inv) == 0:
        logger.warning(
            "Manual subset fallback unavailable (%s fh%03d %s; priority=%s): no inventory rows",
            model_id,
            fh,
            search_pattern,
            priority,
        )
        return None

    row = inv.iloc[0]
    try:
        start_byte = int(row["start_byte"])
    except Exception as exc:
        logger.warning(
            "Manual subset fallback unavailable (%s fh%03d %s; priority=%s): invalid start_byte (%s)",
            model_id,
            fh,
            search_pattern,
            priority,
            exc,
        )
        return None

    end_byte: int | None = None
    try:
        raw_end = row.get("end_byte")
        if raw_end is not None and np.isfinite(raw_end):
            parsed_end = int(raw_end)
            if parsed_end >= start_byte:
                end_byte = parsed_end
    except Exception:
        end_byte = None

    if end_byte is None:
        try:
            idx_ref = getattr(H, "idx", None)
            idx_key = _inventory_cache_key_from_idx(
                idx_ref,
                priority=priority,
                model_id=model_id,
                run_date=run_date,
                product=product,
                fh=fh,
                grib_ref=getattr(H, "grib", None),
            )
            full_idx = _inventory_index_dataframe(H, idx_key=idx_key) if idx_key else None
            if full_idx is None:
                raise RuntimeError("full idx unavailable")
            starts = (
                full_idx["start_byte"]
                .dropna()
                .astype(int)
            )
            higher = starts[starts > start_byte]
            if len(higher) > 0:
                candidate_end = int(higher.min() - 1)
                if candidate_end >= start_byte:
                    end_byte = candidate_end
        except Exception as exc:
            logger.debug(
                "Manual subset fallback full-index scan failed (%s fh%03d %s; priority=%s): %s",
                model_id,
                fh,
                search_pattern,
                priority,
                exc,
            )

    if end_byte is None or end_byte < start_byte:
        logger.warning(
            "Manual subset fallback unavailable (%s fh%03d %s; priority=%s): invalid byte range start=%s end=%s",
            model_id,
            fh,
            search_pattern,
            priority,
            start_byte,
            end_byte,
        )
        return None

    source = getattr(H, "grib", None)
    if source is None:
        logger.warning(
            "Manual subset fallback unavailable (%s fh%03d %s; priority=%s): no GRIB source URL/path",
            model_id,
            fh,
            search_pattern,
            priority,
        )
        return None

    try:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        source_str = str(source)
        if source_str.startswith(("http://", "https://")):
            headers = {"Range": f"bytes={start_byte}-{end_byte}"}
            response = requests.get(source_str, headers=headers, timeout=45)
            response.raise_for_status()
            data = response.content
            response.close()
        else:
            with open(source_str, "rb") as src:
                src.seek(start_byte)
                data = src.read(end_byte - start_byte + 1)
        if not data:
            logger.warning(
                "Manual subset fallback produced empty payload (%s fh%03d %s; priority=%s; bytes=%d-%d)",
                model_id,
                fh,
                search_pattern,
                priority,
                start_byte,
                end_byte,
            )
            return None
        out_path.write_bytes(data)
        if not out_path.is_file() or out_path.stat().st_size <= 0:
            logger.warning(
                "Manual subset fallback wrote empty file (%s fh%03d %s; priority=%s; path=%s)",
                model_id,
                fh,
                search_pattern,
                priority,
                out_path,
            )
            return None
        logger.info(
            "Downloaded GRIB via manual byte-range fallback: %s (%s fh%03d %s; priority=%s; bytes=%d-%d)",
            out_path.name,
            model_id,
            fh,
            search_pattern,
            priority,
            start_byte,
            end_byte,
        )
        return out_path
    except Exception as exc:
        logger.warning(
            "Manual subset fallback download failed (%s fh%03d %s; priority=%s): %s",
            model_id,
            fh,
            search_pattern,
            priority,
            exc,
        )
        return None


def fetch_variable(
    model_id: str,
    product: str,
    search_pattern: str,
    run_date: datetime,
    fh: int,
    *,
    herbie_kwargs: dict[str, Any] | None = None,
    return_meta: bool = False,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine] | tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine, dict[str, Any]]:
    """Fetch a single GRIB variable via Herbie and return its data.

    Downloads the GRIB subset matching *search_pattern*, then opens it
    with rasterio to extract the data array, CRS, and affine transform
    in the GRIB's native projection.

    Parameters
    ----------
    model_id : str
        Model name for Herbie (e.g. "hrrr", "gfs").
    product : str
        Herbie product string (e.g. "sfc").
    search_pattern : str
        Herbie search/regex for the GRIB message
        (e.g. ":TMP:2 m above ground:").
    run_date : datetime
        Model run initialization time (UTC).
    fh : int
        Forecast hour.
    herbie_kwargs : dict, optional
        Extra keyword arguments for the Herbie constructor
        (e.g. priority, save_dir, overwrite).

    Returns
    -------
    data : np.ndarray
        2-D float32 array in the GRIB's native projection.
    crs : rasterio.crs.CRS
        Source coordinate reference system.
    transform : rasterio.transform.Affine
        Source affine transform.

    Raises
    ------
    RuntimeError
        If the GRIB download fails or produces no data.
    """
    from herbie.core import Herbie  # lazy — not always installed

    kwargs: dict[str, Any] = {
        "model": model_id,
        "product": product,
        "fxx": fh,
    }
    if herbie_kwargs:
        kwargs.update(herbie_kwargs)

    # Herbie expects a tz-naive datetime (assumes UTC internally).
    # Strip tzinfo to avoid pandas tz-naive vs tz-aware comparison errors.
    herbie_date = run_date.replace(tzinfo=None) if run_date.tzinfo else run_date

    priority_list = [_priority_normalized(item) for item in _priority_candidates(herbie_kwargs) if str(item).strip()]
    retries = _retry_count()
    sleep_s = _retry_sleep_seconds()
    lock_enabled = _grib_disk_cache_lock_enabled()

    last_exc: Exception | None = None
    saw_missing_index = False
    saw_missing_subset_file = False
    saw_non_transient_failure = False
    grib_path: Path | None = None
    selected_meta: dict[str, Any] = {
        "inventory_line": "",
        "search_pattern": str(search_pattern),
        "fh": int(fh),
        "product": str(product),
    }
    prs_idx_lag_reason: str | None = None
    prs_fallback_triggered = False
    skipped_cached_priorities: list[tuple[str, float]] = []
    priority_sequence = list(priority_list)
    priority_idx = 0
    while priority_idx < len(priority_sequence):
        priority = priority_sequence[priority_idx]
        priority_cache_key = _idx_negative_key(
            model_id=model_id,
            run_date=run_date,
            product=product,
            fh=fh,
            priority=priority,
        )
        remaining_ttl = _idx_negative_cache_remaining(priority_cache_key)
        if remaining_ttl > 0.0:
            skipped_cached_priorities.append((priority, remaining_ttl))
            priority_idx += 1
            continue

        is_prs_aws = _is_prs_aws_priority(priority=priority, product=product)
        attempts_for_priority = 1 if is_prs_aws else retries
        force_nomads_after_prs_idx_lag = False

        for attempt_idx in range(1, attempts_for_priority + 1):
            run_kwargs = dict(kwargs)
            run_kwargs["priority"] = priority
            try:
                H = Herbie(herbie_date, **run_kwargs)
                precheck_ok, precheck_reason = _precheck_subset_available(
                    H,
                    model_id=model_id,
                    run_date=run_date,
                    product=product,
                    fh=fh,
                    search_pattern=search_pattern,
                    priority=priority,
                    attempt_idx=attempt_idx,
                    retries=attempts_for_priority,
                )
                if not precheck_ok:
                    if precheck_reason in {"idx_missing", "idx_missing_cached"}:
                        saw_missing_index = True
                        if is_prs_aws:
                            prs_idx_lag_reason = precheck_reason
                            force_nomads_after_prs_idx_lag = True
                        break
                    if is_prs_aws and _is_idx_lag_reason(precheck_reason):
                        saw_missing_index = True
                        prs_idx_lag_reason = precheck_reason
                        force_nomads_after_prs_idx_lag = True
                        break
                    saw_missing_subset_file = True
                    if sleep_s > 0 and attempt_idx < attempts_for_priority:
                        time.sleep(sleep_s)
                    continue
                attempt_meta = _inventory_meta_from_herbie(
                    H,
                    search_pattern=search_pattern,
                    fh=fh,
                    product=product,
                    model_id=model_id,
                    run_date=run_date,
                    priority=priority,
                )
                subset_hint: Path | None = None
                if lock_enabled:
                    try:
                        subset_hint = Path(H.get_localFilePath(search_pattern))
                    except Exception:
                        subset_hint = None

                if lock_enabled and subset_hint is not None:
                    with _subset_download_lock(subset_hint):
                        cached_ok, cached_size = _subset_file_status(subset_hint)
                        if cached_ok:
                            grib_path = subset_hint
                            logger.info(
                                "Reusing cached GRIB: %s (%s fh%03d %s; priority=%s; attempt=%d/%d; size=%d)",
                                grib_path.name,
                                model_id,
                                fh,
                                search_pattern,
                                priority,
                                attempt_idx,
                                attempts_for_priority,
                                cached_size,
                            )
                            selected_meta = attempt_meta
                            break

                        subset_path = H.download(search_pattern, errors="raise", overwrite=False)
                        if subset_path is None:
                            saw_missing_subset_file = True
                            logger.warning(
                                "Herbie subset unavailable: download returned None (%s fh%03d %s; priority=%s; attempt=%d/%d)",
                                model_id,
                                fh,
                                search_pattern,
                                priority,
                                attempt_idx,
                                attempts_for_priority,
                            )
                            if sleep_s > 0 and attempt_idx < attempts_for_priority:
                                time.sleep(sleep_s)
                            continue
                        subset_candidate = Path(subset_path)
                        subset_ok, subset_size = _subset_file_status(subset_candidate)
                        if not subset_ok:
                            saw_missing_subset_file = True
                            logger.warning(
                                "Herbie subset file missing/empty after download (%s fh%03d %s; priority=%s; attempt=%d/%d): %s (size=%d)",
                                model_id,
                                fh,
                                search_pattern,
                                priority,
                                attempt_idx,
                                attempts_for_priority,
                                subset_candidate,
                                subset_size,
                            )
                            manual_subset = _manual_subset_download_with_corrected_range(
                                H,
                                search_pattern=search_pattern,
                                out_path=subset_candidate,
                                model_id=model_id,
                                run_date=run_date,
                                product=product,
                                fh=fh,
                                priority=priority,
                            )
                            if manual_subset is not None:
                                grib_path = manual_subset
                                selected_meta = attempt_meta
                                break
                            try:
                                if subset_candidate.exists():
                                    subset_candidate.unlink()
                            except OSError:
                                pass
                            if sleep_s > 0 and attempt_idx < attempts_for_priority:
                                time.sleep(sleep_s)
                            continue

                        grib_path = subset_candidate
                        logger.info(
                            "Downloaded GRIB: %s (%s fh%03d %s; priority=%s; attempt=%d/%d)",
                            grib_path.name,
                            model_id,
                            fh,
                            search_pattern,
                            priority,
                            attempt_idx,
                            attempts_for_priority,
                        )
                        selected_meta = attempt_meta
                        break
                else:
                    subset_path = H.download(search_pattern, errors="raise", overwrite=True)
                    if subset_path is None:
                        saw_missing_subset_file = True
                        logger.warning(
                            "Herbie subset unavailable: download returned None (%s fh%03d %s; priority=%s; attempt=%d/%d)",
                            model_id,
                            fh,
                            search_pattern,
                            priority,
                            attempt_idx,
                            attempts_for_priority,
                        )
                        if sleep_s > 0 and attempt_idx < attempts_for_priority:
                            time.sleep(sleep_s)
                        continue
                    subset_candidate = Path(subset_path)
                    subset_ok, subset_size = _subset_file_status(subset_candidate)

                    if not subset_ok:
                        saw_missing_subset_file = True
                        logger.warning(
                            "Herbie subset file missing/empty after download (%s fh%03d %s; priority=%s; attempt=%d/%d): %s (size=%d)",
                            model_id,
                            fh,
                            search_pattern,
                            priority,
                            attempt_idx,
                            attempts_for_priority,
                            subset_candidate,
                            subset_size,
                        )
                        manual_subset = _manual_subset_download_with_corrected_range(
                            H,
                            search_pattern=search_pattern,
                            out_path=subset_candidate,
                            model_id=model_id,
                            run_date=run_date,
                            product=product,
                            fh=fh,
                            priority=priority,
                        )
                        if manual_subset is not None:
                            grib_path = manual_subset
                            selected_meta = attempt_meta
                            break
                        try:
                            if subset_candidate.exists():
                                subset_candidate.unlink()
                        except OSError:
                            pass
                        if sleep_s > 0 and attempt_idx < attempts_for_priority:
                            time.sleep(sleep_s)
                        continue

                    grib_path = subset_candidate
                    logger.info(
                        "Downloaded GRIB: %s (%s fh%03d %s; priority=%s; attempt=%d/%d)",
                        grib_path.name,
                        model_id,
                        fh,
                        search_pattern,
                        priority,
                        attempt_idx,
                        attempts_for_priority,
                    )
                    selected_meta = attempt_meta
                    break
            except Exception as exc:
                last_exc = exc
                if _is_missing_index_error(exc):
                    saw_missing_index = True
                    _record_and_log_idx_missing(
                        model_id=model_id,
                        run_date=run_date,
                        product=product,
                        fh=fh,
                        priority=priority,
                        search_pattern=search_pattern,
                        source="subset_exception_missing_idx",
                    )
                    if is_prs_aws:
                        prs_idx_lag_reason = "idx_missing_exception"
                        force_nomads_after_prs_idx_lag = True
                    break
                if _is_grib_not_found_error(exc):
                    saw_missing_subset_file = True
                    logger.warning(
                        "Herbie subset unavailable (%s fh%03d %s; priority=%s; attempt=%d/%d): grib not found",
                        model_id,
                        fh,
                        search_pattern,
                        priority,
                        attempt_idx,
                        attempts_for_priority,
                    )
                    if sleep_s > 0 and attempt_idx < attempts_for_priority:
                        time.sleep(sleep_s)
                    continue
                saw_non_transient_failure = True
                logger.warning(
                    "Herbie subset fetch failed (%s fh%03d %s; priority=%s; attempt=%d/%d): %s",
                    model_id,
                    fh,
                    search_pattern,
                    priority,
                    attempt_idx,
                    attempts_for_priority,
                    exc,
                )
                if sleep_s > 0 and attempt_idx < attempts_for_priority:
                    time.sleep(sleep_s)
        if grib_path is not None:
            break
        if force_nomads_after_prs_idx_lag:
            prs_fallback_triggered = True
            _metric_increment("prs_idx_lag_count")
            _metric_increment("source_switch_count")
            _log_source_fallback(
                from_source="prs",
                to_source="nomads",
                reason="idx_lag",
                model_id=model_id,
                run_date=run_date,
                fh=fh,
                var_pattern=search_pattern,
            )
            priority_sequence = _fallback_to_nomads_sequence(priority_sequence, current_index=priority_idx)
        priority_idx += 1

    if grib_path is None:
        if prs_fallback_triggered:
            nomads_error = str(last_exc) if last_exc is not None else "unavailable"
            raise HerbieTransientUnavailableError(
                f"Herbie PRS idx-lag fallback failed (aws->nomads) for {model_id} "
                f"run={_run_id_from_date(run_date)} product={product} fh{fh:03d} "
                f"pattern={search_pattern!r}; aws_reason={prs_idx_lag_reason or 'idx_lag'}; "
                f"nomads_error={nomads_error}"
            ) from last_exc
        if len(skipped_cached_priorities) == len(priority_list) and priority_list:
            suppress_ttl = min(ttl for _, ttl in skipped_cached_priorities)
            _log_idx_missing_once(
                model_id=model_id,
                run_date=run_date,
                product=product,
                fh=fh,
                priority="all",
                search_pattern=search_pattern,
                ttl_seconds=suppress_ttl,
                source="cached_short_circuit",
            )
            raise HerbieTransientUnavailableError(
                f"Herbie idx transiently unavailable (cached) after priorities={priority_list} "
                f"for {model_id} fh{fh:03d} pattern={search_pattern!r}"
            ) from last_exc
        if (saw_missing_index or saw_missing_subset_file) and not saw_non_transient_failure:
            raise HerbieTransientUnavailableError(
                f"Herbie subset transiently unavailable after priorities={priority_list} "
                f"for {model_id} fh{fh:03d} pattern={search_pattern!r}"
            ) from last_exc
        raise RuntimeError(
            f"Herbie subset download failed after trying priorities={priority_list} "
            f"for {model_id} fh{fh:03d} pattern={search_pattern!r}"
        ) from last_exc

    # Open with rasterio to get array + CRS + transform
    try:
        with rasterio.open(grib_path) as src:
            band_data = src.read(1, masked=True)
            data = np.asarray(np.ma.filled(band_data, np.nan), dtype=np.float32)
            band_mask = np.ma.getmaskarray(band_data)
            if band_mask is not np.ma.nomask:
                data = np.where(band_mask, np.nan, data).astype(np.float32, copy=False)

            nodata_val = _parse_float_tag(getattr(src, "nodata", None))
            if nodata_val is not None:
                atol = max(1e-6, abs(nodata_val) * 1e-6)
                data = np.where(np.isclose(data, nodata_val, rtol=0.0, atol=atol), np.nan, data).astype(np.float32, copy=False)

            tag_values: list[float] = []
            for tags in (src.tags(), src.tags(1)):
                for key in _MISSING_VALUE_TAG_KEYS:
                    parsed = _parse_float_tag(tags.get(key))
                    if parsed is not None:
                        tag_values.append(parsed)
            for missing_val in set(tag_values):
                atol = max(1e-6, abs(missing_val) * 1e-6)
                data = np.where(np.isclose(data, missing_val, rtol=0.0, atol=atol), np.nan, data).astype(np.float32, copy=False)

            # GRIB nodata sentinels occasionally leak through metadata handling.
            data = np.where(np.abs(data) > 1e12, np.nan, data).astype(np.float32, copy=False)
            crs = src.crs
            transform = src.transform
    except rasterio.errors.RasterioIOError as exc:
        if _is_missing_file_error(exc):
            raise HerbieTransientUnavailableError(
                f"Herbie subset file disappeared before open for {model_id} fh{fh:03d} "
                f"pattern={search_pattern!r} path={grib_path}"
            ) from exc
        raise

    logger.debug(
        "GRIB data: shape=%s, CRS=%s, dtype=%s",
        data.shape, crs, data.dtype,
    )

    if return_meta:
        return data, crs, transform, selected_meta
    return data, crs, transform


# ---------------------------------------------------------------------------
# Unit conversions
# Keyed by conversion id, (model_id, var_key), or legacy var_key.
# Each converter takes a float32 array (in-place safe) and returns float32.
# ---------------------------------------------------------------------------

def _celsius_to_fahrenheit(data: np.ndarray) -> np.ndarray:
    """Convert Celsius → Fahrenheit, preserving NaN.

    GDAL's GRIB driver normalizes temperatures to °C by default
    (GRIB_NORMALIZE_UNITS=YES since GDAL 2.0), so GRIB TMP fields
    arrive as °C, not Kelvin.
    """
    return data * 9.0 / 5.0 + 32.0


def _ms_to_mph(data: np.ndarray) -> np.ndarray:
    """Convert m/s → mph, preserving NaN."""
    return data * 2.23694


def _meters_to_inches(data: np.ndarray) -> np.ndarray:
    """Convert meters → inches, preserving NaN."""
    return data * 39.37007874015748


def _kgm2_to_inches(data: np.ndarray) -> np.ndarray:
    """Convert kg/m^2 liquid water equivalent → inches.

    For water, 1 kg/m^2 == 1 mm depth.
    """
    return data * 0.03937007874015748


# Registry: conversion-key -> converter function.
# Variables not listed here need no conversion (GRIB units match spec units).
# NOTE: GDAL's GRIB driver applies GRIB_NORMALIZE_UNITS=YES by default,
# so temperatures arrive in °C (not K) and wind speeds in m/s.
UNIT_CONVERTERS: dict[tuple[str, str] | str, Any] = {
    # Conversion IDs for capability metadata
    "c_to_f": _celsius_to_fahrenheit,
    "ms_to_mph": _ms_to_mph,
    "m_to_in": _meters_to_inches,
    "kgm2_to_in": _kgm2_to_inches,
    # Legacy var-key fallback path
    "tmp2m": _celsius_to_fahrenheit,
    "dp2m": _celsius_to_fahrenheit,
    "wspd10m": _ms_to_mph,
    "wgst10m": _ms_to_mph,
    "snowfall_total": _meters_to_inches,
    "precip_total": _kgm2_to_inches,
}


def convert_units(
    data: np.ndarray,
    var_key: str,
    *,
    model_id: str | None = None,
    var_capability: Any | None = None,
) -> np.ndarray:
    """Apply unit conversion for a variable if one is registered.

    Returns a new array (or the original if no conversion needed).
    """
    converter = None

    # Authoritative path: conversion id set in model capability metadata.
    conversion_id = getattr(var_capability, "conversion", None) if var_capability is not None else None
    if isinstance(conversion_id, str) and conversion_id:
        converter = UNIT_CONVERTERS.get(conversion_id)

    # Optional model-specific override fallback.
    if converter is None and isinstance(model_id, str) and model_id:
        converter = UNIT_CONVERTERS.get((model_id, var_key))

    # Legacy fallback for existing callers/vars.
    if converter is None:
        converter = UNIT_CONVERTERS.get(var_key)

    if converter is None:
        return data
    result = converter(data.astype(np.float32, copy=True))
    logger.debug("Unit conversion applied for model=%s var=%s", model_id, var_key)
    return result

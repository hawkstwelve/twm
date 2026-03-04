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

import logging
import os
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
ENV_GRIB_DISK_CACHE_LOCK = "TWF_V3_GRIB_DISK_CACHE_LOCK"
DEFAULT_GRIB_DISK_LOCK_TIMEOUT_SECONDS = 8.0
DEFAULT_GRIB_DISK_LOCK_POLL_SECONDS = 0.1
_GRIB_DISK_CACHE_LOCK_WAITS = 0

_MISSING_VALUE_TAG_KEYS = (
    "missing_value",
    "_FillValue",
    "GRIB_missingValue",
    "GRIB_NODATA",
    "GRIB_noDataValue",
    "NODATA",
)


class HerbieTransientUnavailableError(RuntimeError):
    """Raised when all Herbie attempts fail due to transient source/index availability."""


def _priority_candidates(herbie_kwargs: dict[str, Any] | None) -> list[str]:
    if herbie_kwargs and herbie_kwargs.get("priority"):
        return [str(herbie_kwargs["priority"]).strip()]

    raw = os.getenv(ENV_HERBIE_PRIORITY, "")
    if raw.strip():
        parsed = [item.strip().lower() for item in raw.split(",") if item.strip()]
        if parsed:
            return parsed
    return list(DEFAULT_HERBIE_PRIORITY)


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
    fh: int,
    search_pattern: str,
    priority: str,
    attempt_idx: int,
    retries: int,
) -> bool:
    try:
        idx_ref = getattr(H, "idx", None)
    except Exception as exc:
        if _is_missing_index_error(exc):
            logger.warning(
                "Herbie precheck unavailable (%s fh%03d %s; priority=%s; attempt=%d/%d): missing index",
                model_id,
                fh,
                search_pattern,
                priority,
                attempt_idx,
                retries,
            )
            return False
        logger.debug(
            "Herbie precheck idx introspection failed (%s fh%03d %s; priority=%s): %s",
            model_id,
            fh,
            search_pattern,
            priority,
            exc,
        )
        return True

    if not idx_ref:
        logger.warning(
            "Herbie precheck unavailable (%s fh%03d %s; priority=%s; attempt=%d/%d): no idx",
            model_id,
            fh,
            search_pattern,
            priority,
            attempt_idx,
            retries,
        )
        return False

    try:
        inventory = H.inventory(search_pattern)
        if inventory is None or len(inventory) == 0:
            logger.warning(
                "Herbie precheck unavailable (%s fh%03d %s; priority=%s; attempt=%d/%d): no inventory match",
                model_id,
                fh,
                search_pattern,
                priority,
                attempt_idx,
                retries,
            )
            return False
        return True
    except Exception as exc:
        if _is_missing_index_error(exc):
            logger.warning(
                "Herbie precheck unavailable (%s fh%03d %s; priority=%s; attempt=%d/%d): missing index",
                model_id,
                fh,
                search_pattern,
                priority,
                attempt_idx,
                retries,
            )
            return False
        logger.debug(
            "Herbie precheck inventory check failed; continuing with subset download (%s fh%03d %s; priority=%s): %s",
            model_id,
            fh,
            search_pattern,
            priority,
            exc,
        )
        return True


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


def _inventory_meta_from_herbie(H: Any, *, search_pattern: str, fh: int, product: str) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "inventory_line": "",
        "search_pattern": str(search_pattern),
        "fh": int(fh),
        "product": str(product),
    }
    try:
        inventory = H.inventory(search_pattern)
    except Exception:
        return meta
    if inventory is None or len(inventory) == 0:
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
        inv = H.inventory(search_pattern)
    except Exception as exc:
        logger.warning(
            "Manual subset fallback inventory failed (%s fh%03d %s; priority=%s): %s",
            model_id,
            fh,
            search_pattern,
            priority,
            exc,
        )
        return None

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
            full_idx = H.index_as_dataframe
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

    priority_list = _priority_candidates(herbie_kwargs)
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
    for priority in priority_list:
        for attempt_idx in range(1, retries + 1):
            run_kwargs = dict(kwargs)
            run_kwargs["priority"] = priority
            try:
                H = Herbie(herbie_date, **run_kwargs)
                if not _precheck_subset_available(
                    H,
                    model_id=model_id,
                    fh=fh,
                    search_pattern=search_pattern,
                    priority=priority,
                    attempt_idx=attempt_idx,
                    retries=retries,
                ):
                    saw_missing_index = True
                    if sleep_s > 0 and attempt_idx < retries:
                        time.sleep(sleep_s)
                    continue
                attempt_meta = _inventory_meta_from_herbie(
                    H,
                    search_pattern=search_pattern,
                    fh=fh,
                    product=product,
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
                                retries,
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
                                retries,
                            )
                            if sleep_s > 0 and attempt_idx < retries:
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
                                retries,
                                subset_candidate,
                                subset_size,
                            )
                            manual_subset = _manual_subset_download_with_corrected_range(
                                H,
                                search_pattern=search_pattern,
                                out_path=subset_candidate,
                                model_id=model_id,
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
                            if sleep_s > 0 and attempt_idx < retries:
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
                            retries,
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
                            retries,
                        )
                        if sleep_s > 0 and attempt_idx < retries:
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
                            retries,
                            subset_candidate,
                            subset_size,
                        )
                        manual_subset = _manual_subset_download_with_corrected_range(
                            H,
                            search_pattern=search_pattern,
                            out_path=subset_candidate,
                            model_id=model_id,
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
                        if sleep_s > 0 and attempt_idx < retries:
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
                        retries,
                    )
                    selected_meta = attempt_meta
                    break
            except Exception as exc:
                last_exc = exc
                if _is_missing_index_error(exc):
                    saw_missing_index = True
                    logger.warning(
                        "Herbie subset unavailable (%s fh%03d %s; priority=%s; attempt=%d/%d): missing index",
                        model_id,
                        fh,
                        search_pattern,
                        priority,
                        attempt_idx,
                        retries,
                    )
                    if sleep_s > 0 and attempt_idx < retries:
                        time.sleep(sleep_s)
                    continue
                if _is_grib_not_found_error(exc):
                    saw_missing_subset_file = True
                    logger.warning(
                        "Herbie subset unavailable (%s fh%03d %s; priority=%s; attempt=%d/%d): grib not found",
                        model_id,
                        fh,
                        search_pattern,
                        priority,
                        attempt_idx,
                        retries,
                    )
                    if sleep_s > 0 and attempt_idx < retries:
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
                    retries,
                    exc,
                )
                if sleep_s > 0 and attempt_idx < retries:
                    time.sleep(sleep_s)
        if grib_path is not None:
            break

    if grib_path is None:
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

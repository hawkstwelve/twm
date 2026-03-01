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
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
import rasterio.crs
import rasterio.transform

logger = logging.getLogger(__name__)

DEFAULT_HERBIE_PRIORITY = ["aws", "nomads", "google", "azure", "pando", "pando2"]
ENV_HERBIE_PRIORITY = "TWF_HERBIE_PRIORITY"
ENV_HERBIE_RETRIES = "TWF_HERBIE_SUBSET_RETRIES"
ENV_HERBIE_RETRY_SLEEP = "TWF_HERBIE_RETRY_SLEEP_SECONDS"

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


def fetch_variable(
    model_id: str,
    product: str,
    search_pattern: str,
    run_date: datetime,
    fh: int,
    *,
    herbie_kwargs: dict[str, Any] | None = None,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
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

    last_exc: Exception | None = None
    saw_missing_index = False
    saw_missing_subset_file = False
    saw_non_transient_failure = False
    grib_path: Path | None = None
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
                subset_ok = False
                subset_size = 0
                try:
                    if subset_candidate.is_file():
                        subset_size = int(subset_candidate.stat().st_size)
                        subset_ok = subset_size > 0
                except OSError:
                    subset_ok = False

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

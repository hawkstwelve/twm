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
    grib_path: Path | None = None
    for priority in priority_list:
        for attempt_idx in range(1, retries + 1):
            run_kwargs = dict(kwargs)
            run_kwargs["priority"] = priority
            try:
                H = Herbie(herbie_date, **run_kwargs)
                subset_path = H.download(search_pattern)
                if subset_path is None:
                    raise RuntimeError(
                        f"Herbie subset download returned None ({model_id} fh{fh:03d} {search_pattern!r})"
                    )
                grib_path = Path(subset_path)
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
        raise RuntimeError(
            f"Herbie subset download failed after trying priorities={priority_list} "
            f"for {model_id} fh{fh:03d} pattern={search_pattern!r}"
        ) from last_exc

    # Open with rasterio to get array + CRS + transform
    with rasterio.open(grib_path) as src:
        data = src.read(1).astype(np.float32)
        crs = src.crs
        transform = src.transform

    logger.debug(
        "GRIB data: shape=%s, CRS=%s, dtype=%s",
        data.shape, crs, data.dtype,
    )

    return data, crs, transform


# ---------------------------------------------------------------------------
# Unit conversions
# Keyed by (model, var_id) or just var_id for model-agnostic conversions.
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


# Registry: var_id → converter function
# Variables not listed here need no conversion (GRIB units match spec units).
# NOTE: GDAL's GRIB driver applies GRIB_NORMALIZE_UNITS=YES by default,
# so temperatures arrive in °C (not K) and wind speeds in m/s.
UNIT_CONVERTERS: dict[str, Any] = {
    "tmp2m": _celsius_to_fahrenheit,
    "wspd10m": _ms_to_mph,
}


def convert_units(data: np.ndarray, var_id: str) -> np.ndarray:
    """Apply unit conversion for a variable if one is registered.

    Returns a new array (or the original if no conversion needed).
    """
    converter = UNIT_CONVERTERS.get(var_id)
    if converter is None:
        return data
    result = converter(data.astype(np.float32, copy=True))
    logger.debug("Unit conversion applied for %s", var_id)
    return result

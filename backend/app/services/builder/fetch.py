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
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
import rasterio.crs
import rasterio.transform

logger = logging.getLogger(__name__)


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

    H = Herbie(run_date, **kwargs)

    # Download the subset matching the search pattern
    grib_path = H.download(search_pattern)
    if grib_path is None:
        raise RuntimeError(
            f"Herbie download failed: {model_id} fh{fh:03d} "
            f"pattern={search_pattern!r}"
        )
    grib_path = Path(grib_path)
    logger.info(
        "Downloaded GRIB: %s (%s fh%03d %s)",
        grib_path.name, model_id, fh, search_pattern,
    )

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

def _kelvin_to_fahrenheit(data: np.ndarray) -> np.ndarray:
    """Convert Kelvin → Fahrenheit, preserving NaN."""
    return (data - 273.15) * 9.0 / 5.0 + 32.0


def _ms_to_mph(data: np.ndarray) -> np.ndarray:
    """Convert m/s → mph, preserving NaN."""
    return data * 2.23694


# Registry: var_id → converter function
# Variables not listed here need no conversion (GRIB units match spec units).
UNIT_CONVERTERS: dict[str, Any] = {
    "tmp2m": _kelvin_to_fahrenheit,
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

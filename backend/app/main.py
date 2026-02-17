"""TWF V3 API — Discovery + Sampling endpoints."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import rasterio
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pyproj import Transformer

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.environ.get("TWF_V3_DATA_ROOT", "./data/v3"))

app = FastAPI(title="TWF V3 API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Reusable WGS84 → Web Mercator transformer (thread-safe, cached)
_wgs84_to_3857 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)


def _resolve_val_cog(model: str, region: str, run: str, var: str, fh: int) -> Path | None:
    """Find the float32 value COG on disk (published first, then staging)."""
    filename = f"fh{fh:03d}.val.cog.tif"
    for prefix in ("published", "staging"):
        candidate = DATA_ROOT / prefix / model / region / run / var / filename
        if candidate.is_file():
            return candidate
    return None


def _resolve_sidecar(model: str, region: str, run: str, var: str, fh: int) -> dict | None:
    """Load sidecar JSON for units/metadata (published first, then staging)."""
    filename = f"fh{fh:03d}.json"
    for prefix in ("published", "staging"):
        candidate = DATA_ROOT / prefix / model / region / run / var / filename
        if candidate.is_file():
            try:
                return json.loads(candidate.read_text())
            except Exception:
                return None
    return None


@app.get("/api/v3/health")
def health():
    return {"ok": True, "data_root": str(DATA_ROOT)}


@app.get("/api/v3")
def root():
    return {"service": "twf-v3-api", "version": "1.0.0"}


@app.get("/api/v3/sample")
def sample(
    model: str = Query(..., description="Model ID (e.g. hrrr)"),
    region: str = Query(..., description="Region ID (e.g. pnw)"),
    run: str = Query(..., description="Run ID (e.g. 20260217_20z)"),
    var: str = Query(..., description="Variable ID (e.g. tmp2m)"),
    fh: int = Query(..., description="Forecast hour"),
    lat: float = Query(..., ge=-90, le=90, description="Latitude (WGS84)"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude (WGS84)"),
):
    """Point query against value-grid COG for hover-for-data.

    Transforms (lat, lon) from WGS84 to the COG's EPSG:3857 CRS,
    reads the nearest pixel value, and returns it with units from
    the sidecar JSON.
    """
    val_cog = _resolve_val_cog(model, region, run, var, fh)
    if val_cog is None:
        return Response(status_code=404, content='{"error": "COG not found"}',
                        media_type="application/json")

    try:
        # Project WGS84 → Web Mercator
        mx, my = _wgs84_to_3857.transform(lon, lat)

        with rasterio.open(val_cog) as ds:
            # Convert map coordinates to pixel row/col
            row, col = ds.index(mx, my)

            # Bounds check
            if row < 0 or row >= ds.height or col < 0 or col >= ds.width:
                return Response(
                    status_code=204,
                    headers={"Cache-Control": "public, max-age=15"},
                )

            # Read single pixel
            window = rasterio.windows.Window(col, row, 1, 1)
            pixel = ds.read(1, window=window)
            value = float(pixel[0, 0])

        # NaN / nodata → 204
        if np.isnan(value):
            return Response(
                status_code=204,
                headers={"Cache-Control": "public, max-age=15"},
            )

        # Round to reasonable precision
        value = round(value, 1)

        # Get metadata from sidecar
        sidecar = _resolve_sidecar(model, region, run, var, fh)
        units = sidecar.get("units", "") if sidecar else ""
        valid_time = sidecar.get("valid_time", "") if sidecar else ""

        return {
            "value": value,
            "units": units,
            "model": model,
            "var": var,
            "fh": fh,
            "valid_time": valid_time,
            "lat": lat,
            "lon": lon,
        }

    except Exception:
        logger.exception("Sample query failed: %s/%s/%s/%s/fh%03d @ (%.4f, %.4f)",
                         model, region, run, var, fh, lat, lon)
        return Response(status_code=500, content='{"error": "internal error"}',
                        media_type="application/json")

"""TWF V3 API — Discovery + Sampling endpoints."""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import rasterio
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pyproj import Transformer

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.environ.get("TWF_V3_DATA_ROOT", "./data/v3"))

# Model display names
MODEL_NAMES = {
    "hrrr": "HRRR",
    "gfs": "GFS",
    "ecmwf": "ECMWF",
    "nam": "NAM",
}

# Regex to match run IDs like 20260217_20z
_RUN_ID_RE = re.compile(r"^\d{8}_\d{2}z$")

app = FastAPI(title="TWF V3 API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Reusable WGS84 → Web Mercator transformer (thread-safe, cached)
_wgs84_to_3857 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)


# ---------------------------------------------------------------------------
# Helpers — filesystem scanning + "latest" resolution
# ---------------------------------------------------------------------------

def _scan_subdirs(parent: Path) -> list[str]:
    """Return sorted list of immediate subdirectory names under *parent*,
    checking both published/ and staging/ prefixes."""
    names: set[str] = set()
    for prefix in ("published", "staging"):
        d = DATA_ROOT / prefix / parent
        if d.is_dir():
            for child in d.iterdir():
                if child.is_dir():
                    names.add(child.name)
    return sorted(names)


def _resolve_latest_run(model: str, region: str) -> str | None:
    """Find the latest (lexicographically greatest) run ID for a model/region.

    Checks published/ first, then staging/.  Run dirs match YYYYMMDD_HHz pattern.
    """
    runs: list[str] = []
    for prefix in ("published", "staging"):
        d = DATA_ROOT / prefix / model / region
        if d.is_dir():
            for child in d.iterdir():
                if child.is_dir() and _RUN_ID_RE.match(child.name):
                    runs.append(child.name)
    if not runs:
        return None
    return sorted(set(runs))[-1]  # Latest by lexicographic sort (YYYYMMDD_HHz)


def _resolve_run(model: str, region: str, run: str) -> str | None:
    """Resolve a run value — if 'latest', find the actual latest run ID."""
    if run == "latest":
        return _resolve_latest_run(model, region)
    return run


# ---------------------------------------------------------------------------
# Discovery endpoints
# ---------------------------------------------------------------------------

@app.get("/api/v3/health")
def health():
    return {"ok": True, "data_root": str(DATA_ROOT)}


@app.get("/api/v3")
def root():
    return {"service": "twf-v3-api", "version": "1.0.0"}


@app.get("/api/v3/models")
def list_models():
    """List available models by scanning the data directory."""
    model_ids = _scan_subdirs(Path(""))
    return [
        {"id": m, "name": MODEL_NAMES.get(m, m.upper())}
        for m in model_ids
    ]


@app.get("/api/v3/{model}/regions")
def list_regions(model: str):
    """List available regions for a model."""
    return _scan_subdirs(Path(model))


@app.get("/api/v3/{model}/{region}/runs")
def list_runs(model: str, region: str):
    """List available runs for a model/region, newest first."""
    runs: list[str] = []
    for prefix in ("published", "staging"):
        d = DATA_ROOT / prefix / model / region
        if d.is_dir():
            for child in d.iterdir():
                if child.is_dir() and _RUN_ID_RE.match(child.name):
                    runs.append(child.name)
    return sorted(set(runs), reverse=True)  # newest first


@app.get("/api/v3/{model}/{region}/{run}/vars")
def list_vars(model: str, region: str, run: str):
    """List available variables for a model/region/run."""
    resolved = _resolve_run(model, region, run)
    if resolved is None:
        return Response(status_code=404, content='{"error": "no runs found"}',
                        media_type="application/json")

    var_ids: set[str] = set()
    for prefix in ("published", "staging"):
        d = DATA_ROOT / prefix / model / region / resolved
        if d.is_dir():
            for child in d.iterdir():
                if child.is_dir():
                    var_ids.add(child.name)

    # Import VAR_SPECS for display names
    from .services.colormaps import VAR_SPECS
    result = []
    for v in sorted(var_ids):
        spec = VAR_SPECS.get(v, {})
        result.append({
            "id": v,
            "display_name": spec.get("display_name", v),
        })
    return result


@app.get("/api/v3/{model}/{region}/{run}/{var}/frames")
def list_frames(model: str, region: str, run: str, var: str):
    """List available frames for a model/region/run/variable."""
    resolved = _resolve_run(model, region, run)
    if resolved is None:
        return Response(status_code=404, content='{"error": "no runs found"}',
                        media_type="application/json")

    # Scan for fhNNN.rgba.cog.tif files
    frames: list[dict] = []
    seen_fh: set[int] = set()

    for prefix in ("published", "staging"):
        d = DATA_ROOT / prefix / model / region / resolved / var
        if not d.is_dir():
            continue
        for f in d.iterdir():
            if not f.name.endswith(".rgba.cog.tif"):
                continue
            m = re.match(r"^fh(\d{3})\.rgba\.cog\.tif$", f.name)
            if not m:
                continue
            fh = int(m.group(1))
            if fh in seen_fh:
                continue
            seen_fh.add(fh)

            # Load sidecar JSON for metadata if available
            sidecar_path = f.parent / f"fh{fh:03d}.json"
            meta = None
            if sidecar_path.is_file():
                try:
                    meta = json.loads(sidecar_path.read_text())
                except Exception:
                    pass

            frames.append({
                "fh": fh,
                "has_cog": True,
                "run": resolved,
                "meta": {"meta": meta},
            })

    frames.sort(key=lambda x: x["fh"])
    return frames


# ---------------------------------------------------------------------------
# Helpers — COG / sidecar resolution (with latest support)
# ---------------------------------------------------------------------------

def _resolve_val_cog(model: str, region: str, run: str, var: str, fh: int) -> Path | None:
    """Find the float32 value COG on disk (published first, then staging)."""
    resolved = _resolve_run(model, region, run) or run
    filename = f"fh{fh:03d}.val.cog.tif"
    for prefix in ("published", "staging"):
        candidate = DATA_ROOT / prefix / model / region / resolved / var / filename
        if candidate.is_file():
            return candidate
    return None


def _resolve_sidecar(model: str, region: str, run: str, var: str, fh: int) -> dict | None:
    """Load sidecar JSON for units/metadata (published first, then staging)."""
    resolved = _resolve_run(model, region, run) or run
    filename = f"fh{fh:03d}.json"
    for prefix in ("published", "staging"):
        candidate = DATA_ROOT / prefix / model / region / resolved / var / filename
        if candidate.is_file():
            try:
                return json.loads(candidate.read_text())
            except Exception:
                return None
    return None


# ---------------------------------------------------------------------------
# Sample endpoint
# ---------------------------------------------------------------------------

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

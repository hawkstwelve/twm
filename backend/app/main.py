"""TWF V3 API — Discovery + Sampling endpoints."""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

import numpy as np
import rasterio
from rasterio.windows import Window
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pyproj import Transformer

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.environ.get("TWF_V3_DATA_ROOT", "./data/v3"))
PUBLISHED_ROOT = DATA_ROOT / "published"

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
# Caches — keep COG datasets + sidecar JSON hot for hover sampling
# ---------------------------------------------------------------------------

_ds_cache: dict[str, rasterio.DatasetReader] = {}  # path → open dataset
_ds_cache_lock = threading.Lock()
_DS_CACHE_MAX = 16  # keep at most 16 open datasets


def _get_cached_dataset(path: Path) -> rasterio.DatasetReader:
    """Return an open rasterio dataset, reusing from cache if possible."""
    key = str(path)
    with _ds_cache_lock:
        ds = _ds_cache.get(key)
        if ds is not None and not ds.closed:
            return ds
        # Evict oldest if at capacity
        if len(_ds_cache) >= _DS_CACHE_MAX:
            evict_key = next(iter(_ds_cache))
            try:
                _ds_cache.pop(evict_key).close()
            except Exception:
                _ds_cache.pop(evict_key, None)
        ds = rasterio.open(path)
        _ds_cache[key] = ds
        return ds


@lru_cache(maxsize=128)
def _cached_sidecar(path_str: str) -> dict | None:
    """Read and cache sidecar JSON. Keyed by string path for lru_cache."""
    try:
        return json.loads(Path(path_str).read_text())
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Helpers — filesystem scanning + "latest" resolution
# ---------------------------------------------------------------------------

def _scan_subdirs(parent: Path) -> list[str]:
    """Return sorted list of immediate subdirectory names under published/*parent*."""
    d = PUBLISHED_ROOT / parent
    if not d.is_dir():
        return []
    return sorted(child.name for child in d.iterdir() if child.is_dir())


def _latest_run_from_pointer(model: str, region: str) -> str | None:
    """Return run_id from published LATEST.json if valid and present on disk."""
    latest_path = PUBLISHED_ROOT / model / region / "LATEST.json"
    if not latest_path.is_file():
        return None
    try:
        payload = json.loads(latest_path.read_text())
    except Exception:
        logger.warning("Failed reading LATEST.json at %s", latest_path)
        return None

    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or not _RUN_ID_RE.match(run_id):
        logger.warning("Invalid run_id in LATEST.json at %s: %r", latest_path, run_id)
        return None

    run_dir = PUBLISHED_ROOT / model / region / run_id
    if not run_dir.is_dir():
        logger.warning("LATEST.json run_id does not exist on disk: %s", run_dir)
        return None
    return run_id


def _resolve_latest_run(model: str, region: str) -> str | None:
    """Find latest published run ID for model/region.

    Preference order:
    1) published/{model}/{region}/LATEST.json run_id (if valid)
    2) lexicographically greatest run directory in published/
    """
    pointed = _latest_run_from_pointer(model, region)
    if pointed is not None:
        return pointed

    d = PUBLISHED_ROOT / model / region
    if not d.is_dir():
        return None

    runs = [
        child.name
        for child in d.iterdir()
        if child.is_dir() and _RUN_ID_RE.match(child.name)
    ]
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
    """List available published runs for a model/region, newest first."""
    d = PUBLISHED_ROOT / model / region
    if not d.is_dir():
        return []

    runs = [
        child.name
        for child in d.iterdir()
        if child.is_dir() and _RUN_ID_RE.match(child.name)
    ]
    return sorted(set(runs), reverse=True)  # newest first


@app.get("/api/v3/{model}/{region}/{run}/vars")
def list_vars(model: str, region: str, run: str):
    """List available variables for a model/region/run."""
    resolved = _resolve_run(model, region, run)
    if resolved is None:
        return Response(status_code=404, content='{"error": "no runs found"}',
                        media_type="application/json")

    var_ids: set[str] = set()
    d = PUBLISHED_ROOT / model / region / resolved
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

    d = PUBLISHED_ROOT / model / region / resolved / var
    if d.is_dir():
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
    """Find the float32 value COG on disk in published."""
    resolved = _resolve_run(model, region, run) or run
    filename = f"fh{fh:03d}.val.cog.tif"
    candidate = PUBLISHED_ROOT / model / region / resolved / var / filename
    if candidate.is_file():
        return candidate
    return None


def _resolve_sidecar(model: str, region: str, run: str, var: str, fh: int) -> dict | None:
    """Load sidecar JSON for units/metadata from published. Cached."""
    resolved = _resolve_run(model, region, run) or run
    filename = f"fh{fh:03d}.json"
    candidate = PUBLISHED_ROOT / model / region / resolved / var / filename
    if candidate.is_file():
        return _cached_sidecar(str(candidate))
    return None


def _resolve_frame_var_dir(model: str, region: str, run: str, var: str, fh: int) -> Path | None:
    """Resolve the directory containing the requested frame artifacts.

    Uses the same precedence as frame discovery: published only.
    """
    resolved = _resolve_run(model, region, run)
    if resolved is None:
        return None

    fh_str = f"fh{fh:03d}"
    var_dir = PUBLISHED_ROOT / model / region / resolved / var
    if var_dir.is_dir():
        frame_cog = var_dir / f"{fh_str}.rgba.cog.tif"
        if frame_cog.is_file():
            return var_dir
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

        ds = _get_cached_dataset(val_cog)
        # Convert map coordinates to pixel row/col
        row, col = ds.index(mx, my)

        # Bounds check
        if row < 0 or row >= ds.height or col < 0 or col >= ds.width:
            return Response(
                status_code=204,
                headers={"Cache-Control": "public, max-age=15"},
            )

        # Read single pixel
        window = Window(col, row, 1, 1)  # type: ignore[call-arg]
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


@app.get("/api/v3/{model}/{region}/{run}/{var}/{fh:int}/contours/{key}")
def get_contour_geojson(
    model: str,
    region: str,
    run: str,
    var: str,
    fh: int,
    key: str,
):
    """Return a precomputed contour GeoJSON for a frame.

    Resolves the frame directory using published-only lookup, then loads
    sidecar + contour path from that exact var directory.
    """
    var_dir = _resolve_frame_var_dir(model, region, run, var, fh)
    if var_dir is None:
        raise HTTPException(status_code=404, detail="Frame not found")

    sidecar_path = var_dir / f"fh{fh:03d}.json"
    if not sidecar_path.is_file():
        raise HTTPException(status_code=404, detail="Sidecar not found")

    try:
        sidecar = json.loads(sidecar_path.read_text())
    except Exception as exc:
        logger.exception(
            "Failed to read sidecar for contour: %s/%s/%s/%s/fh%03d (%s)",
            model,
            region,
            run,
            var,
            fh,
            sidecar_path,
        )
        raise HTTPException(status_code=500, detail=f"Failed to read sidecar: {exc}") from exc

    contours = sidecar.get("contours")
    if not isinstance(contours, dict) or key not in contours:
        raise HTTPException(status_code=404, detail=f"Contour '{key}' not found")

    contour_meta = contours[key]
    contour_rel_path = contour_meta.get("path") if isinstance(contour_meta, dict) else None
    if not isinstance(contour_rel_path, str) or not contour_rel_path:
        raise HTTPException(status_code=500, detail=f"Contour '{key}' has invalid sidecar path")

    contour_path = var_dir / contour_rel_path
    if not contour_path.is_file():
        raise HTTPException(status_code=404, detail=f"Contour file missing: {contour_rel_path}")

    try:
        return json.loads(contour_path.read_text())
    except Exception as exc:
        logger.exception(
            "Failed to read contour GeoJSON: %s/%s/%s/%s/fh%03d/%s (%s)",
            model,
            region,
            run,
            var,
            fh,
            key,
            contour_path,
        )
        raise HTTPException(status_code=500, detail=f"Failed to read contour GeoJSON: {exc}") from exc

"""TWF V3 API — canonical discovery + sampling endpoints."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
from functools import lru_cache
from pathlib import Path

import numpy as np
import rasterio
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pyproj import Transformer
from rasterio.windows import Window

from .config.regions import REGION_PRESETS

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.environ.get("TWF_V3_DATA_ROOT", "./data/v3"))
PUBLISHED_ROOT = DATA_ROOT / "published"
MANIFESTS_ROOT = DATA_ROOT / "manifests"

MODEL_NAMES = {
    "hrrr": "HRRR",
    "gfs": "GFS",
    "ecmwf": "ECMWF",
    "nam": "NAM",
}

VAR_ORDER_BY_MODEL = {
    "hrrr": [
        "tmp2m",
        "tmp850",
        "precip_total",
        "snowfall_total",
        "wspd10m",
        "radar_ptype",
    ],
}

_RUN_ID_RE = re.compile(r"^\d{8}_\d{2}z$")


def _if_none_match_values(header_value: str) -> list[str]:
    return [v.strip() for v in header_value.split(",") if v.strip()]


def _etag_matches(if_none_match: str | None, etag: str) -> bool:
    if not if_none_match:
        return False
    vals = _if_none_match_values(if_none_match)
    if "*" in vals:
        return True
    return etag in vals


def _make_etag(payload: object) -> str:
    digest = hashlib.md5(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:12]
    return f'"{digest}"'


def _maybe_304(request: Request, *, etag: str, cache_control: str) -> Response | None:
    inm = request.headers.get("if-none-match")
    if _etag_matches(inm, etag):
        return Response(
            status_code=304,
            headers={
                "ETag": etag,
                "Cache-Control": cache_control,
            },
        )
    return None


app = FastAPI(title="TWF V3 API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_wgs84_to_3857 = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)

_ds_cache: dict[str, rasterio.DatasetReader] = {}
_ds_cache_lock = threading.Lock()
_DS_CACHE_MAX = 16


@lru_cache(maxsize=256)
def _cached_sidecar(path_str: str) -> dict | None:
    try:
        return json.loads(Path(path_str).read_text())
    except Exception:
        return None


@lru_cache(maxsize=256)
def _cached_manifest(path_str: str) -> dict | None:
    try:
        return json.loads(Path(path_str).read_text())
    except Exception:
        return None


def _get_cached_dataset(path: Path) -> rasterio.DatasetReader:
    key = str(path)
    with _ds_cache_lock:
        ds = _ds_cache.get(key)
        if ds is not None and not ds.closed:
            return ds
        if len(_ds_cache) >= _DS_CACHE_MAX:
            evict_key = next(iter(_ds_cache))
            try:
                _ds_cache.pop(evict_key).close()
            except Exception:
                _ds_cache.pop(evict_key, None)
        ds = rasterio.open(path)
        _ds_cache[key] = ds
        return ds


def _latest_run_from_pointer(model: str) -> str | None:
    latest_path = PUBLISHED_ROOT / model / "LATEST.json"
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

    run_dir = PUBLISHED_ROOT / model / run_id
    manifest_path = MANIFESTS_ROOT / model / f"{run_id}.json"
    if not run_dir.is_dir() or not manifest_path.is_file():
        logger.warning("LATEST.json points to incomplete run state for %s/%s", model, run_id)
        return None
    return run_id


def _scan_manifest_runs(model: str) -> list[str]:
    model_manifest_dir = MANIFESTS_ROOT / model
    if not model_manifest_dir.is_dir():
        return []
    runs: list[str] = []
    for file_path in model_manifest_dir.glob("*.json"):
        run_id = file_path.stem
        if not _RUN_ID_RE.match(run_id):
            continue
        if not (PUBLISHED_ROOT / model / run_id).is_dir():
            continue
        runs.append(run_id)
    return sorted(set(runs), reverse=True)


def _resolve_latest_run(model: str) -> str | None:
    pointed = _latest_run_from_pointer(model)
    if pointed is not None:
        return pointed
    runs = _scan_manifest_runs(model)
    return runs[0] if runs else None


def _resolve_run(model: str, run: str) -> str | None:
    if run == "latest":
        return _resolve_latest_run(model)
    if not _RUN_ID_RE.match(run):
        return None
    run_dir = PUBLISHED_ROOT / model / run
    manifest_path = MANIFESTS_ROOT / model / f"{run}.json"
    if run_dir.is_dir() and manifest_path.is_file():
        return run
    return None


def _manifest_path(model: str, run: str) -> Path:
    return MANIFESTS_ROOT / model / f"{run}.json"


def _load_manifest(model: str, run: str) -> dict | None:
    path = _manifest_path(model, run)
    if not path.is_file():
        return None
    return _cached_manifest(str(path))


def _published_var_dir(model: str, run: str, var: str) -> Path:
    return PUBLISHED_ROOT / model / run / var


def _resolve_val_cog(model: str, run: str, var: str, fh: int) -> Path | None:
    resolved = _resolve_run(model, run) or run
    candidate = _published_var_dir(model, resolved, var) / f"fh{fh:03d}.val.cog.tif"
    if candidate.is_file():
        return candidate
    return None


def _resolve_sidecar(model: str, run: str, var: str, fh: int) -> dict | None:
    resolved = _resolve_run(model, run) or run
    candidate = _published_var_dir(model, resolved, var) / f"fh{fh:03d}.json"
    if candidate.is_file():
        return _cached_sidecar(str(candidate))
    return None


def _resolve_frame_var_dir(model: str, run: str, var: str, fh: int) -> Path | None:
    resolved = _resolve_run(model, run)
    if resolved is None:
        return None
    var_dir = _published_var_dir(model, resolved, var)
    if not var_dir.is_dir():
        return None
    if not (var_dir / f"fh{fh:03d}.rgba.cog.tif").is_file():
        return None
    return var_dir


@app.get("/api/v3/health")
def health():
    return {"ok": True, "data_root": str(DATA_ROOT)}


@app.get("/api/v3")
def root():
    return {"service": "twf-v3-api", "version": "2.0.0"}


@app.get("/api/regions")
def list_region_presets(request: Request):
    payload = {"regions": REGION_PRESETS}
    cache_control = "public, max-age=300"
    etag = _make_etag(payload)
    r304 = _maybe_304(request, etag=etag, cache_control=cache_control)
    if r304 is not None:
        return r304
    return JSONResponse(
        content=payload,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
        },
    )


@app.get("/api/v3/models")
def list_models():
    models: set[str] = set()
    if MANIFESTS_ROOT.is_dir():
        models.update(child.name for child in MANIFESTS_ROOT.iterdir() if child.is_dir())
    if PUBLISHED_ROOT.is_dir():
        models.update(child.name for child in PUBLISHED_ROOT.iterdir() if child.is_dir())
    model_ids = sorted(models)
    return [{"id": model_id, "name": MODEL_NAMES.get(model_id, model_id.upper())} for model_id in model_ids]


@app.get("/api/v3/{model}/runs")
def list_runs(request: Request, model: str):
    runs = _scan_manifest_runs(model)
    cache_control = "public, max-age=60"
    etag = _make_etag(runs)
    r304 = _maybe_304(request, etag=etag, cache_control=cache_control)
    if r304 is not None:
        return r304
    return JSONResponse(
        content=runs,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
        },
    )


@app.get("/api/v3/{model}/{run}/manifest")
def get_manifest(request: Request, model: str, run: str):
    resolved = _resolve_run(model, run)
    if resolved is None:
        return Response(status_code=404, content='{"error": "run not found"}', media_type="application/json")
    manifest = _load_manifest(model, resolved)
    if manifest is None:
        return Response(status_code=404, content='{"error": "manifest not found"}', media_type="application/json")

    cache_control = "public, max-age=60"
    etag = _make_etag(manifest)
    r304 = _maybe_304(request, etag=etag, cache_control=cache_control)
    if r304 is not None:
        return r304
    return JSONResponse(
        content=manifest,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
        },
    )


@app.get("/api/v3/{model}/{run}/vars")
def list_vars(model: str, run: str):
    resolved = _resolve_run(model, run)
    if resolved is None:
        return Response(status_code=404, content='{"error": "run not found"}', media_type="application/json")

    manifest = _load_manifest(model, resolved)
    if manifest is None:
        return Response(status_code=404, content='{"error": "manifest not found"}', media_type="application/json")

    variables = manifest.get("variables")
    if not isinstance(variables, dict):
        return []

    from .services.colormaps import VAR_SPECS

    priority = VAR_ORDER_BY_MODEL.get(model, [])
    priority_index = {var_id: idx for idx, var_id in enumerate(priority)}
    ordered_var_ids = sorted(
        variables.keys(),
        key=lambda var_id: (priority_index.get(var_id, len(priority_index)), var_id),
    )

    result = []
    for var_id in ordered_var_ids:
        spec = VAR_SPECS.get(var_id, {})
        result.append({"id": var_id, "display_name": spec.get("display_name", var_id)})
    return result


@app.get("/api/v3/{model}/{run}/{var}/frames")
def list_frames(request: Request, model: str, run: str, var: str):
    resolved = _resolve_run(model, run)
    if resolved is None:
        return Response(status_code=404, content='{"error": "run not found"}', media_type="application/json")

    manifest = _load_manifest(model, resolved)
    if manifest is None:
        return Response(status_code=404, content='{"error": "manifest not found"}', media_type="application/json")

    variables = manifest.get("variables")
    if not isinstance(variables, dict):
        return []
    var_entry = variables.get(var)
    if not isinstance(var_entry, dict):
        return []

    frame_entries = var_entry.get("frames")
    if not isinstance(frame_entries, list):
        frame_entries = []

    frames: list[dict] = []
    for item in frame_entries:
        if not isinstance(item, dict):
            continue
        fh = item.get("fh")
        if not isinstance(fh, int):
            continue

        meta = _resolve_sidecar(model, resolved, var, fh)
        frames.append(
            {
                "fh": fh,
                "has_cog": True,
                "run": resolved,
                "meta": {"meta": meta},
            }
        )

    frames.sort(key=lambda row: row["fh"])
    cache_control = "public, max-age=60"
    etag = _make_etag(frames)
    r304 = _maybe_304(request, etag=etag, cache_control=cache_control)
    if r304 is not None:
        return r304

    return JSONResponse(
        content=frames,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
        },
    )


@app.get("/api/v3/sample")
def sample(
    model: str = Query(..., description="Model ID (e.g. hrrr)"),
    run: str = Query(..., description="Run ID (e.g. 20260217_20z or latest)"),
    var: str = Query(..., description="Variable ID (e.g. tmp2m)"),
    fh: int = Query(..., description="Forecast hour"),
    lat: float = Query(..., ge=-90, le=90, description="Latitude (WGS84)"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude (WGS84)"),
):
    val_cog = _resolve_val_cog(model, run, var, fh)
    if val_cog is None:
        return Response(status_code=404, content='{"error": "COG not found"}', media_type="application/json")

    try:
        mx, my = _wgs84_to_3857.transform(lon, lat)
        ds = _get_cached_dataset(val_cog)
        row, col = ds.index(mx, my)

        if row < 0 or row >= ds.height or col < 0 or col >= ds.width:
            return Response(status_code=204, headers={"Cache-Control": "private, max-age=300"})

        window = Window(col, row, 1, 1)  # type: ignore[call-arg]
        pixel = ds.read(1, window=window)
        value = float(pixel[0, 0])

        if np.isnan(value):
            return Response(status_code=204, headers={"Cache-Control": "private, max-age=300"})

        sidecar = _resolve_sidecar(model, run, var, fh)
        payload = {
            "value": round(value, 1),
            "units": sidecar.get("units", "") if sidecar else "",
            "model": model,
            "var": var,
            "fh": fh,
            "valid_time": sidecar.get("valid_time", "") if sidecar else "",
            "lat": lat,
            "lon": lon,
        }
        return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=86400"})

    except Exception:
        logger.exception(
            "Sample query failed: %s/%s/%s/fh%03d @ (%.4f, %.4f)",
            model,
            run,
            var,
            fh,
            lat,
            lon,
        )
        return Response(status_code=500, content='{"error": "internal error"}', media_type="application/json")


@app.get("/api/v3/{model}/{run}/{var}/{fh:int}/contours/{key}")
def get_contour_geojson(
    model: str,
    run: str,
    var: str,
    fh: int,
    key: str,
):
    var_dir = _resolve_frame_var_dir(model, run, var, fh)
    if var_dir is None:
        raise HTTPException(status_code=404, detail="Frame not found")

    sidecar_path = var_dir / f"fh{fh:03d}.json"
    if not sidecar_path.is_file():
        raise HTTPException(status_code=404, detail="Sidecar not found")

    try:
        sidecar = json.loads(sidecar_path.read_text())
    except Exception as exc:
        logger.exception(
            "Failed to read sidecar for contour: %s/%s/%s/fh%03d (%s)",
            model,
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
            "Failed to read contour GeoJSON: %s/%s/%s/fh%03d/%s (%s)",
            model,
            run,
            var,
            fh,
            key,
            contour_path,
        )
        raise HTTPException(status_code=500, detail=f"Failed to read contour GeoJSON: {exc}") from exc


@app.get("/api/v3/admin/{model}/scan-runs")
def admin_scan_runs(model: str):
    runs = []
    d = PUBLISHED_ROOT / model
    if d.is_dir():
        runs = sorted(
            [child.name for child in d.iterdir() if child.is_dir() and _RUN_ID_RE.match(child.name)],
            reverse=True,
        )
    return {"model": model, "runs": runs}

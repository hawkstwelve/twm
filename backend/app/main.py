"""TWF V3 API — canonical discovery + sampling endpoints."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import tempfile
import threading
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from PIL import Image
from pyproj import Transformer
from rasterio.enums import Resampling
from rasterio.windows import Window

from .config.regions import REGION_PRESETS

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.environ.get("TWF_V3_DATA_ROOT", "./data/v3"))
PUBLISHED_ROOT = DATA_ROOT / "published"
MANIFESTS_ROOT = DATA_ROOT / "manifests"
LOOP_CACHE_ROOT = Path(os.environ.get("TWF_V3_LOOP_CACHE_ROOT", "/tmp/twf_v3_loop_webp_cache"))

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
_JSON_CACHE_RECHECK_SECONDS = float(os.environ.get("TWF_V3_JSON_CACHE_RECHECK_SECONDS", "1.0"))
LOOP_WEBP_QUALITY = int(os.environ.get("TWF_V3_LOOP_WEBP_QUALITY", "82"))
LOOP_WEBP_MAX_DIM = int(os.environ.get("TWF_V3_LOOP_WEBP_MAX_DIM", "1600"))
LOOP_WEBP_TIER1_QUALITY = int(os.environ.get("TWF_V3_LOOP_WEBP_TIER1_QUALITY", "86"))
LOOP_WEBP_TIER1_MAX_DIM = int(os.environ.get("TWF_V3_LOOP_WEBP_TIER1_MAX_DIM", "2400"))
SAMPLE_CACHE_TTL_SECONDS = float(os.environ.get("TWF_V3_SAMPLE_CACHE_TTL_SECONDS", "2.0"))
SAMPLE_INFLIGHT_WAIT_SECONDS = float(os.environ.get("TWF_V3_SAMPLE_INFLIGHT_WAIT_SECONDS", "0.2"))

LOOP_TIER_CONFIG: dict[int, dict[str, int]] = {
    0: {
        "max_dim": LOOP_WEBP_MAX_DIM,
        "quality": LOOP_WEBP_QUALITY,
    },
    1: {
        "max_dim": LOOP_WEBP_TIER1_MAX_DIM,
        "quality": LOOP_WEBP_TIER1_QUALITY,
    },
}

CACHE_HIT = "public, max-age=31536000, immutable"
CACHE_MISS = "public, max-age=15"


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

_manifest_cache: dict[str, dict[str, Any]] = {}
_sidecar_cache: dict[str, dict[str, Any]] = {}
_json_cache_lock = threading.Lock()


class _SampleInflight:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.payload: dict[str, Any] | None = None


_sample_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_sample_inflight: dict[str, _SampleInflight] = {}
_sample_lock = threading.Lock()


def _load_json_cached(path: Path, cache: dict[str, dict[str, Any]]) -> dict | None:
    key = str(path)
    now = time.monotonic()

    with _json_cache_lock:
        entry = cache.get(key)
        if entry is not None:
            last_checked = float(entry.get("last_checked", 0.0))
            if now - last_checked < _JSON_CACHE_RECHECK_SECONDS:
                payload = entry.get("payload")
                return payload if isinstance(payload, dict) else None

    try:
        stat = path.stat()
        mtime_ns = int(stat.st_mtime_ns)
    except OSError:
        with _json_cache_lock:
            cache.pop(key, None)
        return None

    with _json_cache_lock:
        entry = cache.get(key)
        if entry is not None and int(entry.get("mtime_ns", -1)) == mtime_ns:
            entry["last_checked"] = now
            payload = entry.get("payload")
            return payload if isinstance(payload, dict) else None

    try:
        payload = json.loads(path.read_text())
    except Exception:
        logger.warning("Failed to read JSON cache file %s; serving last-good payload if available", path)
        with _json_cache_lock:
            entry = cache.get(key)
            if entry is not None:
                entry["last_checked"] = now
                cached_payload = entry.get("payload")
                return cached_payload if isinstance(cached_payload, dict) else None
        return None

    if not isinstance(payload, dict):
        return None

    with _json_cache_lock:
        cache[key] = {
            "mtime_ns": mtime_ns,
            "last_checked": now,
            "payload": payload,
        }
    return payload


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
    return _load_json_cached(path, _manifest_cache)


def _run_version_token(model: str, run: str) -> str:
    path = _manifest_path(model, run)
    try:
        mtime_ns = int(path.stat().st_mtime_ns)
    except OSError:
        mtime_ns = 0
    return f"{run}-{mtime_ns}"


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
        return _load_json_cached(candidate, _sidecar_cache)
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


def _resolve_rgba_cog(model: str, run: str, var: str, fh: int) -> Path | None:
    resolved = _resolve_run(model, run)
    if resolved is None:
        return None
    candidate = _published_var_dir(model, resolved, var) / f"fh{fh:03d}.rgba.cog.tif"
    if candidate.is_file():
        return candidate
    return None


def _loop_webp_path(model: str, run: str, var: str, fh: int, *, tier: int) -> Path | None:
    resolved = _resolve_run(model, run)
    if resolved is None:
        return None
    return LOOP_CACHE_ROOT / model / resolved / var / f"tier{tier}" / f"fh{fh:03d}.loop.webp"


def _legacy_loop_webp_path(model: str, run: str, var: str, fh: int, *, tier: int) -> Path | None:
    if tier != 0:
        return None
    resolved = _resolve_run(model, run)
    if resolved is None:
        return None
    candidate = _published_var_dir(model, resolved, var) / f"fh{fh:03d}.loop.webp"
    if candidate.is_file():
        return candidate
    return None


def _ensure_loop_webp(cog_path: Path, out_path: Path, *, tier: int) -> bool:
    if out_path.is_file():
        return True

    tier_cfg = LOOP_TIER_CONFIG.get(tier)
    if tier_cfg is None:
        return False


def _sample_cache_key(model: str, run: str, var: str, fh: int, row: int, col: int) -> str:
    return f"{model}:{run}:{var}:{fh}:{row}:{col}"


def _sample_payload(
    *,
    model: str,
    run: str,
    var: str,
    fh: int,
    lat: float,
    lon: float,
    value: float | None,
    units: str,
    valid_time: str,
    no_data: bool,
) -> dict[str, Any]:
    return {
        "value": round(float(value), 1) if value is not None else None,
        "units": units,
        "model": model,
        "run": run,
        "var": var,
        "fh": fh,
        "valid_time": valid_time,
        "lat": lat,
        "lon": lon,
        "noData": no_data,
    }

    max_dim_cfg = max(1, int(tier_cfg.get("max_dim", LOOP_WEBP_MAX_DIM)))
    quality_cfg = max(1, min(100, int(tier_cfg.get("quality", LOOP_WEBP_QUALITY))))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix=".webp", delete=False, dir=str(out_path.parent)) as tmp:
        tmp_path = Path(tmp.name)

    try:
        with rasterio.open(cog_path) as ds:
            src_h = int(ds.height)
            src_w = int(ds.width)
            max_dim = max(src_h, src_w)
            if max_dim <= 0:
                return False

            scale = min(1.0, float(max_dim_cfg) / float(max_dim))
            out_h = max(1, int(round(src_h * scale)))
            out_w = max(1, int(round(src_w * scale)))

            data = ds.read(
                indexes=(1, 2, 3, 4),
                out_shape=(4, out_h, out_w),
                resampling=Resampling.bilinear,
            )

        rgba = np.moveaxis(data, 0, -1)
        image = Image.fromarray(rgba, mode="RGBA")
        image.save(tmp_path, format="WEBP", quality=quality_cfg, method=6)
        tmp_path.replace(out_path)
        return True
    except Exception:
        logger.exception("Failed generating loop WebP: %s -> %s", cog_path, out_path)
        try:
            if tmp_path.is_file():
                tmp_path.unlink()
        except Exception:
            pass
        return False


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

    version_token = _run_version_token(model, resolved)

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
                "loop_webp_url": f"/api/v3/{model}/{resolved}/{var}/{fh}/loop.webp?v={version_token}",
                "loop_webp_tier0_url": f"/api/v3/{model}/{resolved}/{var}/{fh}/loop.webp?tier=0&v={version_token}",
                "loop_webp_tier1_url": f"/api/v3/{model}/{resolved}/{var}/{fh}/loop.webp?tier=1&v={version_token}",
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


@app.get("/api/v3/{model}/{run}/{var}/{fh:int}/loop.webp")
def get_loop_webp(
    model: str,
    run: str,
    var: str,
    fh: int,
    tier: int = Query(0, ge=0, le=1, description="Loop tier (0=default, 1=high-res)"),
):
    resolved = _resolve_run(model, run)
    if resolved is None:
        return Response(status_code=404, headers={"Cache-Control": CACHE_MISS})

    cog_path = _resolve_rgba_cog(model, resolved, var, fh)
    if cog_path is None:
        return Response(status_code=404, headers={"Cache-Control": CACHE_MISS})

    legacy_path = _legacy_loop_webp_path(model, resolved, var, fh, tier=tier)
    if legacy_path is not None:
        cache_control = CACHE_HIT if run != "latest" else CACHE_MISS
        return FileResponse(
            path=str(legacy_path),
            media_type="image/webp",
            headers={"Cache-Control": cache_control},
        )

    out_path = _loop_webp_path(model, resolved, var, fh, tier=tier)
    if out_path is None:
        return Response(status_code=404, headers={"Cache-Control": CACHE_MISS})

    if not _ensure_loop_webp(cog_path, out_path, tier=tier):
        return Response(status_code=500, headers={"Cache-Control": CACHE_MISS})

    cache_control = CACHE_HIT if run != "latest" else CACHE_MISS
    return FileResponse(
        path=str(out_path),
        media_type="image/webp",
        headers={"Cache-Control": cache_control},
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
        return Response(status_code=404, content='{"error": "val.cog.tif not found"}', media_type="application/json")

    try:
        mx, my = _wgs84_to_3857.transform(lon, lat)
        ds = _get_cached_dataset(val_cog)
        row, col = ds.index(mx, my)
        resolved_run = _resolve_run(model, run) or run
        sidecar = _resolve_sidecar(model, run, var, fh)
        units = sidecar.get("units", "") if sidecar else ""
        valid_time = sidecar.get("valid_time", "") if sidecar else ""

        if row < 0 or row >= ds.height or col < 0 or col >= ds.width:
            payload = _sample_payload(
                model=model,
                run=resolved_run,
                var=var,
                fh=fh,
                lat=lat,
                lon=lon,
                value=None,
                units=units,
                valid_time=valid_time,
                no_data=True,
            )
            return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=300"})

        key = _sample_cache_key(model, resolved_run, var, fh, row, col)
        now = time.monotonic()
        inflight: _SampleInflight | None = None
        is_leader = False

        with _sample_lock:
            cached = _sample_cache.get(key)
            if cached is not None:
                expires_at, payload = cached
                if expires_at > now:
                    return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=300"})
                _sample_cache.pop(key, None)

            inflight = _sample_inflight.get(key)
            if inflight is None:
                inflight = _SampleInflight()
                _sample_inflight[key] = inflight
                is_leader = True

        if not is_leader:
            assert inflight is not None
            inflight.event.wait(timeout=SAMPLE_INFLIGHT_WAIT_SECONDS)
            with _sample_lock:
                cached = _sample_cache.get(key)
                if cached is not None:
                    expires_at, payload = cached
                    if expires_at > time.monotonic():
                        return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=300"})
                payload = inflight.payload
                if payload is not None:
                    return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=300"})

        window = Window(col, row, 1, 1)  # type: ignore[call-arg]
        pixel = ds.read(1, window=window)
        value = float(pixel[0, 0])

        payload = _sample_payload(
            model=model,
            run=resolved_run,
            var=var,
            fh=fh,
            lat=lat,
            lon=lon,
            value=None if np.isnan(value) else value,
            units=units,
            valid_time=valid_time,
            no_data=bool(np.isnan(value)),
        )

        with _sample_lock:
            _sample_cache[key] = (time.monotonic() + SAMPLE_CACHE_TTL_SECONDS, payload)
            sample_inflight = _sample_inflight.pop(key, None)
            if sample_inflight is not None:
                sample_inflight.payload = payload
                sample_inflight.event.set()

        return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=86400"})

    except Exception:
        with _sample_lock:
            key = locals().get("key")
            if isinstance(key, str):
                sample_inflight = _sample_inflight.pop(key, None)
                if sample_inflight is not None:
                    sample_inflight.event.set()
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

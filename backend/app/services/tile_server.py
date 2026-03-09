"""TWF V3 Tile Server — PNG tile responses from published artifacts.

Default path serves pre-styled 4-band RGBA COGs.
Coarse-model continuous fields may be value-rendered at request time:
value COG sample -> in-memory colorize -> PNG encode.
Render-time resampling remains kind-driven to keep tile extraction aligned
with loop WebP rendering.
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import re
import sqlite3
import threading
import traceback
from pathlib import Path

import numpy as np
from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from PIL import Image
from rio_tiler.io.rasterio import Reader
from rio_tiler.errors import TileOutsideBounds

from .builder.colorize import float_to_rgba
from .render_resampling import rio_tiler_resampling_kwargs, use_value_render_for_variable, variable_color_map_id

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.environ.get("TWF_V3_DATA_ROOT", "./data/v3"))
PUBLISHED_ROOT = DATA_ROOT / "published"
BOUNDARIES_MBTILES = Path(
    os.environ.get(
        "TWF_V3_BOUNDARIES_MBTILES",
        str(DATA_ROOT / "boundaries" / "v1" / "twf_boundaries.mbtiles"),
    )
)
BOUNDARIES_TILESET_ID = os.environ.get("TWF_V3_BOUNDARIES_TILESET_ID", "twf-boundaries-v1")
BOUNDARIES_TILESET_NAME = os.environ.get("TWF_V3_BOUNDARIES_TILESET_NAME", "TWF Boundaries v1")
TILES_PUBLIC_BASE_URL = os.environ.get("TWF_V3_TILES_PUBLIC_BASE_URL", "https://api.cartosky.com").rstrip("/")

# Regex to match run IDs like 20260217_20z
_RUN_ID_RE = re.compile(r"^\d{8}_\d{2}z$")

# Cache headers per the caching strategy in ROADMAP_V3
CACHE_HIT = "public, max-age=31536000, immutable"
CACHE_MISS = "public, max-age=15"
# Empty gzip-compressed MVT tile body; use 200 responses for expected-empty vector tiles.
EMPTY_GZIP_MVT_TILE = base64.b64decode("H4sIAHR2n2kC/wMAAAAAAAAAAAA=")
TILE_RENDER_COUNTER_LOG_EVERY = 200

_tile_render_counter_lock = threading.Lock()
_tile_render_totals: dict[str, int] = {"rgba": 0, "value": 0}
_tile_render_by_model_var: dict[str, dict[tuple[str, str], int]] = {"rgba": {}, "value": {}}


def _mbtiles_get_metadata(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    try:
        conn = sqlite3.connect(str(path))
        cur = conn.cursor()
        cur.execute("SELECT name, value FROM metadata")
        rows = cur.fetchall()
        conn.close()
        return {str(name): str(value) for name, value in rows}
    except Exception:
        logger.exception("Failed reading MBTiles metadata: %s", path)
        return {}


def _mbtiles_lookup_tile(path: Path, *, z: int, x: int, y: int) -> bytes | None:
    if not path.is_file():
        return None

    if z < 0 or x < 0 or y < 0:
        return None

    max_coord = (1 << z) - 1
    if x > max_coord or y > max_coord:
        return None

    tms_y = max_coord - y

    try:
        conn = sqlite3.connect(str(path))
        cur = conn.cursor()
        cur.execute(
            """
            SELECT tile_data
            FROM tiles
            WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?
            """,
            (z, x, tms_y),
        )
        row = cur.fetchone()
        conn.close()
        if row is None:
            return None
        return row[0]
    except Exception:
        logger.exception("Failed reading MBTiles tile z/x/y=%s/%s/%s from %s", z, x, y, path)
        return None


def _mbtiles_min_max_zoom(metadata: dict[str, str]) -> tuple[int, int]:
    try:
        minzoom = int(metadata.get("minzoom", "0"))
    except Exception:
        minzoom = 0
    try:
        maxzoom = int(metadata.get("maxzoom", "10"))
    except Exception:
        maxzoom = 10
    return minzoom, maxzoom


def _tilejson_for_boundaries() -> dict:
    metadata = _mbtiles_get_metadata(BOUNDARIES_MBTILES)
    minzoom, maxzoom = _mbtiles_min_max_zoom(metadata)
    bounds_raw = metadata.get("bounds", "-180,-85.0511,180,85.0511")
    center_raw = metadata.get("center", "-98.58,39.83,4")

    try:
        bounds = [float(v) for v in bounds_raw.split(",")[:4]]
        if len(bounds) != 4:
            raise ValueError("invalid bounds length")
    except Exception:
        bounds = [-180.0, -85.0511, 180.0, 85.0511]

    try:
        center_vals = [float(v) for v in center_raw.split(",")[:3]]
        if len(center_vals) != 3:
            raise ValueError("invalid center length")
    except Exception:
        center_vals = [-98.58, 39.83, 4.0]

    tilejson = {
        "tilejson": "2.2.0",
        "name": metadata.get("name", BOUNDARIES_TILESET_NAME),
        "id": metadata.get("id", BOUNDARIES_TILESET_ID),
        "scheme": "xyz",
        "format": "pbf",
        "minzoom": minzoom,
        "maxzoom": maxzoom,
        "bounds": bounds,
        "center": center_vals,
        "tiles": [f"{TILES_PUBLIC_BASE_URL}/tiles/v3/boundaries/v1/{{z}}/{{x}}/{{y}}.mvt"],
    }

    if "vector_layers" in metadata:
        try:
            tilejson["vector_layers"] = json.loads(metadata["vector_layers"])
        except Exception:
            logger.warning("Invalid vector_layers metadata in %s", BOUNDARIES_MBTILES)

    if "attribution" in metadata:
        tilejson["attribution"] = metadata["attribution"]

    if "description" in metadata:
        tilejson["description"] = metadata["description"]

    return tilejson

def _build_transparent_png_tile(tilesize: int = 512) -> bytes:
    safe_size = max(1, int(tilesize))
    try:
        image = Image.new("RGBA", (safe_size, safe_size), (0, 0, 0, 0))
        buf = io.BytesIO()
        image.save(buf, format="PNG", optimize=True)
        return buf.getvalue()
    except Exception:
        logger.exception("Failed to build transparent tile PNG; falling back to 1x1")
        return base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMB/ax7n7kAAAAASUVORK5CYII="
        )


TRANSPARENT_PNG_TILE = _build_transparent_png_tile(512)

app = FastAPI(title="TWF V3 Tile Server", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _read_tile_compat(
    cog: Reader,
    *,
    x: int,
    y: int,
    z: int,
    indexes: tuple[int, ...] = (1, 2, 3, 4),
    resampling_method: str,
    reproject_method: str,
):
    """Read a tile with compatibility across rio-tiler versions.

    Some deployments may run older rio-tiler builds that don't accept
    resampling kwargs on `Reader.tile`. Try explicit strategy first, then
    gracefully fall back to defaults to avoid service outages.
    """
    common_args = {
        "indexes": indexes,
        "tilesize": 512,
        "resampling_method": resampling_method,
        "reproject_method": reproject_method,
    }

    try:
        return cog.tile(
            x,
            y,
            z,
            **common_args,
        )
    except TileOutsideBounds:
        raise
    except Exception as exc:
        logger.warning(
            "Reader.tile() explicit resampling unsupported/failed (%s: %s); trying default args",
            exc.__class__.__name__,
            exc,
        )

    try:
        return cog.tile(x, y, z, **common_args)
    except TileOutsideBounds:
        raise
    except Exception as exc:
        logger.warning(
            "Reader.tile() with common args failed (%s: %s); trying minimal args",
            exc.__class__.__name__,
            exc,
        )
        return cog.tile(x, y, z)


def _render_png_compat(tile) -> bytes:
    """Render PNG with compatibility across rio-tiler versions."""
    try:
        return tile.render(img_format="PNG", add_mask=False)
    except TypeError:
        logger.warning("tile.render(add_mask=...) unsupported; falling back")
        return tile.render(img_format="PNG")


def _resolve_value_cog_path(model: str, run: str, var: str, fh: int) -> Path | None:
    resolved = run
    if run == "latest":
        resolved = _resolve_latest_run(model)
        if resolved is None:
            return None

    fh_str = f"fh{fh:03d}"
    filename = f"{fh_str}.val.cog.tif"
    candidate = PUBLISHED_ROOT / model / resolved / var / filename
    if candidate.is_file():
        return candidate
    return None


def _maybe_blur_value_tile(values: np.ndarray, *, sigma: float | None = None) -> np.ndarray:
    # Reserved optional hook for coarse-model value rendering.
    # Deliberately disabled for this change set (sigma=None everywhere).
    if sigma is None:
        return values
    try:
        if float(sigma) <= 0.0:
            return values
    except (TypeError, ValueError):
        return values
    return values


def _colorize_value_tile(
    tile,
    *,
    model: str,
    var: str,
    blur_sigma: float | None = None,
) -> np.ndarray | None:
    color_map_id = variable_color_map_id(model, var)
    if not color_map_id:
        logger.warning("Value-render color_map_id missing for model=%s var=%s", model, var)
        return None

    raw_data = np.asarray(getattr(tile, "data", None))
    if raw_data.size == 0:
        return None
    if raw_data.ndim == 3:
        values = raw_data[0]
    elif raw_data.ndim == 2:
        values = raw_data
    else:
        logger.warning(
            "Unexpected value tile ndim for model=%s var=%s: shape=%s",
            model,
            var,
            getattr(raw_data, "shape", None),
        )
        return None

    values_f32 = np.asarray(values, dtype=np.float32)
    mask = getattr(tile, "mask", None)
    if mask is not None:
        mask_arr = np.asarray(mask)
        if mask_arr.shape == values_f32.shape:
            values_f32 = values_f32.copy()
            values_f32[mask_arr == 0] = np.nan
    values_f32 = _maybe_blur_value_tile(values_f32, sigma=blur_sigma)

    rgba, _ = float_to_rgba(
        values_f32,
        color_map_id,
        meta_var_key=var,
    )
    return rgba


def _rgba_array_to_png_bytes(rgba: np.ndarray) -> bytes:
    rgba_hwc = np.moveaxis(rgba, 0, -1)
    image = Image.fromarray(rgba_hwc, mode="RGBA")
    buf = io.BytesIO()
    image.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def _record_tile_render_mode(mode: str, *, model: str, var: str) -> None:
    if mode not in {"rgba", "value"}:
        return
    model_key = str(model or "").strip().lower() or "<unknown-model>"
    var_key = str(var or "").strip().lower() or "<unknown-var>"
    with _tile_render_counter_lock:
        _tile_render_totals[mode] = _tile_render_totals.get(mode, 0) + 1
        key = (model_key, var_key)
        per_mode = _tile_render_by_model_var.setdefault(mode, {})
        per_mode[key] = per_mode.get(key, 0) + 1

        total = _tile_render_totals.get("rgba", 0) + _tile_render_totals.get("value", 0)
        if total <= 0 or total % TILE_RENDER_COUNTER_LOG_EVERY != 0:
            return

        value_total = _tile_render_totals.get("value", 0)
        rgba_total = _tile_render_totals.get("rgba", 0)
        value_top = sorted(
            _tile_render_by_model_var.get("value", {}).items(),
            key=lambda item: item[1],
            reverse=True,
        )[:4]
        rgba_top = sorted(
            _tile_render_by_model_var.get("rgba", {}).items(),
            key=lambda item: item[1],
            reverse=True,
        )[:4]
    logger.info(
        "Tile render counters total=%d value=%d rgba=%d value_top=%s rgba_top=%s",
        total,
        value_total,
        rgba_total,
        value_top,
        rgba_top,
    )


def _transparent_png_response(*, cache_control: str) -> Response:
    return Response(
        content=TRANSPARENT_PNG_TILE,
        media_type="image/png",
        headers={"Cache-Control": cache_control},
    )


def _empty_mvt_response(*, cache_control: str) -> Response:
    headers = {"Cache-Control": cache_control, "Content-Encoding": "gzip"}
    return Response(
        content=EMPTY_GZIP_MVT_TILE,
        media_type="application/vnd.mapbox-vector-tile",
        headers=headers,
    )


def _tile_is_fully_masked(tile) -> bool:
    """Return True when tile mask indicates no visible pixels (all zeros)."""
    mask = getattr(tile, "mask", None)
    if mask is None:
        return False
    try:
        size = getattr(mask, "size", None)
        if size is not None and int(size) == 0:
            return True
        return not bool(mask.any())
    except Exception:
        return False


def _latest_run_from_pointer(model: str) -> str | None:
    """Return run_id from published LATEST.json if valid and present on disk."""
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
    if not run_dir.is_dir():
        logger.warning("LATEST.json run_id does not exist on disk: %s", run_dir)
        return None
    return run_id


def _resolve_latest_run(model: str) -> str | None:
    """Find latest published run ID for model.

    Preference order:
    1) published/{model}/LATEST.json run_id (if valid)
    2) lexicographically greatest run directory in published/
    """
    pointed = _latest_run_from_pointer(model)
    if pointed is not None:
        return pointed

    d = PUBLISHED_ROOT / model
    if not d.is_dir():
        return None

    runs = [
        child.name
        for child in d.iterdir()
        if child.is_dir() and _RUN_ID_RE.match(child.name)
    ]
    if not runs:
        return None
    return sorted(set(runs))[-1]


def _resolve_cog_path(model: str, run: str, var: str, fh: int) -> Path | None:
    """Find the RGBA COG on disk.

    Resolves 'latest' to the actual latest run directory.
    Checks published/ only. Returns the path if it exists, else None.
    """
    resolved = run
    if run == "latest":
        resolved = _resolve_latest_run(model)
        if resolved is None:
            return None

    fh_str = f"fh{fh:03d}"
    filename = f"{fh_str}.rgba.cog.tif"

    candidate = PUBLISHED_ROOT / model / resolved / var / filename
    if candidate.is_file():
        return candidate
    return None


@app.get("/tiles/v3/health")
def health():
    return {
        "ok": True,
        "data_root": str(DATA_ROOT),
        "boundaries_mbtiles": str(BOUNDARIES_MBTILES),
        "boundaries_mbtiles_exists": BOUNDARIES_MBTILES.is_file(),
    }


@app.get("/tiles/v3/boundaries/v1/tilejson.json")
def boundaries_tilejson():
    if not BOUNDARIES_MBTILES.is_file():
        raise HTTPException(
            status_code=404,
            detail={
                "error": "boundaries tileset not found",
                "path": str(BOUNDARIES_MBTILES),
            },
        )

    return Response(
        content=json.dumps(_tilejson_for_boundaries()),
        media_type="application/json",
        headers={"Cache-Control": CACHE_MISS},
    )


@app.get("/tiles/v3/boundaries/v1/{z:int}/{x:int}/{y:int}.mvt")
def boundaries_tile(z: int, x: int, y: int):
    tile = _mbtiles_lookup_tile(BOUNDARIES_MBTILES, z=z, x=x, y=y)
    if tile is None:
        # Return an empty vector tile (200) so MapLibre treats expected-empty tiles as normal.
        return _empty_mvt_response(cache_control=CACHE_MISS)

    headers = {"Cache-Control": CACHE_HIT}
    if len(tile) >= 2 and tile[0] == 0x1F and tile[1] == 0x8B:
        headers["Content-Encoding"] = "gzip"

    return Response(
        content=tile,
        media_type="application/vnd.mapbox-vector-tile",
        headers=headers,
    )


@app.get("/tiles/v3/{model}/{run}/{var}/{fh:int}/{z:int}/{x:int}/{y:int}.png")
def get_tile(
    model: str, run: str, var: str, fh: int,
    z: int, x: int, y: int,
):
    """Serve a single PNG weather tile.

    Default path: RGBA COG -> PNG.
    Gated path: value COG -> colorize -> PNG.
    """
    use_value_render = use_value_render_for_variable(model_id=model, var_key=var)

    if use_value_render:
        val_path = _resolve_value_cog_path(model, run, var, fh)
        if val_path is not None:
            try:
                with Reader(input=str(val_path)) as val_cog:  # type: ignore[call-arg]
                    resampling_kwargs = rio_tiler_resampling_kwargs(model_id=model, var_key=var)
                    tile = _read_tile_compat(
                        val_cog,
                        x=x,
                        y=y,
                        z=z,
                        indexes=(1,),
                        resampling_method=resampling_kwargs["resampling_method"],
                        reproject_method=resampling_kwargs["reproject_method"],
                    )

                if _tile_is_fully_masked(tile):
                    _record_tile_render_mode("value", model=model, var=var)
                    return _transparent_png_response(cache_control=CACHE_HIT)

                rgba = _colorize_value_tile(tile, model=model, var=var, blur_sigma=None)
                if rgba is not None:
                    content = _rgba_array_to_png_bytes(rgba)
                    _record_tile_render_mode("value", model=model, var=var)
                    return Response(
                        content=content,
                        media_type="image/png",
                        headers={"Cache-Control": CACHE_HIT},
                    )
                logger.warning(
                    "Value-render colorization failed; falling back to RGBA path for %s/%s/%s/fh%03d/%d/%d/%d",
                    model,
                    run,
                    var,
                    fh,
                    z,
                    x,
                    y,
                )
            except TileOutsideBounds:
                # Tile coordinates outside extent are expected-empty tiles.
                return _transparent_png_response(cache_control=CACHE_MISS)
            except Exception:
                logger.exception(
                    "Value-render tile read/colorize failed; trying RGBA fallback: %s/%s/%s/fh%03d/%d/%d/%d",
                    model,
                    run,
                    var,
                    fh,
                    z,
                    x,
                    y,
                )
        else:
            logger.debug(
                "Value COG missing for value-render path; trying RGBA fallback: %s/%s/%s/fh%03d",
                model,
                run,
                var,
                fh,
            )

    cog_path = _resolve_cog_path(model, run, var, fh)
    if cog_path is None:
        return Response(
            status_code=404,
            headers={"Cache-Control": CACHE_MISS},
        )

    try:
        with Reader(input=str(cog_path)) as cog:  # type: ignore[call-arg]
            resampling_kwargs = rio_tiler_resampling_kwargs(model_id=model, var_key=var)
            tile = _read_tile_compat(
                cog,
                x=x,
                y=y,
                z=z,
                resampling_method=resampling_kwargs["resampling_method"],
                reproject_method=resampling_kwargs["reproject_method"],
            )

        if _tile_is_fully_masked(tile):
            _record_tile_render_mode("rgba", model=model, var=var)
            return _transparent_png_response(cache_control=CACHE_HIT)

        content = _render_png_compat(tile)
        _record_tile_render_mode("rgba", model=model, var=var)
        return Response(
            content=content,
            media_type="image/png",
            headers={"Cache-Control": CACHE_HIT},
        )

    except TileOutsideBounds:
        # Tile coordinates outside extent are expected-empty tiles.
        return _transparent_png_response(cache_control=CACHE_MISS)
    except Exception as exc:
        logger.exception(
            "Tile read failed: %s/%s/%s/fh%03d/%d/%d/%d",
            model, run, var, fh, z, x, y,
        )
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(exc),
                "trace": traceback.format_exc().splitlines()[-40:],
            },
        )

"""TWF V3 Tile Server — dumb RGBA COG → PNG tile server.

Hard Rule: No runtime colormap transformation.
The tile server reads pre-styled 4-band RGBA COGs and returns PNG tiles.
Render-time resampling is kind-driven (continuous vs categorical) to keep
tile extraction consistent with loop WebP rendering.
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import re
import sqlite3
import traceback
from pathlib import Path

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from PIL import Image
from rio_tiler.io.rasterio import Reader
from rio_tiler.errors import TileOutsideBounds

from app.services.render_resampling import rio_tiler_resampling_kwargs

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
TILES_PUBLIC_BASE_URL = os.environ.get("TWF_V3_TILES_PUBLIC_BASE_URL", "https://api.theweathermodels.com").rstrip("/")

# Regex to match run IDs like 20260217_20z
_RUN_ID_RE = re.compile(r"^\d{8}_\d{2}z$")

# Cache headers per the caching strategy in ROADMAP_V3
CACHE_HIT = "public, max-age=31560000, immutable"
CACHE_MISS = "public, max-age=15"
# Empty gzip-compressed MVT tile body; use 200 responses for expected-empty vector tiles.
EMPTY_GZIP_MVT_TILE = base64.b64decode("H4sIAHR2n2kC/wMAAAAAAAAAAAA=")


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
    resampling_method: str,
    reproject_method: str,
):
    """Read a tile with compatibility across rio-tiler versions.

    Some deployments may run older rio-tiler builds that don't accept
    resampling kwargs on `Reader.tile`. Try explicit strategy first, then
    gracefully fall back to defaults to avoid service outages.
    """
    common_args = {
        "indexes": (1, 2, 3, 4),
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
    """Serve a single PNG map tile from a pre-styled RGBA COG.

    No colormap logic. No var-branching. Read 4 bands, encode PNG, return.
    """
    cog_path = _resolve_cog_path(model, run, var, fh)
    if cog_path is None:
        return Response(
            status_code=404,
            headers={"Cache-Control": CACHE_MISS},
        )

    try:
        with Reader(input=str(cog_path)) as cog:
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
            return _transparent_png_response(cache_control=CACHE_HIT)

        content = _render_png_compat(tile)
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
            "Tile read failed: %s/%s/%s/%s/fh%03d/%d/%d/%d",
            model, run, var, fh, z, x, y,
        )
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(exc),
                "trace": traceback.format_exc().splitlines()[-40:],
            },
        )

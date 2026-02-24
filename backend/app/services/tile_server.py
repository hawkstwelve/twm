"""TWF V3 Tile Server — dumb RGBA COG → PNG tile server.

Hard Rule: No Runtime Transformation.
The tile server MUST NOT apply any variable-dependent transformation.
It reads 4-band RGBA COGs and returns PNG tiles. That's it.
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import re
import traceback
from pathlib import Path

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from PIL import Image
from rio_tiler.io.rasterio import Reader
from rio_tiler.errors import TileOutsideBounds

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.environ.get("TWF_V3_DATA_ROOT", "./data/v3"))
PUBLISHED_ROOT = DATA_ROOT / "published"

# Regex to match run IDs like 20260217_20z
_RUN_ID_RE = re.compile(r"^\d{8}_\d{2}z$")

# Cache headers per the caching strategy in ROADMAP_V3
CACHE_HIT = "public, max-age=31536000, immutable"
CACHE_MISS = "public, max-age=15"

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
):
    """Read a tile with compatibility across rio-tiler versions.

    Some deployments may run older rio-tiler builds that don't accept
    resampling kwargs on `Reader.tile`. Try explicit strategy first, then
    gracefully fall back to defaults to avoid service outages.
    """
    common_args = {
        "indexes": (1, 2, 3, 4),
        "tilesize": 512,
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
    return {"ok": True, "data_root": str(DATA_ROOT)}


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
        with Reader(str(cog_path)) as cog:
            tile = _read_tile_compat(
                cog,
                x=x,
                y=y,
                z=z,
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

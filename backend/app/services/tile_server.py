"""TWF V3 Tile Server — dumb RGBA COG → PNG tile server.

Hard Rule: No Runtime Transformation.
The tile server MUST NOT apply any variable-dependent transformation.
It reads 4-band RGBA COGs and returns PNG tiles. That's it.
"""

from __future__ import annotations

import logging
import os
import re
import traceback
from pathlib import Path

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from rio_tiler.io.rasterio import Reader
from rio_tiler.errors import TileOutsideBounds

from app.services.colormaps import VAR_SPECS

logger = logging.getLogger(__name__)

DATA_ROOT = Path(os.environ.get("TWF_V3_DATA_ROOT", "./data/v3"))

# Regex to match run IDs like 20260217_20z
_RUN_ID_RE = re.compile(r"^\d{8}_\d{2}z$")

# Cache headers per the caching strategy in ROADMAP_V3
CACHE_HIT = "public, max-age=31536000, immutable"
CACHE_MISS = "public, max-age=15"

app = FastAPI(title="TWF V3 Tile Server", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _tile_resampling_for_var(var: str) -> tuple[str, str]:
    """Return (resampling_method, reproject_method) for tile reads.

    Explicitly set behavior by variable kind instead of relying on rio-tiler
    defaults. This keeps categorical products crisp while allowing smooth
    continuous interpolation.
    """
    spec = VAR_SPECS.get(var, {})
    kind = str(spec.get("type", "")).strip().lower()
    if kind in {"discrete", "indexed", "categorical"}:
        return "nearest", "nearest"
    return "bilinear", "bilinear"


def _resolve_latest_run(model: str, region: str) -> str | None:
    """Find the latest (lexicographically greatest) run ID for a model/region."""
    runs: list[str] = []
    for prefix in ("published", "staging"):
        d = DATA_ROOT / prefix / model / region
        if d.is_dir():
            for child in d.iterdir():
                if child.is_dir() and _RUN_ID_RE.match(child.name):
                    runs.append(child.name)
    if not runs:
        return None
    return sorted(set(runs))[-1]


def _resolve_cog_path(model: str, region: str, run: str, var: str, fh: int) -> Path | None:
    """Find the RGBA COG on disk.

    Resolves 'latest' to the actual latest run directory.
    Checks published/ first, then staging/.  Returns the first path that
    exists, or None if the COG cannot be found.
    """
    resolved = run
    if run == "latest":
        resolved = _resolve_latest_run(model, region)
        if resolved is None:
            return None

    fh_str = f"fh{fh:03d}"
    filename = f"{fh_str}.rgba.cog.tif"

    for prefix in ("published", "staging"):
        candidate = DATA_ROOT / prefix / model / region / resolved / var / filename
        if candidate.is_file():
            return candidate
    return None


@app.get("/tiles/v3/health")
def health():
    return {"ok": True, "data_root": str(DATA_ROOT)}


@app.get("/tiles/v3/{model}/{region}/{run}/{var}/{fh:int}/{z:int}/{x:int}/{y:int}.png")
def get_tile(
    model: str, region: str, run: str, var: str, fh: int,
    z: int, x: int, y: int,
):
    """Serve a single PNG map tile from a pre-styled RGBA COG.

    No colormap logic. No var-branching. Read 4 bands, encode PNG, return.
    """
    cog_path = _resolve_cog_path(model, region, run, var, fh)
    if cog_path is None:
        return Response(
            status_code=404,
            headers={"Cache-Control": CACHE_MISS},
        )

    try:
        resampling_method, reproject_method = _tile_resampling_for_var(var)
        with Reader(str(cog_path)) as cog:
            tile = cog.tile(
                x,
                y,
                z,
                indexes=(1, 2, 3, 4),
                tilesize=512,
                resampling_method=resampling_method,
                reproject_method=reproject_method,
            )

        content = tile.render(img_format="PNG", add_mask=False)
        return Response(
            content=content,
            media_type="image/png",
            headers={"Cache-Control": CACHE_HIT},
        )

    except TileOutsideBounds:
        # Tile coordinates outside the COG extent — return empty 204
        return Response(
            status_code=204,
            headers={"Cache-Control": CACHE_MISS},
        )
    except Exception as exc:
        logger.exception(
            "Tile read failed: %s/%s/%s/%s/fh%03d/%d/%d/%d",
            model, region, run, var, fh, z, x, y,
        )
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(exc),
                "trace": traceback.format_exc().splitlines()[-40:],
            },
        )

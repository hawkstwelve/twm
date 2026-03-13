from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import rasterio
import pytest
from rasterio.transform import from_origin
from rasterio.warp import transform_bounds

BACKEND_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("TWF_BASE", "https://example.com")
os.environ.setdefault("TWF_CLIENT_ID", "test-client")
os.environ.setdefault("TWF_CLIENT_SECRET", "test-secret")
os.environ.setdefault("TWF_REDIRECT_URI", "https://example.com/callback")
os.environ.setdefault("TWF_SCOPES", "profile forums_posts")
os.environ.setdefault("FRONTEND_RETURN", "https://example.com/models-v3")
os.environ.setdefault("TOKEN_DB_PATH", "/tmp/twf_oauth_test.sqlite3")
os.environ.setdefault("TOKEN_ENC_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")

from app import main as main_module


def test_loop_manifest_bbox_uses_actual_cog_bounds(tmp_path, monkeypatch) -> None:
    cog_path = tmp_path / "fh000.rgba.cog.tif"
    transform = from_origin(-14920000.0, 7362000.0, 3000.0, 3000.0)
    data = np.full((4, 2, 2), 255, dtype=np.uint8)

    with rasterio.open(
        cog_path,
        "w",
        driver="GTiff",
        height=2,
        width=2,
        count=4,
        dtype="uint8",
        crs="EPSG:3857",
        transform=transform,
    ) as ds:
        ds.write(data)

    with rasterio.open(cog_path) as ds:
        expected = transform_bounds(ds.crs, "EPSG:4326", *ds.bounds, densify_pts=21)

    main_module._loop_manifest_bbox_for_path.cache_clear()
    monkeypatch.setattr(main_module, "_resolve_rgba_cog", lambda model, run, var, fh: cog_path if fh == 0 else None)

    bbox = main_module._resolve_loop_manifest_bbox(
        "hrrr",
        "20260224_14z",
        "tmp2m",
        [{"fh": 0}],
    )

    assert bbox == [pytest.approx(value) for value in expected]


def test_loop_manifest_bbox_falls_back_when_no_cog(monkeypatch) -> None:
    main_module._loop_manifest_bbox_for_path.cache_clear()
    monkeypatch.setattr(main_module, "_resolve_rgba_cog", lambda model, run, var, fh: None)

    bbox = main_module._resolve_loop_manifest_bbox(
        "hrrr",
        "20260224_14z",
        "tmp2m",
        [{"fh": 0}],
    )

    assert bbox == [float(value) for value in main_module.LOOP_MANIFEST_BBOX]
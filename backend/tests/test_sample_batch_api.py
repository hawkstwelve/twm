import json
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

BACKEND_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = BACKEND_ROOT.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("TWF_BASE", "https://example.com")
os.environ.setdefault("TWF_CLIENT_ID", "client-id")
os.environ.setdefault("TWF_CLIENT_SECRET", "client-secret")
os.environ.setdefault("TWF_REDIRECT_URI", "https://example.com/callback")
os.environ.setdefault("FRONTEND_RETURN", "https://example.com/app")
os.environ.setdefault("TOKEN_DB_PATH", "/tmp/twf_test_tokens.sqlite3")
os.environ.setdefault("TOKEN_ENC_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")

from app import main as main_module

pytestmark = pytest.mark.anyio


def _reset_main_caches() -> None:
    with main_module._ds_cache_lock:
        for ds in main_module._ds_cache.values():
            try:
                ds.close()
            except Exception:
                pass
        main_module._ds_cache.clear()

    with main_module._sample_lock:
        main_module._sample_cache.clear()
        main_module._sample_inflight.clear()
        main_module._sample_rate_window.clear()

    main_module._manifest_cache.clear()
    main_module._sidecar_cache.clear()
    main_module._sample_transformer.cache_clear()


def _write_value_raster(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = np.array(
        [
            [1.34, 2.21, 3.09],
            [4.04, -9999.0, np.nan],
            [7.77, 8.88, 9.99],
        ],
        dtype=np.float32,
    )
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(-101.0, 46.0, 1.0, 1.0),
        nodata=-9999.0,
    ) as ds:
        ds.write(data, 1)


@pytest.fixture
async def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
    data_root = tmp_path / "data" / "v3"
    manifests_root = data_root / "manifests"
    published_root = data_root / "published"

    model = "hrrr"
    run_id = "20260306_00z"
    variable = "tmp2m"
    fh = 1

    manifest_dir = manifests_root / model
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / f"{run_id}.json").write_text(
        json.dumps(
            {
                "variables": {
                    variable: {
                        "expected_frames": 1,
                        "available_frames": 1,
                        "frames": [{"fh": fh}],
                    }
                }
            }
        )
    )

    model_root = published_root / model
    model_root.mkdir(parents=True, exist_ok=True)
    (model_root / "LATEST.json").write_text(json.dumps({"run_id": run_id}))

    var_dir = model_root / run_id / variable
    _write_value_raster(var_dir / f"fh{fh:03d}.val.cog.tif")
    (var_dir / f"fh{fh:03d}.json").write_text(
        json.dumps({"units": "K", "valid_time": "2026-03-06T01:00:00Z"})
    )

    monkeypatch.setattr(main_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(main_module, "MANIFESTS_ROOT", manifests_root)
    monkeypatch.setattr(main_module, "PUBLISHED_ROOT", published_root)

    _reset_main_caches()

    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as test_client:
        yield test_client

    _reset_main_caches()


async def test_sample_batch_returns_values_for_valid_points(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v4/sample/batch",
        json={
            "model": "hrrr",
            "run": "latest",
            "variable": "tmp2m",
            "forecast_hour": 1,
            "points": [
                {"id": "SD_1", "lat": 45.5, "lon": -100.5},
                {"id": "SD_2", "lat": 44.5, "lon": -100.5},
            ],
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "units": "K",
        "values": {
            "SD_1": 1.3,
            "SD_2": 4.0,
        },
    }


async def test_sample_batch_returns_null_for_out_of_bounds_and_nodata(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v4/sample/batch",
        json={
            "model": "hrrr",
            "run": "latest",
            "variable": "tmp2m",
            "forecast_hour": 1,
            "points": [
                {"id": "OOB", "lat": 60.0, "lon": -120.0},
                {"id": "NODATA", "lat": 44.5, "lon": -99.5},
                {"id": "NAN", "lat": 44.5, "lon": -98.5},
            ],
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "units": "K",
        "values": {
            "OOB": None,
            "NODATA": None,
            "NAN": None,
        },
    }


async def test_sample_batch_invalid_payload_returns_422_detail(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v4/sample/batch",
        json={
            "model": "hrrr",
            "run": "latest",
            "variable": "tmp2m",
            "forecast_hour": 1,
            "points": [
                {"id": "BAD", "lat": 999.0, "lon": -100.5},
            ],
        },
    )

    assert response.status_code == 422
    payload = response.json()
    assert "detail" in payload
    assert isinstance(payload["detail"], list)


async def test_sample_batch_reuses_cached_payload(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    call_count = 0
    original = main_module._sample_batch_values

    def wrapped(ds: rasterio.DatasetReader, *, points: list[main_module.SampleBatchPointIn]) -> dict[str, float | None]:
        nonlocal call_count
        call_count += 1
        return original(ds, points=points)

    monkeypatch.setattr(main_module, "_sample_batch_values", wrapped)

    payload = {
        "model": "hrrr",
        "run": "latest",
        "variable": "tmp2m",
        "forecast_hour": 1,
        "points": [
            {"id": "SD_1", "lat": 45.5, "lon": -100.5},
            {"id": "SD_2", "lat": 44.5, "lon": -100.5},
        ],
    }

    first = await client.post("/api/v4/sample/batch", json=payload)
    second = await client.post("/api/v4/sample/batch", json=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert call_count == 1
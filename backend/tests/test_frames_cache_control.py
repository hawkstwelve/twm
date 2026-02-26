import json
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app import main as main_module

pytestmark = pytest.mark.anyio


@pytest.fixture
async def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
    data_root = tmp_path / "data" / "v3"
    manifests_root = data_root / "manifests"
    published_root = data_root / "published"
    loop_cache_root = tmp_path / "loop-cache"

    run_id = "20260224_14z"
    incomplete_run_id = "20260224_15z"
    model = "hrrr"
    var = "radar_ptype"

    model_manifest_dir = manifests_root / model
    model_manifest_dir.mkdir(parents=True, exist_ok=True)
    (model_manifest_dir / f"{run_id}.json").write_text(
        json.dumps(
            {
                "variables": {
                    var: {
                        "expected_frames": 2,
                        "available_frames": 2,
                        "frames": [
                            {"fh": 0},
                            {"fh": 1},
                        ]
                    }
                }
            }
        )
    )
    (model_manifest_dir / f"{incomplete_run_id}.json").write_text(
        json.dumps(
            {
                "variables": {
                    var: {
                        "expected_frames": 2,
                        "available_frames": 1,
                        "frames": [
                            {"fh": 0},
                        ],
                    }
                }
            }
        )
    )

    model_published_dir = published_root / model
    (model_published_dir / run_id).mkdir(parents=True, exist_ok=True)
    (model_published_dir / incomplete_run_id).mkdir(parents=True, exist_ok=True)
    (model_published_dir / "LATEST.json").write_text(json.dumps({"run_id": run_id}))

    # Seed loop cache artifacts so frame payloads include loop URLs.
    for fh in (0, 1):
        tier0_path = loop_cache_root / model / run_id / var / "tier0" / f"fh{fh:03d}.loop.webp"
        tier0_path.parent.mkdir(parents=True, exist_ok=True)
        tier0_path.write_bytes(b"RIFFxxxxWEBPVP8 ")
    tier1_path = loop_cache_root / model / run_id / var / "tier1" / "fh000.loop.webp"
    tier1_path.parent.mkdir(parents=True, exist_ok=True)
    tier1_path.write_bytes(b"RIFFxxxxWEBPVP8 ")

    monkeypatch.setattr(main_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(main_module, "MANIFESTS_ROOT", manifests_root)
    monkeypatch.setattr(main_module, "PUBLISHED_ROOT", published_root)
    monkeypatch.setattr(main_module, "LOOP_CACHE_ROOT", loop_cache_root)

    main_module._manifest_cache.clear()
    main_module._sidecar_cache.clear()

    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as test_client:
        yield test_client


async def test_frames_latest_cache_control_is_short(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v4/hrrr/latest/radar_ptype/frames")

    assert response.status_code == 200
    cache_control = response.headers.get("cache-control", "")
    assert "max-age=60" in cache_control
    assert "immutable" not in cache_control
    assert response.headers.get("etag")


async def test_frames_historical_cache_control_is_immutable(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v4/hrrr/20260224_14z/radar_ptype/frames")

    assert response.status_code == 200
    cache_control = response.headers.get("cache-control", "")
    assert "max-age=31536000" in cache_control
    assert "immutable" in cache_control
    assert response.headers.get("etag")


async def test_frames_incomplete_historical_cache_control_is_short(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v4/hrrr/20260224_15z/radar_ptype/frames")

    assert response.status_code == 200
    cache_control = response.headers.get("cache-control", "")
    assert "max-age=60" in cache_control
    assert "immutable" not in cache_control
    assert response.headers.get("etag")


async def test_frame_loop_urls_emit_v4_runtime_paths(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v4/hrrr/latest/radar_ptype/frames")

    assert response.status_code == 200
    rows = response.json()
    assert isinstance(rows, list) and rows
    first = rows[0]
    assert first["loop_webp_url"].startswith("/api/v4/hrrr/")
    assert "/loop.webp?tier=0" in first["loop_webp_url"]
    assert first["loop_webp_tier0_url"].startswith("/api/v4/hrrr/")

    tier1_row = next((row for row in rows if row.get("loop_webp_tier1_url")), None)
    assert tier1_row is not None
    assert tier1_row["loop_webp_tier1_url"].startswith("/api/v4/hrrr/")
    assert "/loop.webp?tier=1" in tier1_row["loop_webp_tier1_url"]


async def test_v3_runtime_routes_are_retired(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v3/hrrr/latest/radar_ptype/frames")
    assert response.status_code == 404


async def test_v4_health_endpoint(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v4/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True


async def test_capabilities_invariant_supported_models_matches_catalog(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v4/capabilities")

    assert response.status_code == 200
    payload = response.json()
    supported_models = payload["supported_models"]
    model_catalog = payload["model_catalog"]
    availability = payload["availability"]

    assert sorted(supported_models) == sorted(model_catalog.keys())
    assert sorted(supported_models) == sorted(availability.keys())
    assert payload["contract_version"] == "v1"

    for model_id, model_payload in model_catalog.items():
        variables = model_payload.get("variables", {})
        assert isinstance(variables, dict)
        for var_key, var_payload in variables.items():
            assert var_payload["var_key"] == var_key
            assert "buildable" in var_payload

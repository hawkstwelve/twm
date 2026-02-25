import json
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app import main as main_module


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
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

    monkeypatch.setattr(main_module, "DATA_ROOT", data_root)
    monkeypatch.setattr(main_module, "MANIFESTS_ROOT", manifests_root)
    monkeypatch.setattr(main_module, "PUBLISHED_ROOT", published_root)
    monkeypatch.setattr(main_module, "LOOP_CACHE_ROOT", loop_cache_root)

    main_module._manifest_cache.clear()
    main_module._sidecar_cache.clear()

    with TestClient(main_module.app) as test_client:
        yield test_client


def test_frames_latest_cache_control_is_short(client: TestClient) -> None:
    response = client.get("/api/v3/hrrr/latest/radar_ptype/frames")

    assert response.status_code == 200
    cache_control = response.headers.get("cache-control", "")
    assert "max-age=60" in cache_control
    assert "immutable" not in cache_control
    assert response.headers.get("etag")


def test_frames_historical_cache_control_is_immutable(client: TestClient) -> None:
    response = client.get("/api/v3/hrrr/20260224_14z/radar_ptype/frames")

    assert response.status_code == 200
    cache_control = response.headers.get("cache-control", "")
    assert "max-age=31536000" in cache_control
    assert "immutable" in cache_control
    assert response.headers.get("etag")


def test_frames_incomplete_historical_cache_control_is_short(client: TestClient) -> None:
    response = client.get("/api/v3/hrrr/20260224_15z/radar_ptype/frames")

    assert response.status_code == 200
    cache_control = response.headers.get("cache-control", "")
    assert "max-age=60" in cache_control
    assert "immutable" not in cache_control
    assert response.headers.get("etag")

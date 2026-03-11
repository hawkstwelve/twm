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
os.environ.setdefault("TWM_ADMIN_MEMBER_IDS", "42")

from app import main as main_module

twf_oauth = main_module.twf_oauth
admin_telemetry = main_module.admin_telemetry

pytestmark = pytest.mark.anyio


def _create_session(*, session_id: str, member_id: int, name: str) -> None:
    twf_oauth.upsert_session(
        twf_oauth.TwfSession(
            session_id=session_id,
            member_id=member_id,
            display_name=name,
            photo_url=None,
            access_token="access-token",
            refresh_token="refresh-token",
            expires_at=2_000_000_000,
        )
    )


def _write_value_grid(path: Path, data: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=data.shape[1],
        height=data.shape[0],
        count=1,
        dtype="float32",
        transform=from_origin(0, float(data.shape[0]), 1.0, 1.0),
        crs="EPSG:3857",
    ) as dataset:
        dataset.write(data.astype("float32"), 1)


def _write_sidecar(path: Path, *, model_id: str, variable_id: str, run_id: str, forecast_hour: int, min_value: float, max_value: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "contract_version": "3.0",
                "model": model_id,
                "run": run_id,
                "var": variable_id,
                "fh": forecast_hour,
                "units": "in",
                "kind": "continuous",
                "min": min_value,
                "max": max_value,
            }
        )
    )


def _write_manifest(path: Path, *, model_id: str, run_id: str, variables: dict[str, list[int]]) -> None:
    payload = {
        "contract_version": "3.0",
        "model": model_id,
        "run": run_id,
        "variables": {
            variable_id: {
                "display_name": variable_id,
                "kind": "continuous",
                "units": "in",
                "expected_frames": len(hours),
                "available_frames": len(hours),
                "frames": [{"fh": forecast_hour} for forecast_hour in hours],
            }
            for variable_id, hours in variables.items()
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


@pytest.fixture(autouse=True)
def isolate_environment(tmp_path: Path) -> None:
    token_db = tmp_path / "tokens.sqlite3"
    telemetry_db = tmp_path / "telemetry.sqlite3"
    data_root = tmp_path / "data"

    twf_oauth.TOKEN_DB_PATH = str(token_db)
    admin_telemetry.TELEMETRY_DB_PATH = telemetry_db
    admin_telemetry._db_initialized = False

    main_module.DATA_ROOT = data_root
    main_module.PUBLISHED_ROOT = data_root / "published"
    main_module.MANIFESTS_ROOT = data_root / "manifests"


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as test_client:
        yield test_client


def _seed_verification_files(root: Path) -> None:
    model_id = "hrrr"
    run_id = "20260310_12z"
    _write_manifest(
        root / "manifests" / model_id / f"{run_id}.json",
        model_id=model_id,
        run_id=run_id,
        variables={
            "tmp2m": [1],
            "precip_total": [1, 2],
        },
    )

    _write_value_grid(
        root / "published" / model_id / run_id / "tmp2m" / "fh001.val.cog.tif",
        np.array([[48.0, 49.5], [50.0, 51.0]], dtype=np.float32),
    )
    _write_sidecar(
        root / "published" / model_id / run_id / "tmp2m" / "fh001.json",
        model_id=model_id,
        variable_id="tmp2m",
        run_id=run_id,
        forecast_hour=1,
        min_value=48.0,
        max_value=51.0,
    )

    _write_value_grid(
        root / "published" / model_id / run_id / "precip_total" / "fh001.val.cog.tif",
        np.array([[0.2, 0.5], [0.7, 1.0]], dtype=np.float32),
    )
    _write_sidecar(
        root / "published" / model_id / run_id / "precip_total" / "fh001.json",
        model_id=model_id,
        variable_id="precip_total",
        run_id=run_id,
        forecast_hour=1,
        min_value=0.2,
        max_value=1.0,
    )

    _write_value_grid(
        root / "published" / model_id / run_id / "precip_total" / "fh002.val.cog.tif",
        np.array([[0.1, 0.4], [0.6, 0.9]], dtype=np.float32),
    )
    _write_sidecar(
        root / "published" / model_id / run_id / "precip_total" / "fh002.json",
        model_id=model_id,
        variable_id="precip_total",
        run_id=run_id,
        forecast_hour=2,
        min_value=0.1,
        max_value=0.9,
    )


async def test_verification_summary_and_results(client: httpx.AsyncClient) -> None:
    _create_session(session_id="admin-session", member_id=42, name="Admin")
    _seed_verification_files(main_module.DATA_ROOT)

    summary = await client.get(
        "/api/v4/admin/verification/summary?window=30d",
        cookies={twf_oauth.SESSION_COOKIE_NAME: "admin-session"},
    )

    assert summary.status_code == 200
    assert summary.json()["total_rows"] == 3
    assert summary.json()["auto_pass_rows"] == 2
    assert summary.json()["manual_review_rows"] == 3
    assert summary.json()["flagged_rows"] == 1

    results = await client.get(
        "/api/v4/admin/verification/results?window=30d",
        cookies={twf_oauth.SESSION_COOKIE_NAME: "admin-session"},
    )

    assert results.status_code == 200
    body = results.json()
    assert len(body["results"]) == 3
    warning_row = next(item for item in body["results"] if item["variable_id"] == "precip_total" and item["forecast_hour"] == 2)
    assert warning_row["auto_status"] == "warning"
    assert warning_row["auto_checks"]["monotonic"] is False


async def test_verification_review_update(client: httpx.AsyncClient) -> None:
    _create_session(session_id="admin-session", member_id=42, name="Admin")
    _seed_verification_files(main_module.DATA_ROOT)

    results = await client.get(
        "/api/v4/admin/verification/results?window=30d",
        cookies={twf_oauth.SESSION_COOKIE_NAME: "admin-session"},
    )
    review_id = results.json()["results"][0]["id"]

    update = await client.post(
        f"/api/v4/admin/verification/results/{review_id}/review",
        cookies={twf_oauth.SESSION_COOKIE_NAME: "admin-session"},
        json={
            "manual_status": "pass",
            "benchmark_site": "Tropical Tidbits",
            "notes": "Looks close enough.",
        },
    )

    assert update.status_code == 200
    payload = update.json()
    assert payload["manual_status"] == "pass"
    assert payload["benchmark_site"] == "Tropical Tidbits"
    assert payload["notes"] == "Looks close enough."
    assert payload["reviewer_name"] == "Admin"

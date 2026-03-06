import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

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
os.environ.setdefault("CORS_ORIGINS", "https://theweathermodels.com")
os.environ.setdefault("TOKEN_DB_PATH", "/tmp/twf_test_tokens.sqlite3")
os.environ.setdefault("TOKEN_ENC_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")

from app import main as main_module
from app.auth import twf_oauth

pytestmark = pytest.mark.anyio


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    with main_module._twf_rate_lock:
        main_module._twf_ip_windows.clear()
        main_module._twf_session_windows.clear()
        main_module._twf_last_prune_monotonic = 0.0

    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as test_client:
        yield test_client

    with main_module._twf_rate_lock:
        main_module._twf_ip_windows.clear()
        main_module._twf_session_windows.clear()
        main_module._twf_last_prune_monotonic = 0.0


async def test_non_twf_validation_still_422_detail(client: httpx.AsyncClient) -> None:
    response = await client.get(
        "/api/v4/sample",
        params={
            "model": "hrrr",
            "run": "latest",
            "var": "tmp2m",
            "fh": 1,
            "lat": 999,
            "lon": -97,
        },
    )

    assert response.status_code == 422
    payload = response.json()
    assert "detail" in payload
    assert isinstance(payload["detail"], list)


async def test_sample_batch_cors_preflight_allows_content_type(client: httpx.AsyncClient) -> None:
    response = await client.options(
        "/api/v4/sample/batch",
        headers={
            "Origin": "https://theweathermodels.com",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == "https://theweathermodels.com"
    allow_headers = response.headers.get("access-control-allow-headers", "").lower()
    assert "content-type" in allow_headers


async def test_twf_share_post_invalid_payload_is_enveloped(client: httpx.AsyncClient) -> None:
    response = await client.post("/twf/share/post", json={"topic_id": 1})

    assert response.status_code == 400
    payload = response.json()
    assert payload["error"]["code"] == "TWF_VALIDATION_ERROR"
    assert isinstance(payload["error"]["message"], str)


async def test_twf_share_post_payload_too_large(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/twf/share/post",
        json={"topic_id": 1, "content": "x" * 17000},
    )

    assert response.status_code == 413
    payload = response.json()
    assert payload == {"error": {"code": "PAYLOAD_TOO_LARGE", "message": "Request body too large"}}


async def test_twf_share_post_rate_limited_returns_retry_after(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sess = twf_oauth.TwfSession(
        session_id="sid-1",
        member_id=1,
        display_name="tester",
        photo_url=None,
        access_token="token",
        refresh_token="refresh",
        expires_at=9999999999,
    )

    async def fake_create_post(
        _sess: twf_oauth.TwfSession,
        topic_id: int,
        content: str,
    ) -> dict[str, object]:
        return {"id": 11, "url": "https://example.com/post/11", "topic": {"id": topic_id}}

    monkeypatch.setattr(main_module.twf_oauth, "get_session", lambda _sid: sess)
    monkeypatch.setattr(main_module.twf_oauth, "create_post", fake_create_post)
    monkeypatch.setattr(main_module, "_TWF_IP_LIMIT", 1)
    monkeypatch.setattr(main_module, "_TWF_SESSION_LIMIT", 1)
    client.cookies.set(twf_oauth.SESSION_COOKIE_NAME, sess.session_id)

    ok_response = await client.post(
        "/twf/share/post",
        json={"topic_id": 10, "content": "hello"},
    )
    assert ok_response.status_code == 200

    limited_response = await client.post(
        "/twf/share/post",
        json={"topic_id": 10, "content": "hello again"},
    )
    assert limited_response.status_code == 429
    assert limited_response.headers.get("Retry-After")
    payload = limited_response.json()
    assert payload == {
        "error": {"code": "RATE_LIMITED", "message": "Too many requests. Try again shortly."}
    }

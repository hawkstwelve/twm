from __future__ import annotations

import base64
import html
import hashlib
import json
import logging
import os
import secrets
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, NoReturn, Optional, Tuple

import httpx
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)


# ----------------------------
# Config
# ----------------------------

def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v

TWF_BASE = _env("TWF_BASE")
CLIENT_ID = _env("TWF_CLIENT_ID")
CLIENT_SECRET = _env("TWF_CLIENT_SECRET")
REDIRECT_URI = _env("TWF_REDIRECT_URI")
def _resolved_scopes() -> str:
    raw = _env("TWF_SCOPES", "profile").strip()
    parts = [p for p in raw.split() if p]
    if "forums_posts" not in parts:
        parts.append("forums_posts")
    return " ".join(parts)

SCOPES = _resolved_scopes()
FRONTEND_RETURN = _env("FRONTEND_RETURN")

SESSION_COOKIE_NAME = os.getenv("SESSION_COOKIE_NAME", "twm_session")
OAUTH_COOKIE_NAME = os.getenv("OAUTH_COOKIE_NAME", "twm_twf_oauth")

TOKEN_DB_PATH = _env("TOKEN_DB_PATH")
TOKEN_ENC_KEY = _env("TOKEN_ENC_KEY")
FERNET = Fernet(TOKEN_ENC_KEY.encode("utf-8"))

AUTHORIZE_ENDPOINT = f"{TWF_BASE.rstrip('/')}/oauth/authorize/"
TOKEN_ENDPOINT = f"{TWF_BASE.rstrip('/')}/oauth/token/"
API_ME_ENDPOINT = os.getenv("TWF_ME_ENDPOINT", f"{TWF_BASE.rstrip('/')}/api/index.php?/core/me").strip()
API_CREATE_TOPIC = os.getenv("TWF_TOPICS_ENDPOINT", f"{TWF_BASE.rstrip('/')}/api/index.php?/forums/topics").strip()
API_LIST_TOPICS = os.getenv("TWF_LIST_TOPICS_ENDPOINT", f"{TWF_BASE.rstrip('/')}/api/index.php?/forums/topics").strip()
API_LIST_FORUMS = os.getenv("TWF_FORUMS_ENDPOINT", f"{TWF_BASE.rstrip('/')}/api/index.php?/forums/forums").strip()
API_CREATE_POST = os.getenv("TWF_POSTS_ENDPOINT", f"{TWF_BASE.rstrip('/')}/api/index.php?/forums/posts").strip()

TWF_API_KEY = os.getenv("TWF_API_KEY", "").strip()

def _auth_headers(access_token: str) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {access_token}"}
    if TWF_API_KEY:
        headers["X-API-Key"] = TWF_API_KEY
    return headers


# ----------------------------
# PKCE helpers
# ----------------------------

def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

def pkce_pair() -> Tuple[str, str]:
    verifier = _b64url(os.urandom(32))
    challenge = _b64url(hashlib.sha256(verifier.encode("ascii")).digest())
    return verifier, challenge


# ----------------------------
# Token store (SQLite + Fernet)
# ----------------------------

def _db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(TOKEN_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(TOKEN_DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS twf_sessions (
            session_id TEXT PRIMARY KEY,
            member_id INTEGER NOT NULL,
            display_name TEXT NOT NULL,
            photo_url TEXT,
            access_token_enc BLOB NOT NULL,
            refresh_token_enc BLOB NOT NULL,
            expires_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    # Backward-compatible migration for existing DBs created before photo_url existed.
    cols = {str(row[1]) for row in conn.execute("PRAGMA table_info(twf_sessions)").fetchall()}
    if "photo_url" not in cols:
        conn.execute("ALTER TABLE twf_sessions ADD COLUMN photo_url TEXT")
    return conn

def _enc(s: str) -> bytes:
    return FERNET.encrypt(s.encode("utf-8"))

def _dec(b: bytes) -> str:
    return FERNET.decrypt(b).decode("utf-8")

@dataclass
class TwfSession:
    session_id: str
    member_id: int
    display_name: str
    photo_url: str | None
    access_token: str
    refresh_token: str
    expires_at: int

@dataclass
class TwfUpstreamError(Exception):
    status_code: int
    code: str
    message: str
    upstream_status: int | None = None
    upstream_code: str | None = None
    upstream_message: str | None = None

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


_UPSTREAM_BODY_MAX = 500


def _truncate_upstream_body(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    return text[:_UPSTREAM_BODY_MAX]


def _parse_ips_error_response(response: httpx.Response) -> tuple[str | None, str | None, str | None]:
    upstream_body = _truncate_upstream_body(response.text)
    try:
        payload = response.json()
    except Exception:
        return None, None, upstream_body

    if not isinstance(payload, dict):
        return None, None, upstream_body

    # IPS commonly returns {errorCode, errorMessage} but some responses may nest under {error:{...}}
    src: Any = payload
    if isinstance(payload.get("error"), dict):
        src = payload.get("error")

    error_code = src.get("errorCode") if isinstance(src, dict) else None
    error_message = src.get("errorMessage") if isinstance(src, dict) else None

    parsed_code = str(error_code) if isinstance(error_code, str) and error_code.strip() else None
    parsed_message = str(error_message) if isinstance(error_message, str) and error_message.strip() else None
    return parsed_code, parsed_message, upstream_body


def _map_upstream_error(upstream_status: int | None, error_message: str | None) -> tuple[int, str, str]:
    normalized_message = (error_message or "").strip().upper()
    if normalized_message == "NO_TOPIC":
        return 400, "IPS_NO_TOPIC", "Topic not found or you don't have access."
    if normalized_message == "NO_API_KEY":
        return 502, "IPS_NO_API_KEY", "Forum API key missing/invalid on upstream."
    if upstream_status in (401, 403):
        return 401, "IPS_UNAUTHORIZED", "Forum authorization failed."
    if upstream_status == 429:
        return 429, "IPS_RATE_LIMITED", "Forum API rate limit exceeded."
    if upstream_status is not None and upstream_status >= 500:
        return 502, "IPS_UPSTREAM_ERROR", "Forum API temporarily unavailable."
    return 502, "IPS_UPSTREAM_ERROR", "Forum API temporarily unavailable."

def _raise_mapped_response_error(response: httpx.Response) -> NoReturn:
    upstream_status = response.status_code
    upstream_code, error_message, upstream_body = _parse_ips_error_response(response)
    status_code, code, message = _map_upstream_error(upstream_status, error_message)
    upstream_message = error_message or upstream_body
    err = TwfUpstreamError(
        status_code=status_code,
        code=code,
        message=message,
        upstream_status=upstream_status,
        upstream_code=upstream_code,
        upstream_message=upstream_message,
    )
    raise err

def _raise_mapped_request_error(exc: httpx.RequestError, upstream_message: str | None = None) -> NoReturn:
    # Network / transport failures: treat as upstream unavailable.
    err = TwfUpstreamError(
        status_code=502,
        code="IPS_UPSTREAM_ERROR",
        message="Forum API temporarily unavailable.",
        upstream_status=None,
        upstream_code=None,
        upstream_message=upstream_message or str(exc),
    )
    raise err from exc

async def _request_json_with_variants(
    *,
    method: str,
    urls: list[str],
    headers: dict[str, str],
    timeout: float,
    data: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
) -> dict[str, Any]:
    last_error: TwfUpstreamError | None = None
    async with httpx.AsyncClient(timeout=timeout) as client:
        for url in urls:
            try:
                return await _request_json(client, method, url, headers=headers, data=data, params=params)
            except TwfUpstreamError as exc:
                last_error = exc
                # Only retry on 404 (slash/no-slash variant) or true transient upstream failures.
                # For semantic 4xx (NO_TOPIC/UNAUTHORIZED/etc.), retrying other variants is noise.
                if exc.upstream_status in (404, None) or (exc.upstream_status is not None and exc.upstream_status >= 500):
                    continue
                raise

    if last_error is not None:
        raise last_error
    raise TwfUpstreamError(
        status_code=502,
        code="IPS_UPSTREAM_ERROR",
        message="Forum API temporarily unavailable.",
        upstream_status=None,
        upstream_code=None,
        upstream_message=None,
    )


async def _request_json(client: httpx.AsyncClient, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    try:
        if method == "GET":
            kwargs.pop("data", None)
            r = await client.get(url, **kwargs)
        elif method == "POST":
            r = await client.post(url, **kwargs)
        else:
            raise RuntimeError(f"Unsupported method: {method}")
    except httpx.RequestError as exc:
        _raise_mapped_request_error(exc)

    if r.status_code >= 400:
        _raise_mapped_response_error(r)
    return r.json()


def upsert_session(sess: TwfSession) -> None:
    now = int(time.time())
    with _db() as conn:
        conn.execute(
            """
            INSERT INTO twf_sessions(session_id, member_id, display_name, photo_url, access_token_enc, refresh_token_enc, expires_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
              member_id=excluded.member_id,
              display_name=excluded.display_name,
              photo_url=excluded.photo_url,
              access_token_enc=excluded.access_token_enc,
              refresh_token_enc=excluded.refresh_token_enc,
              expires_at=excluded.expires_at,
              updated_at=excluded.updated_at
            """,
            (
                sess.session_id,
                sess.member_id,
                sess.display_name,
                sess.photo_url,
                _enc(sess.access_token),
                _enc(sess.refresh_token),
                sess.expires_at,
                now,
                now,
            ),
        )

def get_session(session_id: str) -> Optional[TwfSession]:
    with _db() as conn:
        row = conn.execute(
            "SELECT session_id, member_id, display_name, photo_url, access_token_enc, refresh_token_enc, expires_at FROM twf_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
    if not row:
        return None
    return TwfSession(
        session_id=row[0],
        member_id=int(row[1]),
        display_name=str(row[2]),
        photo_url=str(row[3]) if row[3] else None,
        access_token=_dec(row[4]),
        refresh_token=_dec(row[5]),
        expires_at=int(row[6]),
    )

def delete_session(session_id: str) -> None:
    with _db() as conn:
        conn.execute("DELETE FROM twf_sessions WHERE session_id=?", (session_id,))


# ----------------------------
# OAuth + API calls
# ----------------------------

def build_authorize_url(state: str, code_challenge: str) -> str:
    from urllib.parse import urlencode
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return f"{AUTHORIZE_ENDPOINT}?{urlencode(params)}"

async def exchange_code_for_token(code: str, code_verifier: str) -> dict[str, Any]:
    data = {
        "grant_type": "authorization_code",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "code": code,
        "code_verifier": code_verifier,
    }
    async with httpx.AsyncClient(timeout=20) as client:
        return await _request_json(client, "POST", TOKEN_ENDPOINT, data=data)

async def refresh_access_token(refresh_token: str) -> dict[str, Any]:
    data = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
    }
    async with httpx.AsyncClient(timeout=20) as client:
        return await _request_json(client, "POST", TOKEN_ENDPOINT, data=data)

async def twf_me(access_token: str) -> dict[str, Any]:
    headers = _auth_headers(access_token)
    base = API_ME_ENDPOINT
    # Try both slash/no-slash variants because IPS installs vary.
    urls = [base, base.rstrip("/"), base.rstrip("/") + "/"]

    last_error: TwfUpstreamError | None = None
    last_request_exc: httpx.RequestError | None = None
    async with httpx.AsyncClient(timeout=20) as client:
        for url in urls:
            try:
                return await _request_json(client, "GET", url, headers=headers)
            except TwfUpstreamError as err:
                last_error = err
                continue
            except httpx.RequestError as exc:
                last_request_exc = exc
                continue

    if last_error is not None:
        raise last_error
    if last_request_exc is not None:
        _raise_mapped_request_error(last_request_exc)
    raise TwfUpstreamError(
        status_code=502,
        code="IPS_UPSTREAM_ERROR",
        message="Forum API temporarily unavailable.",
        upstream_status=None,
        upstream_code=None,
        upstream_message=None,
    )

async def ensure_fresh_tokens(sess: TwfSession) -> TwfSession:
    # refresh if expiring within 60 seconds
    if sess.expires_at > int(time.time()) + 60:
        return sess

    tok = await refresh_access_token(sess.refresh_token)
    access = tok["access_token"]
    refresh = tok.get("refresh_token", sess.refresh_token)
    expires_in = int(tok.get("expires_in", 3600))
    sess.access_token = access
    sess.refresh_token = refresh
    sess.expires_at = int(time.time()) + expires_in
    upsert_session(sess)
    return sess

async def create_topic(sess: TwfSession, forum_id: int, title: str, content: str) -> dict[str, Any]:
    sess = await ensure_fresh_tokens(sess)
    headers = _auth_headers(sess.access_token)

    # Invision expects form-encoded for POST/PUT in practice; use data= not json=
    data = {
        "forum": str(forum_id),
        "title": title,
        "post": content,
    }

    base = API_CREATE_TOPIC
    urls = [base, base.rstrip("/"), base.rstrip("/") + "/"]

    return await _request_json_with_variants(
        method="POST",
        urls=urls,
        headers=headers,
        timeout=30,
        data=data,
    )

def _plain_text_to_ips_html(content: str) -> str:
    escaped = html.escape(content, quote=False)
    return escaped.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")

async def create_post(sess: TwfSession, topic_id: int, content: str) -> dict[str, Any]:
    sess = await ensure_fresh_tokens(sess)
    headers = _auth_headers(sess.access_token)

    # Match create_topic(): form-encoded, not JSON
    data = {
        "topic": str(topic_id),
        "post": _plain_text_to_ips_html(content),
    }

    base = API_CREATE_POST
    urls = [base, base.rstrip("/"), base.rstrip("/") + "/"]

    return await _request_json_with_variants(
        method="POST",
        urls=urls,
        headers=headers,
        timeout=30,
        data=data,
    )

async def list_forums(sess: TwfSession) -> dict[str, Any]:
    sess = await ensure_fresh_tokens(sess)
    headers = _auth_headers(sess.access_token)

    # Try configured endpoint plus slash/no-slash variants.
    base = API_LIST_FORUMS
    urls = [base, base.rstrip("/"), base.rstrip("/") + "/"]

    return await _request_json_with_variants(
        method="GET",
        urls=urls,
        headers=headers,
        timeout=20,
    )


async def list_topics(sess: TwfSession, forum_id: int, pinned: bool, per_page: int) -> dict[str, Any]:
    sess = await ensure_fresh_tokens(sess)
    headers = _auth_headers(sess.access_token)

    base = API_LIST_TOPICS
    urls = [base, base.rstrip("/"), base.rstrip("/") + "/"]
    # IPS query parameter names can vary by install; these are the expected/default names.
    params = {
        "forum": str(forum_id),
        "pinned": "1" if pinned else "0",
        "sortBy": "updated",
        "sortDir": "desc",
        "perPage": str(per_page),
    }
    logger.debug(
        "TWF list_topics request forum_id=%s pinned=%s per_page=%s base_url=%s",
        forum_id,
        pinned,
        per_page,
        urls[0],
    )

    return await _request_json_with_variants(
        method="GET",
        urls=urls,
        headers=headers,
        timeout=20,
        params=params,
    )


# ----------------------------
# Cookie payload helpers (state/verifier only)
# ----------------------------

def pack_oauth_cookie(state: str, verifier: str) -> str:
    blob = json.dumps({"state": state, "verifier": verifier}).encode("utf-8")
    return _b64url(blob)

def unpack_oauth_cookie(val: str) -> dict[str, str]:
    padded = val + "=" * (-len(val) % 4)
    raw = base64.urlsafe_b64decode(padded.encode("ascii"))
    obj = json.loads(raw.decode("utf-8"))
    return {"state": obj["state"], "verifier": obj["verifier"]}

def new_session_id() -> str:
    return secrets.token_urlsafe(32)

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Optional, Tuple

import httpx
from cryptography.fernet import Fernet


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
SCOPES = _env("TWF_SCOPES", "profile").strip()
FRONTEND_RETURN = _env("FRONTEND_RETURN")

SESSION_COOKIE_NAME = os.getenv("SESSION_COOKIE_NAME", "twm_session")
OAUTH_COOKIE_NAME = os.getenv("OAUTH_COOKIE_NAME", "twm_twf_oauth")

TOKEN_DB_PATH = _env("TOKEN_DB_PATH")
TOKEN_ENC_KEY = _env("TOKEN_ENC_KEY")
FERNET = Fernet(TOKEN_ENC_KEY.encode("utf-8"))

AUTHORIZE_ENDPOINT = f"{TWF_BASE.rstrip('/')}/oauth/authorize/"
TOKEN_ENDPOINT = f"{TWF_BASE.rstrip('/')}/oauth/token/"
API_ME_ENDPOINT = f"{TWF_BASE.rstrip('/')}/api/core/me"
API_CREATE_TOPIC = f"{TWF_BASE.rstrip('/')}/api/forums/topics"
API_LIST_FORUMS = f"{TWF_BASE.rstrip('/')}/api/forums/forums"


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
            access_token_enc BLOB NOT NULL,
            refresh_token_enc BLOB NOT NULL,
            expires_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
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
    access_token: str
    refresh_token: str
    expires_at: int

def upsert_session(sess: TwfSession) -> None:
    now = int(time.time())
    with _db() as conn:
        conn.execute(
            """
            INSERT INTO twf_sessions(session_id, member_id, display_name, access_token_enc, refresh_token_enc, expires_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
              member_id=excluded.member_id,
              display_name=excluded.display_name,
              access_token_enc=excluded.access_token_enc,
              refresh_token_enc=excluded.refresh_token_enc,
              expires_at=excluded.expires_at,
              updated_at=excluded.updated_at
            """,
            (
                sess.session_id,
                sess.member_id,
                sess.display_name,
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
            "SELECT session_id, member_id, display_name, access_token_enc, refresh_token_enc, expires_at FROM twf_sessions WHERE session_id=?",
            (session_id,),
        ).fetchone()
    if not row:
        return None
    return TwfSession(
        session_id=row[0],
        member_id=int(row[1]),
        display_name=str(row[2]),
        access_token=_dec(row[3]),
        refresh_token=_dec(row[4]),
        expires_at=int(row[5]),
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
        r = await client.post(TOKEN_ENDPOINT, data=data)
        r.raise_for_status()
        return r.json()

async def refresh_access_token(refresh_token: str) -> dict[str, Any]:
    data = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
    }
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(TOKEN_ENDPOINT, data=data)
        r.raise_for_status()
        return r.json()

async def twf_me(access_token: str) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {access_token}"}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(API_ME_ENDPOINT, headers=headers)
        r.raise_for_status()
        return r.json()

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
    headers = {"Authorization": f"Bearer {sess.access_token}"}

    # Invision expects form-encoded for POST/PUT in practice; use data= not json=
    data = {
        "forum": str(forum_id),
        "title": title,
        "post": content,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(API_CREATE_TOPIC, headers=headers, data=data)
        r.raise_for_status()
        return r.json()

async def list_forums(sess: TwfSession) -> dict[str, Any]:
    sess = await ensure_fresh_tokens(sess)
    headers = {"Authorization": f"Bearer {sess.access_token}"}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(API_LIST_FORUMS, headers=headers)
        r.raise_for_status()
        return r.json()


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
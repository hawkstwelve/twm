"""CartoSky API — canonical discovery + sampling endpoints."""

from __future__ import annotations

import hashlib
import io
import json
import logging
import math
import os
import re
import secrets
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import numpy as np
import rasterio
from fastapi import FastAPI, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from PIL import Image, ImageFilter
from pyproj import Transformer
from rasterio.enums import Resampling
from rasterio.windows import Window
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .config.regions import REGION_PRESETS
from .models.registry import list_model_capabilities
from .services.builder.colorize import float_to_rgba
from .services.render_resampling import (
    compute_loop_output_shape,
    display_resampling_override,
    high_quality_loop_resampling,
    log_fixed_loop_size_once,
    loop_fixed_width_for_tier,
    loop_max_dim_for_tier,
    loop_quality_for_tier,
    rasterio_resampling_for_loop,
    use_value_render_for_variable,
    variable_kind,
    variable_color_map_id,
)
from .services import admin_telemetry, share_media as share_media_service
from backend.app.auth import twf_oauth

logger = logging.getLogger(__name__)

def _env_value(*names: str, default: str = "") -> str:
    for name in names:
        raw = os.environ.get(name)
        if raw is not None and raw != "":
            return raw
    return default


def _normalized_path_prefix(value: str, *, default: str) -> str:
    raw = (value or default).strip()
    if not raw:
        raw = default
    return f"/{raw.strip('/')}/"


DATA_ROOT = Path(_env_value("CARTOSKY_DATA_ROOT", "CARTOSKY_V3_DATA_ROOT", "TWF_V3_DATA_ROOT", default="./data"))
PUBLISHED_ROOT = DATA_ROOT / "published"
MANIFESTS_ROOT = DATA_ROOT / "manifests"
LOOP_CACHE_ROOT = Path(
    _env_value(
        "CARTOSKY_LOOP_CACHE_ROOT",
        "CARTOSKY_V3_LOOP_CACHE_ROOT",
        "TWF_V3_LOOP_CACHE_ROOT",
        default=str(DATA_ROOT / "loop_cache"),
    )
)
LOOP_URL_PREFIX = _normalized_path_prefix(
    _env_value("CARTOSKY_LOOP_URL_PREFIX", "CARTOSKY_V3_LOOP_URL_PREFIX", "TWF_V3_LOOP_URL_PREFIX", default="/loop/"),
    default="/loop/",
)
RUNTIME_ONLY_LOOP_URL_VARS = {"radar_ptype"}
CAPABILITIES_CONTRACT_VERSION = "v1"

_RUN_ID_RE = re.compile(r"^\d{8}_\d{2}z$")
_JSON_CACHE_RECHECK_SECONDS = float(
    _env_value(
        "CARTOSKY_JSON_CACHE_RECHECK_SECONDS",
        "CARTOSKY_V3_JSON_CACHE_RECHECK_SECONDS",
        "TWF_V3_JSON_CACHE_RECHECK_SECONDS",
        default="1.0",
    )
)


def _env_bool(*names: str, default: bool) -> bool:
    raw = _env_value(*names).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    logger.warning("Invalid %s=%r; using fallback=%s", "/".join(names), raw, default)
    return default


def _env_int(*names: str, default: int, min_value: int = 0) -> int:
    raw = _env_value(*names).strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using fallback=%d", "/".join(names), raw, default)
        return default
    return parsed if parsed >= min_value else default


def _env_float(*names: str, default: float, min_value: float = 0.0) -> float:
    raw = _env_value(*names).strip()
    if not raw:
        return default
    try:
        parsed = float(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using fallback=%s", "/".join(names), raw, default)
        return default
    return parsed if parsed >= min_value else default


LOOP_WEBP_QUALITY = int(
    _env_value("CARTOSKY_LOOP_WEBP_QUALITY", "CARTOSKY_V3_LOOP_WEBP_QUALITY", "TWF_V3_LOOP_WEBP_QUALITY", default="82")
)
LOOP_WEBP_MAX_DIM = int(
    _env_value("CARTOSKY_LOOP_WEBP_MAX_DIM", "CARTOSKY_V3_LOOP_WEBP_MAX_DIM", "TWF_V3_LOOP_WEBP_MAX_DIM", default="1600")
)
LOOP_WEBP_TIER1_QUALITY = int(
    _env_value(
        "CARTOSKY_LOOP_WEBP_TIER1_QUALITY",
        "CARTOSKY_V3_LOOP_WEBP_TIER1_QUALITY",
        "TWF_V3_LOOP_WEBP_TIER1_QUALITY",
        default="86",
    )
)
LOOP_WEBP_TIER1_MAX_DIM = int(
    _env_value(
        "CARTOSKY_LOOP_WEBP_TIER1_MAX_DIM",
        "CARTOSKY_V3_LOOP_WEBP_TIER1_MAX_DIM",
        "TWF_V3_LOOP_WEBP_TIER1_MAX_DIM",
        default="2400",
    )
)
LOOP_WEBP_TIER0_FIXED_W = int(
    _env_value(
        "CARTOSKY_LOOP_WEBP_TIER0_FIXED_W",
        "CARTOSKY_V3_LOOP_WEBP_TIER0_FIXED_W",
        "TWF_V3_LOOP_WEBP_TIER0_FIXED_W",
        default="1600",
    )
)
LOOP_WEBP_TIER1_FIXED_W = int(
    _env_value(
        "CARTOSKY_LOOP_WEBP_TIER1_FIXED_W",
        "CARTOSKY_V3_LOOP_WEBP_TIER1_FIXED_W",
        "TWF_V3_LOOP_WEBP_TIER1_FIXED_W",
        default="2400",
    )
)
LOOP_SHARPEN_ENABLE = _env_bool(
    "CARTOSKY_LOOP_SHARPEN_ENABLE",
    "CARTOSKY_V3_LOOP_SHARPEN_ENABLE",
    "TWF_V3_LOOP_SHARPEN_ENABLE",
    default=True,
)
LOOP_SHARPEN_RADIUS = _env_float(
    "CARTOSKY_LOOP_SHARPEN_RADIUS",
    "CARTOSKY_V3_LOOP_SHARPEN_RADIUS",
    "TWF_V3_LOOP_SHARPEN_RADIUS",
    default=1.2,
    min_value=0.0,
)
LOOP_SHARPEN_PERCENT = _env_int(
    "CARTOSKY_LOOP_SHARPEN_PERCENT",
    "CARTOSKY_V3_LOOP_SHARPEN_PERCENT",
    "TWF_V3_LOOP_SHARPEN_PERCENT",
    default=35,
    min_value=0,
)
LOOP_SHARPEN_THRESHOLD = _env_int(
    "CARTOSKY_LOOP_SHARPEN_THRESHOLD",
    "CARTOSKY_V3_LOOP_SHARPEN_THRESHOLD",
    "TWF_V3_LOOP_SHARPEN_THRESHOLD",
    default=3,
    min_value=0,
)
SAMPLE_CACHE_TTL_SECONDS = float(
    _env_value(
        "CARTOSKY_SAMPLE_CACHE_TTL_SECONDS",
        "CARTOSKY_V3_SAMPLE_CACHE_TTL_SECONDS",
        "TWF_V3_SAMPLE_CACHE_TTL_SECONDS",
        default="2.0",
    )
)
SAMPLE_INFLIGHT_WAIT_SECONDS = float(
    _env_value(
        "CARTOSKY_SAMPLE_INFLIGHT_WAIT_SECONDS",
        "CARTOSKY_V3_SAMPLE_INFLIGHT_WAIT_SECONDS",
        "TWF_V3_SAMPLE_INFLIGHT_WAIT_SECONDS",
        default="0.2",
    )
)
SAMPLE_RATE_LIMIT_WINDOW_SECONDS = float(
    _env_value(
        "CARTOSKY_SAMPLE_RATE_LIMIT_WINDOW_SECONDS",
        "CARTOSKY_V3_SAMPLE_RATE_LIMIT_WINDOW_SECONDS",
        "TWF_V3_SAMPLE_RATE_LIMIT_WINDOW_SECONDS",
        default="1.0",
    )
)
SAMPLE_RATE_LIMIT_MAX_REQUESTS = int(
    _env_value(
        "CARTOSKY_SAMPLE_RATE_LIMIT_MAX_REQUESTS",
        "CARTOSKY_V3_SAMPLE_RATE_LIMIT_MAX_REQUESTS",
        "TWF_V3_SAMPLE_RATE_LIMIT_MAX_REQUESTS",
        default="240",
    )
)

LOOP_TIER_CONFIG: dict[int, dict[str, int]] = {
    0: {
        "max_dim": LOOP_WEBP_MAX_DIM,
        "quality": LOOP_WEBP_QUALITY,
        "fixed_w": LOOP_WEBP_TIER0_FIXED_W,
    },
    1: {
        "max_dim": LOOP_WEBP_TIER1_MAX_DIM,
        "quality": LOOP_WEBP_TIER1_QUALITY,
        "fixed_w": LOOP_WEBP_TIER1_FIXED_W,
    },
}

CACHE_HIT = "public, max-age=31536000, immutable"
CACHE_MISS = "public, max-age=15"
_TWF_SHARE_BODY_CAP_BYTES = 16 * 1024
_TWF_RATE_WINDOW_SECONDS = 60.0
_TWF_IP_LIMIT = 20
_TWF_SESSION_LIMIT = 10
_TWF_RATE_LIMIT_MESSAGE = "Too many requests. Try again shortly."
_TWF_RATE_LIMIT_PATHS = {"/twf/share/topic", "/twf/share/post"}
_TWF_GUARDED_PATHS = _TWF_RATE_LIMIT_PATHS
_TWF_ERROR_PATHS = {
    "/auth/twf/status",
    "/auth/twf/disconnect",
    "/twf/forums",
    "/twf/topics",
    "/twf/share/topic",
    "/twf/share/post",
}
_TWF_RATE_PRUNE_INTERVAL_SECONDS = 60.0
_ADMIN_WINDOW_SECONDS = {
    "24h": 24 * 60 * 60,
    "7d": 7 * 24 * 60 * 60,
    "30d": 30 * 24 * 60 * 60,
}


def _parse_admin_member_ids(raw: str) -> set[int]:
    member_ids: set[int] = set()
    for part in raw.split(","):
        trimmed = part.strip()
        if not trimmed:
            continue
        try:
            member_ids.add(int(trimmed))
        except ValueError:
            logger.warning("Skipping invalid CARTOSKY_ADMIN_MEMBER_IDS/TWM_ADMIN_MEMBER_IDS entry %r", trimmed)
    return member_ids


ADMIN_MEMBER_IDS = _parse_admin_member_ids(_env_value("CARTOSKY_ADMIN_MEMBER_IDS", "TWM_ADMIN_MEMBER_IDS"))

_twf_rate_lock = threading.Lock()
_twf_ip_windows: dict[str, deque[float]] = {}
_twf_session_windows: dict[str, deque[float]] = {}
_twf_last_prune_monotonic = 0.0


def _frames_cache_control(run: str, *, run_complete: bool) -> str:
    if run == "latest" or not run_complete:
        return "public, max-age=60"
    return "public, max-age=31536000, immutable"


def _if_none_match_values(header_value: str) -> list[str]:
    return [v.strip() for v in header_value.split(",") if v.strip()]


def _etag_matches(if_none_match: str | None, etag: str) -> bool:
    if not if_none_match:
        return False
    vals = _if_none_match_values(if_none_match)
    if "*" in vals:
        return True
    return etag in vals


def _make_etag(payload: object) -> str:
    digest = hashlib.md5(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:12]
    return f'"{digest}"'


def _maybe_304(request: Request, *, etag: str, cache_control: str) -> Response | None:
    inm = request.headers.get("if-none-match")
    if _etag_matches(inm, etag):
        return Response(
            status_code=304,
            headers={
                "ETag": etag,
                "Cache-Control": cache_control,
            },
        )
    return None


app = FastAPI(title="CartoSky API", version="4.0.0")

origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
cors_allow_headers = [
    "Accept",
    "Accept-Language",
    "Content-Language",
    "Content-Type",
    "Origin",
    "Authorization",
    "X-Requested-With",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=cors_allow_headers,
)

@dataclass
class TwfApiError(Exception):
    status_code: int
    code: str
    message: str
    upstream_status: int | None = None
    upstream_code: str | None = None
    upstream_message: str | None = None


def _error_payload(
    *,
    code: str,
    message: str,
    upstream_status: int | None = None,
    upstream_code: str | None = None,
    upstream_message: str | None = None,
    upstream_url: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"code": code, "message": message}
    if upstream_status is not None:
        payload["upstream_status"] = upstream_status
    if upstream_code is not None:
        payload["upstream_code"] = upstream_code
    if upstream_message is not None:
        payload["upstream_message"] = upstream_message
    if upstream_url is not None:
        payload["upstream_url"] = upstream_url
    return {"error": payload}


def _error_response(
    *,
    status_code: int,
    code: str,
    message: str,
    upstream_status: int | None = None,
    upstream_code: str | None = None,
    upstream_message: str | None = None,
    upstream_url: str | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content=_error_payload(
            code=code,
            message=message,
            upstream_status=upstream_status,
            upstream_code=upstream_code,
            upstream_message=upstream_message,
            upstream_url=upstream_url,
        ),
        headers=headers,
    )


def _validation_message(exc: RequestValidationError) -> str:
    errors = exc.errors()
    if not errors:
        return "Invalid request payload."
    first = errors[0]
    msg = first.get("msg")
    if isinstance(msg, str) and msg.strip():
        return msg
    return "Invalid request payload."


def _rate_limit_check(
    bucket: dict[str, deque[float]],
    *,
    key: str,
    limit: int,
    window_seconds: float,
    now: float,
) -> int:
    timestamps = bucket.setdefault(key, deque())
    cutoff = now - window_seconds
    while timestamps and timestamps[0] <= cutoff:
        timestamps.popleft()
    if len(timestamps) >= limit:
        retry_after = max(1, int(math.ceil(window_seconds - (now - timestamps[0]))))
        return retry_after
    timestamps.append(now)
    return 0


def _prune_rate_limit_bucket(
    bucket: dict[str, deque[float]],
    *,
    cutoff: float,
) -> None:
    to_delete: list[str] = []
    for key, timestamps in bucket.items():
        while timestamps and timestamps[0] <= cutoff:
            timestamps.popleft()
        if not timestamps:
            to_delete.append(key)
    for key in to_delete:
        bucket.pop(key, None)


def _maybe_prune_rate_limit_state(now: float) -> None:
    global _twf_last_prune_monotonic
    if now - _twf_last_prune_monotonic < _TWF_RATE_PRUNE_INTERVAL_SECONDS:
        return
    cutoff = now - _TWF_RATE_WINDOW_SECONDS
    _prune_rate_limit_bucket(_twf_ip_windows, cutoff=cutoff)
    _prune_rate_limit_bucket(_twf_session_windows, cutoff=cutoff)
    _twf_last_prune_monotonic = now


def _client_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        first = forwarded_for.split(",")[0].strip()
        if first:
            return first
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


@app.middleware("http")
async def twf_share_guards(request: Request, call_next):
    request_id = secrets.token_hex(8)
    request.state.request_id = request_id

    if request.method == "POST" and request.url.path in _TWF_GUARDED_PATHS:
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                if int(content_length) > _TWF_SHARE_BODY_CAP_BYTES:
                    logger.warning(
                        "TWF payload too large request_id=%s path=%s method=%s ip=%s has_session=%s content_length=%s",
                        request_id,
                        request.url.path,
                        request.method,
                        _client_ip(request),
                        bool(request.cookies.get(twf_oauth.SESSION_COOKIE_NAME)),
                        content_length,
                    )
                    response = _error_response(
                        status_code=413,
                        code="PAYLOAD_TOO_LARGE",
                        message="Request body too large",
                    )
                    response.headers["X-Request-ID"] = request_id
                    return response
            except ValueError:
                pass

        body = await request.body()
        buffered_body = body

        async def receive() -> dict[str, Any]:
            nonlocal buffered_body
            chunk = buffered_body
            buffered_body = b""
            return {"type": "http.request", "body": chunk, "more_body": False}

        request = Request(request.scope, receive)
        request._body = body
        if len(body) > _TWF_SHARE_BODY_CAP_BYTES:
            logger.warning(
                "TWF payload too large request_id=%s path=%s method=%s ip=%s has_session=%s body_bytes=%s",
                request_id,
                request.url.path,
                request.method,
                _client_ip(request),
                bool(request.cookies.get(twf_oauth.SESSION_COOKIE_NAME)),
                len(body),
            )
            response = _error_response(
                status_code=413,
                code="PAYLOAD_TOO_LARGE",
                message="Request body too large",
            )
            response.headers["X-Request-ID"] = request_id
            return response

        now = time.monotonic()
        ip = _client_ip(request)
        session_id = request.cookies.get(twf_oauth.SESSION_COOKIE_NAME)
        retry_after = 0
        with _twf_rate_lock:
            _maybe_prune_rate_limit_state(now)
            retry_after = _rate_limit_check(
                _twf_ip_windows,
                key=ip,
                limit=_TWF_IP_LIMIT,
                window_seconds=_TWF_RATE_WINDOW_SECONDS,
                now=now,
            )
            if retry_after == 0 and session_id:
                retry_after = _rate_limit_check(
                    _twf_session_windows,
                    key=session_id,
                    limit=_TWF_SESSION_LIMIT,
                    window_seconds=_TWF_RATE_WINDOW_SECONDS,
                    now=now,
                )
        if retry_after > 0:
            logger.warning(
                "TWF rate limit exceeded request_id=%s path=%s ip=%s has_session=%s retry_after=%s",
                request_id,
                request.url.path,
                ip,
                bool(session_id),
                retry_after,
            )
            response = _error_response(
                status_code=429,
                code="RATE_LIMITED",
                message=_TWF_RATE_LIMIT_MESSAGE,
                headers={"Retry-After": str(retry_after)},
            )
            response.headers["X-Request-ID"] = request_id
            return response

    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


@app.exception_handler(twf_oauth.TwfUpstreamError)
async def twf_upstream_error_handler(request: Request, exc: twf_oauth.TwfUpstreamError) -> JSONResponse:
    rid = getattr(request.state, "request_id", None)
    logger.warning(
        "TWF upstream error request_id=%s path=%s method=%s ip=%s has_session=%s error_code=%s upstream_status=%s upstream_code=%s upstream_message=%s upstream_url=%s status_code=%s",
        rid,
        request.url.path,
        request.method,
        _client_ip(request),
        bool(request.cookies.get(twf_oauth.SESSION_COOKIE_NAME)),
        exc.code,
        exc.upstream_status,
        exc.upstream_code,
        exc.upstream_message,
        exc.upstream_url,
        exc.status_code,
        extra={
            "request_id": rid,
            "path": request.url.path,
            "method": request.method,
            "ip": _client_ip(request),
            "has_session": bool(request.cookies.get(twf_oauth.SESSION_COOKIE_NAME)),
            "error_code": exc.code,
            "upstream_status": exc.upstream_status,
            "upstream_code": exc.upstream_code,
            "upstream_message": exc.upstream_message,
            "upstream_url": exc.upstream_url,
            "status_code": exc.status_code,
        },
    )
    return _error_response(
        status_code=exc.status_code,
        code=exc.code,
        message=exc.message,
        upstream_status=exc.upstream_status,
        upstream_code=exc.upstream_code,
        upstream_message=exc.upstream_message,
        upstream_url=exc.upstream_url,
    )


@app.exception_handler(TwfApiError)
async def twf_api_error_handler(request: Request, exc: TwfApiError) -> JSONResponse:
    rid = getattr(request.state, "request_id", None)
    logger.warning(
        "TWF API error request_id=%s path=%s method=%s ip=%s has_session=%s error_code=%s status_code=%s",
        rid,
        request.url.path,
        request.method,
        _client_ip(request),
        bool(request.cookies.get(twf_oauth.SESSION_COOKIE_NAME)),
        exc.code,
        exc.status_code,
    )
    return _error_response(
        status_code=exc.status_code,
        code=exc.code,
        message=exc.message,
        upstream_status=exc.upstream_status,
        upstream_code=exc.upstream_code,
        upstream_message=exc.upstream_message,
    )


@app.exception_handler(RequestValidationError)
async def request_validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    if request.url.path in _TWF_ERROR_PATHS:
        return _error_response(
            status_code=400,
            code="TWF_VALIDATION_ERROR",
            message=_validation_message(exc),
        )
    return await request_validation_exception_handler(request, exc)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    if request.url.path in _TWF_ERROR_PATHS:
        detail = exc.detail
        if isinstance(detail, dict):
            return _error_response(
                status_code=exc.status_code,
                code=str(detail.get("code") or "HTTP_ERROR"),
                message=str(detail.get("message") or "Request failed."),
            )
        if isinstance(detail, str):
            return _error_response(
                status_code=exc.status_code,
                code="HTTP_ERROR",
                message=detail,
            )
        return _error_response(
            status_code=exc.status_code,
            code="HTTP_ERROR",
            message="Request failed.",
        )
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled server exception")
    return JSONResponse(
        status_code=500,
        content={"error": {"code": "INTERNAL_ERROR", "message": "Unexpected server error"}},
    )

def _require_twf_session(request: Request) -> twf_oauth.TwfSession:
    """Load the linked The Weather Forums OAuth session for the current browser session.

    Uses the HttpOnly session cookie set by /auth/twf/callback and loads tokens from server-side storage.
    """
    sid = request.cookies.get(twf_oauth.SESSION_COOKIE_NAME)
    if not sid:
        raise TwfApiError(
            status_code=401,
            code="TWF_NOT_LOGGED_IN",
            message="Not logged in",
        )
    sess = twf_oauth.get_session(sid)
    if not sess:
        raise TwfApiError(
            status_code=401,
            code="TWF_SESSION_NOT_FOUND",
            message="Session not found",
        )
    return sess


def _maybe_twf_session(request: Request) -> twf_oauth.TwfSession | None:
    sid = request.cookies.get(twf_oauth.SESSION_COOKIE_NAME)
    if not sid:
        return None
    return twf_oauth.get_session(sid)


def _is_admin_member(member_id: int) -> bool:
    return member_id in ADMIN_MEMBER_IDS


def _require_admin_session(request: Request) -> twf_oauth.TwfSession:
    sess = _require_twf_session(request)
    if not _is_admin_member(sess.member_id):
        raise TwfApiError(
            status_code=403,
            code="TWF_ADMIN_REQUIRED",
            message="Admin access required",
        )
    return sess


def _resolve_window_seconds(window: str) -> int:
    normalized = window.strip().lower()
    if normalized not in _ADMIN_WINDOW_SECONDS:
        raise TwfApiError(
            status_code=400,
            code="INVALID_WINDOW",
            message="Window must be one of: 24h, 7d, 30d.",
        )
    return _ADMIN_WINDOW_SECONDS[normalized]


def _resolve_bucket(window: str, bucket: str) -> str:
    normalized = bucket.strip().lower()
    if normalized == "auto":
        return "hour" if window in {"24h", "7d"} else "day"
    if normalized not in {"hour", "day"}:
        raise TwfApiError(
            status_code=400,
            code="INVALID_BUCKET",
            message="Bucket must be one of: auto, hour, day.",
        )
    return normalized


def _normalize_filter_value(value: str | None) -> str | None:
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed or trimmed.lower() == "all":
        return None
    return trimmed


def _share_media_error_response(*, status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
            }
        },
    )

# ----------------------------
# TWF OAuth + Share Routes
# ----------------------------

# NOTE: add these imports near your other imports if you don't already have them:
# from pydantic import BaseModel, Field


def _sanitize_twf_return_to(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    trimmed = value.strip()
    if not trimmed or not trimmed.startswith("/") or trimmed.startswith("//"):
        return None
    parsed = urlsplit(trimmed)
    if parsed.scheme or parsed.netloc:
        return None
    return trimmed


def _twf_frontend_redirect_url(return_to: str | None, **params: str) -> str:
    fallback = urlsplit(twf_oauth.FRONTEND_RETURN)
    target_path = _sanitize_twf_return_to(return_to) or fallback.path or "/"
    existing_params = dict(parse_qsl(fallback.query, keep_blank_values=True)) if target_path == fallback.path else {}
    existing_params.update({key: value for key, value in params.items() if value})
    return urlunsplit((fallback.scheme, fallback.netloc, target_path, urlencode(existing_params), ""))


@app.get("/auth/twf/start")
async def twf_start(return_to: str | None = None) -> RedirectResponse:
    state = secrets.token_urlsafe(24)
    verifier, challenge = twf_oauth.pkce_pair()
    url = twf_oauth.build_authorize_url(state, challenge)
    resolved_return_to = _sanitize_twf_return_to(return_to)

    resp = RedirectResponse(url=url, status_code=302)
    # Store only state + PKCE verifier (short-lived)
    resp.set_cookie(
        key=twf_oauth.OAUTH_COOKIE_NAME,
        value=twf_oauth.pack_oauth_cookie(state, verifier, resolved_return_to),
        httponly=True,
        secure=True,
        samesite="none",
        max_age=10 * 60,
        path="/",
    )
    return resp

@app.get("/auth/twf/callback")
async def twf_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
) -> RedirectResponse:
    def _error_redirect(message: str, return_to: str | None = None) -> RedirectResponse:
        return RedirectResponse(
            url=_twf_frontend_redirect_url(return_to, twf="error", twf_message=message),
            status_code=302,
        )

    packed: dict[str, str] | None = None
    try:
        if not code or not state:
            return _error_redirect("Missing code or state.")

        cookie_val = request.cookies.get(twf_oauth.OAUTH_COOKIE_NAME)
        if not cookie_val:
            return _error_redirect("OAuth session expired. Try again.")

        packed = twf_oauth.unpack_oauth_cookie(cookie_val)
        if packed.get("state") != state:
            return _error_redirect("Login verification failed. Try again.", packed.get("return_to"))

        tok = await twf_oauth.exchange_code_for_token(code, packed["verifier"])
        access = tok.get("access_token")
        refresh = tok.get("refresh_token")
        if not isinstance(access, str) or not access:
            return _error_redirect("Login failed. No access token returned.", packed.get("return_to"))
        if not isinstance(refresh, str) or not refresh:
            return _error_redirect("Login failed. No refresh token returned.", packed.get("return_to"))

        expires_in = int(tok.get("expires_in", 3600))
        me = await twf_oauth.twf_me(access)

        member_id = int(me["id"])
        display_name = str(me.get("name") or f"member-{member_id}")
        photo_url_raw = me.get("photoUrl")
        photo_url = str(photo_url_raw) if isinstance(photo_url_raw, str) and photo_url_raw.strip() else None

        sid = twf_oauth.new_session_id()
        twf_oauth.upsert_session(
            twf_oauth.TwfSession(
                session_id=sid,
                member_id=member_id,
                display_name=display_name,
                photo_url=photo_url,
                access_token=access,
                refresh_token=refresh,
                expires_at=int(time.time()) + expires_in,
            )
        )

        resp = RedirectResponse(
            url=_twf_frontend_redirect_url(packed.get("return_to"), twf="linked"),
            status_code=302,
        )

        # App session cookie (separate from forum cookies)
        resp.set_cookie(
            key=twf_oauth.SESSION_COOKIE_NAME,
            value=sid,
            httponly=True,
            secure=True,
            samesite="none",
            max_age=60 * 60 * 24 * 30,
            path="/",
        )

        # Clear short-lived OAuth temp cookie
        resp.delete_cookie(key=twf_oauth.OAUTH_COOKIE_NAME, path="/")
        return resp
    except Exception:
        logger.exception("TWF OAuth callback failed")
        return _error_redirect("Login failed. Please try again.", packed.get("return_to") if packed else None)


@app.get("/auth/twf/status")
async def twf_status(request: Request) -> dict[str, Any]:
    sid = request.cookies.get(twf_oauth.SESSION_COOKIE_NAME)
    if not sid:
        return {"linked": False, "admin": False}

    sess = twf_oauth.get_session(sid)
    if not sess:
        return {"linked": False, "admin": False}

    payload: dict[str, Any] = {
        "linked": True,
        "admin": _is_admin_member(sess.member_id),
        "member_id": sess.member_id,
        "display_name": sess.display_name,
    }
    if sess.photo_url:
        payload["photo_url"] = sess.photo_url
    return payload


class TelemetryEventBase(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    session_id: str = Field(min_length=1, max_length=128)
    model_id: str | None = Field(default=None, max_length=32)
    variable_id: str | None = Field(default=None, max_length=64)
    run_id: str | None = Field(default=None, max_length=32)
    region_id: str | None = Field(default=None, max_length=32)
    forecast_hour: int | None = Field(default=None, ge=0, le=999)
    device_type: str | None = Field(default=None, max_length=24)
    viewport_bucket: str | None = Field(default=None, max_length=24)
    page: str | None = Field(default=None, max_length=120)
    meta: dict[str, Any] | None = None


class PerfTelemetryIn(TelemetryEventBase):
    event_name: str = Field(min_length=1, max_length=64)
    duration_ms: float = Field(ge=0, le=600000)


class UsageTelemetryIn(TelemetryEventBase):
    event_name: str = Field(min_length=1, max_length=64)


@app.post("/api/v4/telemetry/perf", status_code=204)
async def post_perf_telemetry(request: Request, payload: PerfTelemetryIn) -> Response:
    sess = _maybe_twf_session(request)
    try:
        admin_telemetry.record_perf_event(payload.model_dump(), member_id=sess.member_id if sess else None)
    except ValueError as exc:
        raise TwfApiError(status_code=400, code="INVALID_PERF_EVENT", message=str(exc)) from exc
    return Response(status_code=204)


@app.post("/api/v4/telemetry/usage", status_code=204)
async def post_usage_telemetry(request: Request, payload: UsageTelemetryIn) -> Response:
    sess = _maybe_twf_session(request)
    try:
        admin_telemetry.record_usage_event(payload.model_dump(), member_id=sess.member_id if sess else None)
    except ValueError as exc:
        raise TwfApiError(status_code=400, code="INVALID_USAGE_EVENT", message=str(exc)) from exc
    return Response(status_code=204)


@app.get("/api/v4/admin/performance/summary")
async def admin_perf_summary(
    request: Request,
    window: str = Query("7d"),
    device: str | None = Query(None),
    model: str | None = Query(None),
    variable: str | None = Query(None),
) -> dict[str, Any]:
    _require_admin_session(request)
    normalized_window = window.strip().lower()
    since_ts = int(time.time()) - _resolve_window_seconds(normalized_window)
    summary = admin_telemetry.get_perf_summary(
        since_ts=since_ts,
        device_type=_normalize_filter_value(device),
        model_id=_normalize_filter_value(model),
        variable_id=_normalize_filter_value(variable),
    )
    return {
        "window": normalized_window,
        "filters": {
            "device": _normalize_filter_value(device),
            "model": _normalize_filter_value(model),
            "variable": _normalize_filter_value(variable),
        },
        **summary,
    }


@app.get("/api/v4/admin/performance/timeseries")
async def admin_perf_timeseries(
    request: Request,
    metric: str = Query(...),
    window: str = Query("7d"),
    bucket: str = Query("auto"),
    device: str | None = Query(None),
    model: str | None = Query(None),
    variable: str | None = Query(None),
) -> dict[str, Any]:
    _require_admin_session(request)
    normalized_window = window.strip().lower()
    since_ts = int(time.time()) - _resolve_window_seconds(normalized_window)
    resolved_bucket = _resolve_bucket(normalized_window, bucket)
    try:
        points = admin_telemetry.get_perf_timeseries(
            since_ts=since_ts,
            metric=metric.strip(),
            bucket=resolved_bucket,
            device_type=_normalize_filter_value(device),
            model_id=_normalize_filter_value(model),
            variable_id=_normalize_filter_value(variable),
        )
    except ValueError as exc:
        raise TwfApiError(status_code=400, code="INVALID_PERF_QUERY", message=str(exc)) from exc
    return {
        "metric": metric.strip(),
        "window": normalized_window,
        "bucket": resolved_bucket,
        "filters": {
            "device": _normalize_filter_value(device),
            "model": _normalize_filter_value(model),
            "variable": _normalize_filter_value(variable),
        },
        "points": points,
    }


@app.get("/api/v4/admin/performance/breakdown")
async def admin_perf_breakdown(
    request: Request,
    metric: str = Query(...),
    by: str = Query("model"),
    window: str = Query("7d"),
    device: str | None = Query(None),
    model: str | None = Query(None),
    variable: str | None = Query(None),
    limit: int = Query(8, ge=1, le=20),
) -> dict[str, Any]:
    _require_admin_session(request)
    normalized_window = window.strip().lower()
    since_ts = int(time.time()) - _resolve_window_seconds(normalized_window)
    try:
        items = admin_telemetry.get_perf_breakdown(
            since_ts=since_ts,
            metric=metric.strip(),
            breakdown_by=by.strip().lower(),
            limit=limit,
            device_type=_normalize_filter_value(device),
            model_id=_normalize_filter_value(model),
            variable_id=_normalize_filter_value(variable),
        )
    except ValueError as exc:
        raise TwfApiError(status_code=400, code="INVALID_PERF_QUERY", message=str(exc)) from exc
    return {
        "metric": metric.strip(),
        "window": normalized_window,
        "by": by.strip().lower(),
        "filters": {
            "device": _normalize_filter_value(device),
            "model": _normalize_filter_value(model),
            "variable": _normalize_filter_value(variable),
        },
        "items": items,
    }


@app.get("/api/v4/admin/usage/summary")
async def admin_usage_summary(request: Request, window: str = Query("30d")) -> dict[str, Any]:
    _require_admin_session(request)
    normalized_window = window.strip().lower()
    since_ts = int(time.time()) - _resolve_window_seconds(normalized_window)
    return {
        "window": normalized_window,
        **admin_telemetry.get_usage_summary(since_ts=since_ts),
    }


@app.get("/api/v4/admin/status/results")
async def admin_status_results(
    request: Request,
    window: str = Query("30d"),
    model: str | None = Query(None),
    status: str | None = Query(None),
    limit: int = Query(200, ge=1, le=500),
) -> dict[str, Any]:
    _require_admin_session(request)
    normalized_window = window.strip().lower()
    since_ts = int(time.time()) - _resolve_window_seconds(normalized_window)
    return {
        "window": normalized_window,
        "filters": {
            "model": _normalize_filter_value(model),
            "status": _normalize_filter_value(status),
        },
        "results": admin_telemetry.get_operational_status_results(
            data_root=DATA_ROOT,
            since_ts=since_ts,
            model_id=_normalize_filter_value(model),
            status_filter=_normalize_filter_value(status),
            limit=limit,
        ),
    }


@app.post("/auth/twf/disconnect")
async def twf_disconnect(request: Request) -> JSONResponse:
    sid = request.cookies.get(twf_oauth.SESSION_COOKIE_NAME)

    resp = JSONResponse({"ok": True})
    if sid:
        twf_oauth.delete_session(sid)

    resp.delete_cookie(key=twf_oauth.SESSION_COOKIE_NAME, path="/")
    return resp


@app.get("/twf/forums")
async def twf_forums(request: Request) -> dict[str, Any]:
    sess = _require_twf_session(request)
    return await twf_oauth.list_forums(sess)


def _extract_topics(payload: dict[str, Any]) -> list[Any]:
    results = payload.get("results")
    if isinstance(results, list):
        return results
    topics = payload.get("topics")
    if isinstance(topics, list):
        return topics
    items = payload.get("items")
    if isinstance(items, list):
        return items
    return []


def _topic_forum_id(t: dict[str, Any]) -> int | None:
    """Best-effort extraction of a topic's forum id across IPS shapes."""
    v = t.get("forum")
    if isinstance(v, dict):
        fid = v.get("id")
        try:
            return int(fid) if fid is not None else None
        except Exception:
            return None
    if isinstance(v, (int, str)):
        try:
            return int(v)
        except Exception:
            return None
    v2 = t.get("forum_id")
    if isinstance(v2, (int, str)):
        try:
            return int(v2)
        except Exception:
            return None
    return None


def _is_truthy_topic_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"1", "true", "yes", "on"}
    return False


def _normalize_topic(raw_topic: Any, *, force_pinned: bool) -> dict[str, Any] | None:
    if not isinstance(raw_topic, dict):
        return None

    raw_id = raw_topic.get("id")
    if raw_id is None:
        return None
    try:
        topic_id = int(raw_id)
    except (TypeError, ValueError):
        return None
    if topic_id <= 0:
        return None

    raw_title = raw_topic.get("title")
    title = str(raw_title).strip() if raw_title is not None else ""
    raw_url = raw_topic.get("url")
    url = str(raw_url).strip() if raw_url is not None else ""
    if not title or not url:
        return None

    pinned = force_pinned or _is_truthy_topic_flag(raw_topic.get("pinned"))
    normalized: dict[str, Any] = {
        "id": topic_id,
        "title": title,
        "url": url,
        "pinned": pinned,
    }

    updated = raw_topic.get("updated")
    if updated is not None:
        normalized["updated"] = str(updated) if not isinstance(updated, str) else updated

    starter: str | None = None
    raw_starter = raw_topic.get("starter")
    if isinstance(raw_starter, dict):
        for key in ("name", "display_name", "displayName"):
            value = raw_starter.get(key)
            if isinstance(value, str) and value.strip():
                starter = value.strip()
                break
    if starter is None:
        raw_author = raw_topic.get("author")
        if isinstance(raw_author, dict):
            for key in ("name", "display_name", "displayName"):
                value = raw_author.get(key)
                if isinstance(value, str) and value.strip():
                    starter = value.strip()
                    break
    if starter is not None:
        normalized["starter"] = starter

    return normalized


def _topic_updated_sort_key(updated: Any) -> tuple[int, float, str]:
    if isinstance(updated, (int, float)):
        return (2, float(updated), "")
    if isinstance(updated, str):
        text = updated.strip()
        if not text:
            return (0, 0.0, "")
        iso_value = text.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(iso_value)
            return (2, parsed.timestamp(), "")
        except ValueError:
            pass
        try:
            return (2, float(text), "")
        except ValueError:
            return (1, 0.0, text.lower())
    return (0, 0.0, "")


@app.get("/twf/topics")
async def twf_topics(
    request: Request,
    forum_id: int = Query(..., ge=1),
    limit: int = Query(15, ge=1, le=25),
) -> dict[str, Any]:
    sess = _require_twf_session(request)

    pinned_payload = await twf_oauth.list_topics(sess, forum_id=forum_id, pinned=True, per_page=min(5, limit))
    regular_payload = await twf_oauth.list_topics(sess, forum_id=forum_id, pinned=False, per_page=limit)
    pinned_items = [item for item in _extract_topics(pinned_payload) if isinstance(item, dict)]
    unpinned_items = [item for item in _extract_topics(regular_payload) if isinstance(item, dict)]

    def _filter_forum(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for it in items:
            fid = _topic_forum_id(it)
            if fid is None:
                continue
            if fid == forum_id:
                out.append(it)
        return out

    pinned_items = _filter_forum(pinned_items)
    unpinned_items = _filter_forum(unpinned_items)
    logger.info(
        "TWF topics filtered",
        extra={
            "request_id": getattr(request.state, "request_id", None),
            "forum_id": forum_id,
            "pinned_count": len(pinned_items),
            "unpinned_count": len(unpinned_items),
        },
    )

    merged_by_id: dict[int, dict[str, Any]] = {}
    for raw_topic in pinned_items:
        normalized = _normalize_topic(raw_topic, force_pinned=True)
        if normalized is None:
            continue
        merged_by_id[normalized["id"]] = normalized

    for raw_topic in unpinned_items:
        normalized = _normalize_topic(raw_topic, force_pinned=False)
        if normalized is None:
            continue
        topic_id = normalized["id"]
        existing = merged_by_id.get(topic_id)
        if existing is None:
            merged_by_id[topic_id] = normalized
            continue
        if not existing.get("pinned", False) and normalized.get("pinned", False):
            merged_by_id[topic_id] = normalized
            continue
        if "updated" not in existing and "updated" in normalized:
            existing["updated"] = normalized["updated"]
        if "starter" not in existing and "starter" in normalized:
            existing["starter"] = normalized["starter"]

    results = list(merged_by_id.values())
    results.sort(
        key=lambda item: (
            1 if item.get("pinned") else 0,
            *_topic_updated_sort_key(item.get("updated")),
            int(item.get("id") or 0),
        ),
        reverse=True,
    )
    return {"forum_id": forum_id, "results": results}


class ShareTopicIn(BaseModel):
    forum_id: int = Field(..., ge=1)
    title: str = Field(..., min_length=1, max_length=255)
    content: str | None = Field(None, min_length=1, max_length=5000)
    summary: str | None = Field(None, min_length=1, max_length=5000)
    permalink: str | None = Field(None, min_length=1, max_length=4096)
    image_url: str | None = Field(None, min_length=1, max_length=4096)

    @model_validator(mode="after")
    def validate_share_payload(self) -> "ShareTopicIn":
        has_content = isinstance(self.content, str) and bool(self.content.strip())
        has_summary = isinstance(self.summary, str) and bool(self.summary.strip())
        has_permalink = isinstance(self.permalink, str) and bool(self.permalink.strip())
        has_image = isinstance(self.image_url, str) and bool(self.image_url.strip())
        if has_content or (has_summary and has_permalink):
            return self
        if has_summary or has_permalink or has_image:
            raise ValueError("Summary and permalink are required.")
        raise ValueError("Content is required.")


def _twf_share_body_from_request(
    *,
    content: str | None,
    summary: str | None,
    permalink: str | None,
    image_url: str | None,
) -> tuple[str, str]:
    summary_value = summary.strip() if isinstance(summary, str) else ""
    permalink_value = permalink.strip() if isinstance(permalink, str) else ""
    image_value = image_url.strip() if isinstance(image_url, str) else ""

    if summary_value or permalink_value or image_value:
        if not summary_value:
            raise TwfApiError(status_code=400, code="TWF_VALIDATION_ERROR", message="Summary is required.")
        if not permalink_value:
            raise TwfApiError(status_code=400, code="TWF_VALIDATION_ERROR", message="Permalink is required.")
        try:
            return (
                twf_oauth.build_twf_share_html(
                summary=summary_value,
                permalink=permalink_value,
                image_url=image_value or None,
                ),
                "html",
            )
        except ValueError as exc:
            raise TwfApiError(status_code=400, code="TWF_VALIDATION_ERROR", message=str(exc)) from exc

    content_value = content.strip() if isinstance(content, str) else ""
    if not content_value:
        raise TwfApiError(status_code=400, code="TWF_VALIDATION_ERROR", message="Content is required.")
    return content_value, "plain"


@app.post("/twf/share/topic")
async def twf_share_topic(request: Request, body: ShareTopicIn) -> dict[str, Any]:
    sess = _require_twf_session(request)
    title = body.title.strip()
    content, content_format = _twf_share_body_from_request(
        content=body.content,
        summary=body.summary,
        permalink=body.permalink,
        image_url=body.image_url,
    )
    if not title:
        raise TwfApiError(status_code=400, code="TWF_VALIDATION_ERROR", message="Title is required.")

    topic = await twf_oauth.create_topic(
        sess,
        forum_id=body.forum_id,
        title=title,
        content=content,
        content_format=content_format,
    )

    # IPS returns a big object; return only what the frontend actually needs.
    topic_id = topic.get("id")
    topic_url = topic.get("url")
    forum = topic.get("forum") or {}
    forum_id = forum.get("id") or body.forum_id

    if not topic_id or not topic_url:
        raise TwfApiError(
            status_code=502,
            code="IPS_UPSTREAM_ERROR",
            message="Forum API temporarily unavailable.",
        )

    return {
        "topicId": int(topic_id),
        "topicUrl": str(topic_url),
        "forumId": int(forum_id),
        "title": str(topic.get("title") or body.title),
    }


class SharePostIn(BaseModel):
    topic_id: int = Field(..., ge=1)
    content: str | None = Field(None, min_length=1, max_length=5000)
    summary: str | None = Field(None, min_length=1, max_length=5000)
    permalink: str | None = Field(None, min_length=1, max_length=4096)
    image_url: str | None = Field(None, min_length=1, max_length=4096)

    @model_validator(mode="after")
    def validate_share_payload(self) -> "SharePostIn":
        has_content = isinstance(self.content, str) and bool(self.content.strip())
        has_summary = isinstance(self.summary, str) and bool(self.summary.strip())
        has_permalink = isinstance(self.permalink, str) and bool(self.permalink.strip())
        has_image = isinstance(self.image_url, str) and bool(self.image_url.strip())
        if has_content or (has_summary and has_permalink):
            return self
        if has_summary or has_permalink or has_image:
            raise ValueError("Summary and permalink are required.")
        raise ValueError("Content is required.")


@app.post("/twf/share/post")
async def twf_share_post(request: Request, body: SharePostIn) -> dict[str, Any]:
    sess = _require_twf_session(request)
    content, content_format = _twf_share_body_from_request(
        content=body.content,
        summary=body.summary,
        permalink=body.permalink,
        image_url=body.image_url,
    )

    post = await twf_oauth.create_post(
        sess,
        topic_id=body.topic_id,
        content=content,
        content_format=content_format,
    )

    post_id = post.get("id")
    post_url = post.get("url")
    topic_id = post.get("topic", {}).get("id") if isinstance(post.get("topic"), dict) else post.get("topic")
    if not topic_id:
        topic_id = body.topic_id

    if not post_id or not post_url:
        raise TwfApiError(
            status_code=502,
            code="IPS_UPSTREAM_ERROR",
            message="Forum API temporarily unavailable.",
        )

    return {
        "postId": int(post_id),
        "postUrl": str(post_url),
        "topicId": int(topic_id),
    }


@app.post("/api/v4/share/media")
async def share_media_upload(
    file: UploadFile | None = File(None),
    model: str | None = Form(None),
    run: str | None = Form(None),
    fh: str | None = Form(None),
    variable: str | None = Form(None),
    region: str | None = Form(None),
) -> JSONResponse:
    if file is None:
        return _share_media_error_response(
            status_code=400,
            code="MISSING_FILE",
            message="A PNG file upload is required.",
        )

    content_type = (file.content_type or "").strip().lower()
    if content_type != share_media_service.PNG_CONTENT_TYPE:
        await file.close()
        return _share_media_error_response(
            status_code=400,
            code="INVALID_CONTENT_TYPE",
            message="Only PNG uploads are supported.",
        )

    data = await file.read()
    await file.close()

    if not data:
        return _share_media_error_response(
            status_code=400,
            code="EMPTY_FILE",
            message="Uploaded file is empty.",
        )

    if len(data) > share_media_service.MAX_SHARE_PNG_BYTES:
        return _share_media_error_response(
            status_code=413,
            code="FILE_TOO_LARGE",
            message="PNG upload exceeds the 10 MB limit.",
        )

    filename_hint = share_media_service.build_share_png_filename_hint(
        model=model,
        run=run,
        fh=fh,
        variable=variable,
        region=region,
    )

    try:
        result = share_media_service.upload_share_png(
            data=data,
            filename_hint=filename_hint,
            content_type=content_type,
        )
    except share_media_service.ShareMediaError as exc:
        return _share_media_error_response(
            status_code=exc.status_code,
            code=exc.code,
            message=exc.message,
        )

    return JSONResponse(content={"ok": True, **result})


class SampleBatchPointIn(BaseModel):
    id: str = Field(..., min_length=1, max_length=128)
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)


class SampleBatchIn(BaseModel):
    model: str = Field(..., min_length=1, max_length=64)
    run: str = Field(..., min_length=1, max_length=32)
    variable: str = Field(..., min_length=1, max_length=128)
    forecast_hour: int = Field(..., ge=0)
    points: list[SampleBatchPointIn] = Field(..., min_length=1, max_length=500)

_ds_cache: dict[str, rasterio.DatasetReader] = {}
_ds_cache_lock = threading.Lock()
_DS_CACHE_MAX = 16

_manifest_cache: dict[str, dict[str, Any]] = {}
_sidecar_cache: dict[str, dict[str, Any]] = {}
_json_cache_lock = threading.Lock()


class _SampleInflight:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.payload: dict[str, Any] | None = None


_sample_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_sample_inflight: dict[str, _SampleInflight] = {}
_sample_rate_window: dict[str, list[float]] = {}
_sample_lock = threading.Lock()

LOOP_MANIFEST_VERSION = 1
LOOP_MANIFEST_PROJECTION = "EPSG:4326"
LOOP_MANIFEST_BBOX = [-134.0, 24.0, -60.0, 55.0]


def _run_hour(run_id: str) -> int | None:
    match = _RUN_ID_RE.match(run_id)
    if not match:
        return None
    try:
        return int(run_id[9:11])
    except ValueError:
        return None


@lru_cache(maxsize=64)
def _model_allowed_cycle_hours(model: str) -> set[int]:
    model_id = model.strip().lower()
    capabilities = list_model_capabilities().get(model_id)
    run_discovery = getattr(capabilities, "run_discovery", {}) if capabilities is not None else {}

    explicit_hours = run_discovery.get("cycle_hours") if isinstance(run_discovery, dict) else None
    if isinstance(explicit_hours, (list, tuple, set)):
        resolved = {
            int(hour)
            for hour in explicit_hours
            if isinstance(hour, int) and 0 <= int(hour) <= 23
        }
        if resolved:
            return resolved

    cadence_raw = run_discovery.get("cycle_cadence_hours") if isinstance(run_discovery, dict) else 1
    try:
        cadence = max(1, int(cadence_raw if cadence_raw is not None else 1))
    except (TypeError, ValueError):
        cadence = 1
    return set(range(0, 24, cadence))


def _run_matches_model_cycle(model: str, run_id: str) -> bool:
    hour = _run_hour(run_id)
    if hour is None:
        return False
    return hour in _model_allowed_cycle_hours(model)


def _loop_webp_url(model: str, run: str, var: str, fh: int, *, tier: int, version_token: str) -> str:
    base = f"/api/v4/{model}/{run}/{var}/{fh}/loop.webp"
    return f"{base}?tier={tier}&v={version_token}"


def _static_loop_webp_url(model: str, run: str, var: str, fh: int, *, tier: int, version_token: str) -> str:
    base = LOOP_URL_PREFIX.rstrip("/")
    return f"{base}/{model}/{run}/{var}/tier{tier}/fh{fh:03d}.loop.webp?v={version_token}"


def _legacy_loop_webp_url(model: str, run: str, var: str, fh: int, *, version_token: str) -> str:
    return _loop_webp_url(model, run, var, fh, tier=0, version_token=version_token)


def _resolve_existing_loop_urls(
    model: str,
    run: str,
    var: str,
    fh: int,
    *,
    version_token: str,
) -> tuple[str | None, str | None]:
    tier0_url: str | None = None
    tier1_url: str | None = None
    var_norm = str(var or "").strip().lower()
    runtime_only = var_norm in RUNTIME_ONLY_LOOP_URL_VARS

    tier0_path = _loop_webp_path(model, run, var, fh, tier=0)
    if tier0_path is not None and tier0_path.is_file():
        if runtime_only:
            tier0_url = _loop_webp_url(model, run, var, fh, tier=0, version_token=version_token)
        else:
            tier0_url = _static_loop_webp_url(model, run, var, fh, tier=0, version_token=version_token)
    else:
        legacy_path = _legacy_loop_webp_path(model, run, var, fh, tier=0)
        if legacy_path is not None and legacy_path.is_file():
            tier0_url = _legacy_loop_webp_url(model, run, var, fh, version_token=version_token)

    tier1_path = _loop_webp_path(model, run, var, fh, tier=1)
    if tier1_path is not None and tier1_path.is_file():
        if runtime_only:
            tier1_url = _loop_webp_url(model, run, var, fh, tier=1, version_token=version_token)
        else:
            tier1_url = _static_loop_webp_url(model, run, var, fh, tier=1, version_token=version_token)

    return tier0_url, tier1_url


def _resolve_loop_urls_for_frame(
    model: str,
    run: str,
    var: str,
    fh: int,
    *,
    version_token: str,
    include_tier0_runtime_fallback: bool = False,
    include_tier1_runtime_fallback: bool = False,
) -> tuple[str | None, str | None]:
    tier0_url, tier1_url = _resolve_existing_loop_urls(
        model,
        run,
        var,
        fh,
        version_token=version_token,
    )

    if tier0_url is None and include_tier0_runtime_fallback:
        tier0_url = _loop_webp_url(model, run, var, fh, tier=0, version_token=version_token)
    if tier1_url is None and include_tier1_runtime_fallback:
        tier1_url = _loop_webp_url(model, run, var, fh, tier=1, version_token=version_token)
    return tier0_url, tier1_url


def _load_json_cached(path: Path, cache: dict[str, dict[str, Any]]) -> dict | None:
    key = str(path)
    now = time.monotonic()

    with _json_cache_lock:
        entry = cache.get(key)
        if entry is not None:
            last_checked = float(entry.get("last_checked", 0.0))
            if now - last_checked < _JSON_CACHE_RECHECK_SECONDS:
                payload = entry.get("payload")
                return payload if isinstance(payload, dict) else None

    try:
        stat = path.stat()
        mtime_ns = int(stat.st_mtime_ns)
    except OSError:
        with _json_cache_lock:
            cache.pop(key, None)
        return None

    with _json_cache_lock:
        entry = cache.get(key)
        if entry is not None and int(entry.get("mtime_ns", -1)) == mtime_ns:
            entry["last_checked"] = now
            payload = entry.get("payload")
            return payload if isinstance(payload, dict) else None

    try:
        payload = json.loads(path.read_text())
    except Exception:
        logger.warning("Failed to read JSON cache file %s; serving last-good payload if available", path)
        with _json_cache_lock:
            entry = cache.get(key)
            if entry is not None:
                entry["last_checked"] = now
                cached_payload = entry.get("payload")
                return cached_payload if isinstance(cached_payload, dict) else None
        return None

    if not isinstance(payload, dict):
        return None

    with _json_cache_lock:
        cache[key] = {
            "mtime_ns": mtime_ns,
            "last_checked": now,
            "payload": payload,
        }
    return payload


def _get_cached_dataset(path: Path) -> rasterio.DatasetReader:
    key = str(path)
    with _ds_cache_lock:
        ds = _ds_cache.get(key)
        if ds is not None and not ds.closed:
            return ds
        if len(_ds_cache) >= _DS_CACHE_MAX:
            evict_key = next(iter(_ds_cache))
            try:
                _ds_cache.pop(evict_key).close()
            except Exception:
                _ds_cache.pop(evict_key, None)
        ds = rasterio.open(path)
        _ds_cache[key] = ds
        return ds


def _latest_run_from_pointer(model: str) -> str | None:
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
    if not _run_matches_model_cycle(model, run_id):
        logger.warning("LATEST.json points to out-of-cycle run for %s: %s", model, run_id)
        return None

    run_dir = PUBLISHED_ROOT / model / run_id
    manifest_path = MANIFESTS_ROOT / model / f"{run_id}.json"
    if not run_dir.is_dir() or not manifest_path.is_file():
        logger.warning("LATEST.json points to incomplete run state for %s/%s", model, run_id)
        return None
    return run_id


def _scan_manifest_runs(model: str) -> list[str]:
    model_manifest_dir = MANIFESTS_ROOT / model
    if not model_manifest_dir.is_dir():
        return []
    runs: list[str] = []
    for file_path in model_manifest_dir.glob("*.json"):
        run_id = file_path.stem
        if not _RUN_ID_RE.match(run_id):
            continue
        if not _run_matches_model_cycle(model, run_id):
            continue
        if not (PUBLISHED_ROOT / model / run_id).is_dir():
            continue
        runs.append(run_id)
    return sorted(set(runs), reverse=True)


def _serialize_variable_capability(model_id: str, capability: Any) -> dict[str, Any]:
    constraints = getattr(capability, "constraints", None)
    constraints_payload = dict(constraints) if isinstance(constraints, dict) else {}
    var_key = str(getattr(capability, "var_key", ""))
    return {
        "var_key": var_key,
        "display_name": str(getattr(capability, "name", "")),
        "kind": getattr(capability, "kind", None),
        "units": getattr(capability, "units", None),
        "order": getattr(capability, "order", None),
        "group": getattr(capability, "group", None),
        "default_fh": getattr(capability, "default_fh", None),
        "buildable": bool(getattr(capability, "buildable", False)),
        "color_map_id": getattr(capability, "color_map_id", None),
        "display_resampling_override": display_resampling_override(model_id, var_key),
        "constraints": constraints_payload,
        "derived": bool(getattr(capability, "derived", False)),
        "derive_strategy_id": getattr(capability, "derive_strategy_id", None),
    }


def _serialize_model_capability(model_id: str, capability: Any) -> dict[str, Any]:
    variable_catalog = getattr(capability, "variable_catalog", {}) or {}
    ordered_items = sorted(
        variable_catalog.items(),
        key=lambda item: (
            getattr(item[1], "order", None) is None,
            getattr(item[1], "order", 0) if getattr(item[1], "order", None) is not None else 0,
            item[0],
        ),
    )
    variables_payload = {
        var_key: _serialize_variable_capability(model_id, var_capability)
        for var_key, var_capability in ordered_items
    }

    defaults = getattr(capability, "ui_defaults", None)
    constraints = getattr(capability, "ui_constraints", None)
    run_discovery = getattr(capability, "run_discovery", None)
    return {
        "model_id": model_id,
        "name": str(getattr(capability, "name", model_id.upper())),
        "product": getattr(capability, "product", None),
        "canonical_region": getattr(capability, "canonical_region", None),
        "defaults": dict(defaults) if isinstance(defaults, dict) else {},
        "constraints": dict(constraints) if isinstance(constraints, dict) else {},
        "run_discovery": dict(run_discovery) if isinstance(run_discovery, dict) else {},
        "variables": variables_payload,
    }


def _manifest_var_available_frames(var_entry: dict[str, Any]) -> int:
    available_raw = var_entry.get("available_frames")
    if isinstance(available_raw, int):
        return max(0, available_raw)
    frames = var_entry.get("frames")
    if isinstance(frames, list):
        return len(frames)
    return 0


def _latest_run_readiness(
    model_id: str,
    latest_run: str | None,
    *,
    model_capability: Any | None,
) -> tuple[bool, list[str], int]:
    if latest_run is None:
        return False, [], 0

    manifest = _load_manifest(model_id, latest_run)
    if not isinstance(manifest, dict):
        return False, [], 0

    variables = manifest.get("variables")
    if not isinstance(variables, dict):
        return False, [], 0

    variable_catalog = getattr(model_capability, "variable_catalog", {}) if model_capability is not None else {}
    catalog_present = isinstance(variable_catalog, dict) and bool(variable_catalog)
    buildable_keys: set[str] = set()
    if catalog_present:
        buildable_keys = {
            str(var_key)
            for var_key, capability in variable_catalog.items()
            if bool(getattr(capability, "buildable", False))
        }

    ready_vars: list[str] = []
    ready_frame_count = 0
    for var_key, var_entry in variables.items():
        if not isinstance(var_entry, dict):
            continue
        if catalog_present and var_key not in buildable_keys:
            continue
        available_frames = _manifest_var_available_frames(var_entry)
        if available_frames <= 0:
            continue
        ready_vars.append(var_key)
        ready_frame_count += available_frames

    ready_vars.sort()
    return bool(ready_vars), ready_vars, ready_frame_count


def _availability_for_models(
    model_ids: list[str],
    capabilities_by_model: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    availability: dict[str, dict[str, Any]] = {}
    for model_id in model_ids:
        published_runs = _scan_manifest_runs(model_id)
        latest_run = _resolve_latest_run(model_id)
        latest_run_ready, latest_run_ready_vars, latest_run_ready_frame_count = _latest_run_readiness(
            model_id,
            latest_run,
            model_capability=capabilities_by_model.get(model_id),
        )
        availability[model_id] = {
            "latest_run": latest_run,
            "published_runs": published_runs,
            "latest_run_ready": latest_run_ready,
            "latest_run_ready_vars": latest_run_ready_vars,
            "latest_run_ready_frame_count": latest_run_ready_frame_count,
        }
    return availability


def _build_capabilities_payload() -> dict[str, Any]:
    capabilities_by_model = list_model_capabilities()
    model_catalog = {
        model_id: _serialize_model_capability(model_id, capability)
        for model_id, capability in sorted(capabilities_by_model.items(), key=lambda item: item[0])
    }
    supported_models = sorted(model_catalog.keys())
    availability = _availability_for_models(supported_models, capabilities_by_model)
    return {
        "contract_version": CAPABILITIES_CONTRACT_VERSION,
        "supported_models": supported_models,
        "model_catalog": model_catalog,
        "availability": availability,
    }


def _ordered_manifest_var_keys(model: str, manifest_vars: dict[str, Any]) -> list[str]:
    if not manifest_vars:
        return []
    capability_map = list_model_capabilities().get(model)
    if capability_map is None:
        return sorted(manifest_vars.keys())

    variable_catalog = getattr(capability_map, "variable_catalog", {}) or {}
    known: list[str] = []
    unknown: list[str] = []
    for var_key in manifest_vars.keys():
        if var_key in variable_catalog:
            known.append(var_key)
        else:
            unknown.append(var_key)

    known.sort(
        key=lambda key: (
            getattr(variable_catalog[key], "order", None) is None,
            getattr(variable_catalog[key], "order", 0)
            if getattr(variable_catalog[key], "order", None) is not None
            else 0,
            key,
        )
    )
    unknown.sort()
    return known + unknown


def _resolve_latest_run(model: str) -> str | None:
    pointed = _latest_run_from_pointer(model)
    if pointed is not None:
        return pointed
    runs = _scan_manifest_runs(model)
    return runs[0] if runs else None


def _resolve_run(model: str, run: str) -> str | None:
    if run == "latest":
        return _resolve_latest_run(model)
    if not _RUN_ID_RE.match(run):
        return None
    if not _run_matches_model_cycle(model, run):
        return None
    run_dir = PUBLISHED_ROOT / model / run
    manifest_path = MANIFESTS_ROOT / model / f"{run}.json"
    if run_dir.is_dir() and manifest_path.is_file():
        return run
    return None


def _manifest_path(model: str, run: str) -> Path:
    return MANIFESTS_ROOT / model / f"{run}.json"


def _load_manifest(model: str, run: str) -> dict | None:
    path = _manifest_path(model, run)
    if not path.is_file():
        return None
    return _load_json_cached(path, _manifest_cache)


def _manifest_run_complete(manifest: dict[str, Any]) -> bool:
    variables = manifest.get("variables")
    if not isinstance(variables, dict) or not variables:
        return False

    saw_expected = False
    for var_entry in variables.values():
        if not isinstance(var_entry, dict):
            return False

        expected_raw = var_entry.get("expected_frames")
        available_raw = var_entry.get("available_frames")
        expected = int(expected_raw) if isinstance(expected_raw, int) else None
        available = int(available_raw) if isinstance(available_raw, int) else None

        if expected is None:
            frames = var_entry.get("frames")
            if isinstance(frames, list):
                expected = len(frames)
                available = len(frames)
            else:
                return False

        if available is None:
            frames = var_entry.get("frames")
            if isinstance(frames, list):
                available = len(frames)
            else:
                return False

        saw_expected = saw_expected or expected > 0
        if available < expected:
            return False

    return saw_expected


def _run_version_token(model: str, run: str) -> str:
    path = _manifest_path(model, run)
    try:
        mtime_ns = int(path.stat().st_mtime_ns)
    except OSError:
        mtime_ns = 0
    return f"{run}-{mtime_ns}"


def _published_var_dir(model: str, run: str, var: str) -> Path:
    return PUBLISHED_ROOT / model / run / var


def _resolve_val_cog(model: str, run: str, var: str, fh: int) -> Path | None:
    resolved = _resolve_run(model, run) or run
    candidate = _published_var_dir(model, resolved, var) / f"fh{fh:03d}.val.cog.tif"
    if candidate.is_file():
        return candidate
    return None


def _resolve_sidecar(model: str, run: str, var: str, fh: int) -> dict | None:
    resolved = _resolve_run(model, run) or run
    candidate = _published_var_dir(model, resolved, var) / f"fh{fh:03d}.json"
    if candidate.is_file():
        return _load_json_cached(candidate, _sidecar_cache)
    return None


def _resolve_frame_var_dir(model: str, run: str, var: str, fh: int) -> Path | None:
    resolved = _resolve_run(model, run)
    if resolved is None:
        return None
    var_dir = _published_var_dir(model, resolved, var)
    if not var_dir.is_dir():
        return None
    if not (var_dir / f"fh{fh:03d}.rgba.cog.tif").is_file():
        return None
    return var_dir


def _resolve_rgba_cog(model: str, run: str, var: str, fh: int) -> Path | None:
    resolved = _resolve_run(model, run)
    if resolved is None:
        return None
    candidate = _published_var_dir(model, resolved, var) / f"fh{fh:03d}.rgba.cog.tif"
    if candidate.is_file():
        return candidate
    return None


def _loop_webp_path(model: str, run: str, var: str, fh: int, *, tier: int) -> Path | None:
    resolved = _resolve_run(model, run)
    if resolved is None:
        return None
    return LOOP_CACHE_ROOT / model / resolved / var / f"tier{tier}" / f"fh{fh:03d}.loop.webp"


def _legacy_loop_webp_path(model: str, run: str, var: str, fh: int, *, tier: int) -> Path | None:
    if tier != 0:
        return None
    resolved = _resolve_run(model, run)
    if resolved is None:
        return None
    candidate = _published_var_dir(model, resolved, var) / f"fh{fh:03d}.loop.webp"
    if candidate.is_file():
        return candidate
    return None


def _maybe_blur_loop_values(values: np.ndarray, *, sigma: float | None = None) -> np.ndarray:
    # Optional hook for value-rendered loop frames. Disabled by default.
    if sigma is None:
        return values
    try:
        if float(sigma) <= 0.0:
            return values
    except (TypeError, ValueError):
        return values
    return values


def _should_sharpen_loop(model: str, kind: str | None) -> bool:
    model_norm = str(model or "").strip().lower()
    kind_norm = str(kind or "").strip().lower()
    return model_norm == "gfs" and kind_norm == "continuous"


def _maybe_unsharp_rgba(
    rgba: np.ndarray,
    *,
    enable: bool,
    radius: float = 1.2,
    percent: int = 35,
    threshold: int = 3,
) -> np.ndarray:
    """Apply subtle unsharp mask to RGB channels while preserving alpha."""
    if not enable:
        return rgba

    rgba_u8 = np.asarray(rgba, dtype=np.uint8)
    im = Image.fromarray(rgba_u8, mode="RGBA")
    r, g, b, a = im.split()
    rgb = Image.merge("RGB", (r, g, b))
    rgb_sharp = rgb.filter(
        ImageFilter.UnsharpMask(
            radius=float(radius),
            percent=int(percent),
            threshold=int(threshold),
        )
    )
    out = Image.merge("RGBA", (*rgb_sharp.split(), a))
    return np.asarray(out, dtype=np.uint8)


def _apply_loop_sharpen_if_needed(*, rgba: np.ndarray, model_id: str, var_key: str) -> np.ndarray:
    kind = variable_kind(model_id, var_key)
    enable = LOOP_SHARPEN_ENABLE and _should_sharpen_loop(model_id, kind)
    return _maybe_unsharp_rgba(
        rgba,
        enable=enable,
        radius=LOOP_SHARPEN_RADIUS,
        percent=LOOP_SHARPEN_PERCENT,
        threshold=LOOP_SHARPEN_THRESHOLD,
    )


def _render_loop_rgba_hwc(
    *,
    cog_path: Path,
    value_cog_path: Path | None,
    model_id: str,
    var_key: str,
    out_h: int,
    out_w: int,
    blur_sigma: float | None = None,
    prefer_high_quality_resize: bool = False,
) -> np.ndarray | None:
    base_resampling = rasterio_resampling_for_loop(model_id=model_id, var_key=var_key)
    if prefer_high_quality_resize and base_resampling != Resampling.nearest:
        render_resampling = high_quality_loop_resampling()
    else:
        render_resampling = base_resampling

    use_value_render = use_value_render_for_variable(model_id=model_id, var_key=var_key)
    if use_value_render and value_cog_path is not None and value_cog_path.is_file():
        color_map_id = variable_color_map_id(model_id, var_key)
        if color_map_id:
            try:
                with rasterio.open(value_cog_path) as value_ds:
                    sampled_values = value_ds.read(
                        1,
                        out_shape=(out_h, out_w),
                        resampling=render_resampling,
                    ).astype(np.float32, copy=False)
                sampled_values = _maybe_blur_loop_values(sampled_values, sigma=blur_sigma)
                rgba, _ = float_to_rgba(
                    sampled_values,
                    color_map_id,
                    meta_var_key=var_key,
                )
                return np.moveaxis(rgba, 0, -1)
            except Exception:
                logger.exception(
                    "Loop value-render failed; falling back to RGBA path: model=%s var=%s src=%s val=%s",
                    model_id,
                    var_key,
                    cog_path,
                    value_cog_path,
                )
        else:
            logger.warning(
                "Loop value-render color_map_id missing; falling back to RGBA path: model=%s var=%s",
                model_id,
                var_key,
            )

    with rasterio.open(cog_path) as ds:
        if render_resampling == Resampling.nearest:
            data = ds.read(
                indexes=(1, 2, 3, 4),
                out_shape=(4, out_h, out_w),
                resampling=render_resampling,
            )
        else:
            rgb = ds.read(
                indexes=(1, 2, 3),
                out_shape=(3, out_h, out_w),
                resampling=render_resampling,
            )
            alpha = ds.read(
                indexes=4,
                out_shape=(out_h, out_w),
                resampling=Resampling.nearest,
            )
            data = np.concatenate((rgb, alpha[np.newaxis, :, :]), axis=0)
    return np.moveaxis(data, 0, -1)


def _ensure_loop_webp(
    cog_path: Path,
    out_path: Path,
    *,
    model_id: str,
    var_key: str,
    tier: int,
    value_cog_path: Path | None = None,
    run_id: str | None = None,
) -> bool:
    if out_path.is_file():
        return True

    tier_cfg = LOOP_TIER_CONFIG.get(tier)
    if tier_cfg is None:
        return False

    max_dim_cfg = loop_max_dim_for_tier(
        model_id=model_id,
        var_key=var_key,
        tier=tier,
        default_max_dim=int(tier_cfg.get("max_dim", LOOP_WEBP_MAX_DIM)),
    )
    fixed_w_cfg = loop_fixed_width_for_tier(
        model_id=model_id,
        var_key=var_key,
        tier=tier,
        default_width=int(tier_cfg.get("fixed_w", max_dim_cfg)),
    )
    quality_cfg = loop_quality_for_tier(
        model_id=model_id,
        var_key=var_key,
        tier=tier,
        default_quality=int(tier_cfg.get("quality", LOOP_WEBP_QUALITY)),
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix=".webp", delete=False, dir=str(out_path.parent)) as tmp:
        tmp_path = Path(tmp.name)

    try:
        with rasterio.open(cog_path) as ds:
            src_h = int(ds.height)
            src_w = int(ds.width)
            out_h, out_w, fixed_applied = compute_loop_output_shape(
                model_id=model_id,
                var_key=var_key,
                src_h=src_h,
                src_w=src_w,
                max_dim=max_dim_cfg,
                fixed_width=fixed_w_cfg,
            )
            if out_h <= 0 or out_w <= 0:
                return False
                if fixed_applied:
                    log_fixed_loop_size_once(
                        model_id=model_id,
                        run_id=run_id,
                        var_key=var_key,
                    tier=tier,
                    src_h=src_h,
                    src_w=src_w,
                        out_h=out_h,
                        out_w=out_w,
                    )
            value_render_active = use_value_render_for_variable(model_id=model_id, var_key=var_key)
            prefer_high_quality_resize = fixed_applied or (
                value_render_active and (out_h < src_h or out_w < src_w)
            )

        rgba = _render_loop_rgba_hwc(
            cog_path=cog_path,
            value_cog_path=value_cog_path,
            model_id=model_id,
            var_key=var_key,
            out_h=out_h,
            out_w=out_w,
            blur_sigma=None,
            prefer_high_quality_resize=prefer_high_quality_resize,
        )
        if rgba is None:
            return False
        rgba = _apply_loop_sharpen_if_needed(rgba=rgba, model_id=model_id, var_key=var_key)
        image = Image.fromarray(rgba, mode="RGBA")
        image.save(tmp_path, format="WEBP", quality=quality_cfg, method=6)
        tmp_path.replace(out_path)
        return True
    except Exception:
        logger.exception("Failed generating loop WebP: %s -> %s", cog_path, out_path)
        try:
            if tmp_path.is_file():
                tmp_path.unlink()
        except Exception:
            pass
        return False


def _render_loop_webp_bytes(
    cog_path: Path,
    *,
    model_id: str,
    var_key: str,
    tier: int,
    value_cog_path: Path | None = None,
    run_id: str | None = None,
) -> bytes | None:
    tier_cfg = LOOP_TIER_CONFIG.get(tier)
    if tier_cfg is None:
        return None

    max_dim_cfg = loop_max_dim_for_tier(
        model_id=model_id,
        var_key=var_key,
        tier=tier,
        default_max_dim=int(tier_cfg.get("max_dim", LOOP_WEBP_MAX_DIM)),
    )
    fixed_w_cfg = loop_fixed_width_for_tier(
        model_id=model_id,
        var_key=var_key,
        tier=tier,
        default_width=int(tier_cfg.get("fixed_w", max_dim_cfg)),
    )
    quality_cfg = loop_quality_for_tier(
        model_id=model_id,
        var_key=var_key,
        tier=tier,
        default_quality=int(tier_cfg.get("quality", LOOP_WEBP_QUALITY)),
    )

    try:
        with rasterio.open(cog_path) as ds:
            src_h = int(ds.height)
            src_w = int(ds.width)
            out_h, out_w, fixed_applied = compute_loop_output_shape(
                model_id=model_id,
                var_key=var_key,
                src_h=src_h,
                src_w=src_w,
                max_dim=max_dim_cfg,
                fixed_width=fixed_w_cfg,
            )
            if out_h <= 0 or out_w <= 0:
                return None
                if fixed_applied:
                    log_fixed_loop_size_once(
                        model_id=model_id,
                        run_id=run_id,
                        var_key=var_key,
                    tier=tier,
                    src_h=src_h,
                    src_w=src_w,
                        out_h=out_h,
                        out_w=out_w,
                    )
            value_render_active = use_value_render_for_variable(model_id=model_id, var_key=var_key)
            prefer_high_quality_resize = fixed_applied or (
                value_render_active and (out_h < src_h or out_w < src_w)
            )

        rgba = _render_loop_rgba_hwc(
            cog_path=cog_path,
            value_cog_path=value_cog_path,
            model_id=model_id,
            var_key=var_key,
            out_h=out_h,
            out_w=out_w,
            blur_sigma=None,
            prefer_high_quality_resize=prefer_high_quality_resize,
        )
        if rgba is None:
            return None
        rgba = _apply_loop_sharpen_if_needed(rgba=rgba, model_id=model_id, var_key=var_key)
        image = Image.fromarray(rgba, mode="RGBA")
        buffer = io.BytesIO()
        image.save(buffer, format="WEBP", quality=quality_cfg, method=6)
        return buffer.getvalue()
    except Exception:
        logger.exception("Failed in-memory loop WebP generation: %s (tier=%s)", cog_path, tier)
        return None


def _sample_cache_key(model: str, run: str, var: str, fh: int, row: int, col: int) -> str:
    return f"{model}:{run}:{var}:{fh}:{row}:{col}"


def _sample_batch_cache_key(model: str, run: str, var: str, fh: int, points_hash: str) -> str:
    return f"batch:{model}:{run}:{var}:{fh}:{points_hash}"


def _sample_points_hash(points: list[SampleBatchPointIn]) -> str:
    canonical_points = [
        {
            "id": point.id,
            "lat": float(point.lat),
            "lon": float(point.lon),
        }
        for point in sorted(points, key=lambda point: point.id)
    ]
    return hashlib.md5(
        json.dumps(canonical_points, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


@lru_cache(maxsize=16)
def _sample_transformer(dst_crs: str) -> Transformer:
    return Transformer.from_crs("EPSG:4326", dst_crs, always_xy=True)


def _sample_dataset_xy(ds: rasterio.DatasetReader, *, lon: float, lat: float) -> tuple[float, float]:
    ds_crs = ds.crs
    if ds_crs is None:
        raise ValueError(f"Sample dataset missing CRS: {ds.name}")
    dst_crs = ds_crs.to_string()
    if dst_crs == "EPSG:4326":
        return float(lon), float(lat)
    return _sample_transformer(dst_crs).transform(lon, lat)


def _sample_dataset_index(ds: rasterio.DatasetReader, *, lon: float, lat: float) -> tuple[int, int]:
    x, y = _sample_dataset_xy(ds, lon=lon, lat=lat)
    row, col = ds.index(x, y)
    return row, col


def _read_sample_value(
    ds: rasterio.DatasetReader,
    *,
    row: int,
    col: int,
    masked: bool,
) -> tuple[float | None, bool]:
    if row < 0 or row >= ds.height or col < 0 or col >= ds.width:
        return None, True

    window = Window(col, row, 1, 1)  # type: ignore[call-arg]
    pixel = ds.read(1, window=window, masked=masked)
    raw_value = pixel[0, 0]
    if np.ma.is_masked(raw_value):
        return None, True

    value = float(raw_value)
    if np.isnan(value):
        return None, True
    return value, False


def _sample_batch_values(
    ds: rasterio.DatasetReader,
    *,
    points: list[SampleBatchPointIn],
) -> dict[str, float | None]:
    values: dict[str, float | None] = {}
    for point in points:
        row, col = _sample_dataset_index(ds, lon=point.lon, lat=point.lat)
        value, no_data = _read_sample_value(ds, row=row, col=col, masked=True)
        values[point.id] = None if no_data or value is None else round(float(value), 1)
    return values


def _sample_rate_limit_allow(client_id: str) -> tuple[bool, float]:
    if SAMPLE_RATE_LIMIT_MAX_REQUESTS <= 0:
        return True, 0.0

    now = time.monotonic()
    cutoff = now - max(0.01, SAMPLE_RATE_LIMIT_WINDOW_SECONDS)
    retry_after = max(1.0, SAMPLE_RATE_LIMIT_WINDOW_SECONDS)

    with _sample_lock:
        window = _sample_rate_window.get(client_id)
        if window is None:
            window = []
            _sample_rate_window[client_id] = window
        while window and window[0] < cutoff:
            window.pop(0)
        if len(window) >= SAMPLE_RATE_LIMIT_MAX_REQUESTS:
            return False, retry_after
        window.append(now)

    return True, 0.0


def _sample_payload(
    *,
    model: str,
    run: str,
    var: str,
    fh: int,
    lat: float,
    lon: float,
    value: float | None,
    units: str,
    valid_time: str,
    no_data: bool,
) -> dict[str, Any]:
    return {
        "value": round(float(value), 1) if value is not None else None,
        "units": units,
        "model": model,
        "run": run,
        "var": var,
        "fh": fh,
        "valid_time": valid_time,
        "lat": lat,
        "lon": lon,
        "noData": no_data,
    }


@app.get("/api/v4/health")
def health_v4():
    return {"ok": True, "data_root": str(DATA_ROOT)}


@app.get("/api/v4")
def root_v4():
    return {"service": "twf-v4-api", "version": "4.0.0", "capabilities_contract": CAPABILITIES_CONTRACT_VERSION}


@app.get("/api/regions")
def list_region_presets(request: Request):
    payload = {"regions": REGION_PRESETS}
    cache_control = "public, max-age=300"
    etag = _make_etag(payload)
    r304 = _maybe_304(request, etag=etag, cache_control=cache_control)
    if r304 is not None:
        return r304
    return JSONResponse(
        content=payload,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
        },
    )


@app.get("/api/v4/models")
def list_models_v4(request: Request):
    capabilities_payload = _build_capabilities_payload()
    supported_models = capabilities_payload["supported_models"]
    model_catalog = capabilities_payload["model_catalog"]
    availability = capabilities_payload["availability"]
    payload = [
        {
            "id": model_id,
            "name": model_catalog.get(model_id, {}).get("name", model_id.upper()),
            "latest_run": availability.get(model_id, {}).get("latest_run"),
            "published_runs": availability.get(model_id, {}).get("published_runs", []),
        }
        for model_id in supported_models
    ]
    cache_control = "public, max-age=60"
    etag = _make_etag(payload)
    r304 = _maybe_304(request, etag=etag, cache_control=cache_control)
    if r304 is not None:
        return r304
    return JSONResponse(
        content=payload,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
        },
    )


@app.get("/api/v4/capabilities")
def get_capabilities_v4(request: Request):
    payload = _build_capabilities_payload()
    cache_control = "public, max-age=60"
    etag = _make_etag(payload)
    r304 = _maybe_304(request, etag=etag, cache_control=cache_control)
    if r304 is not None:
        return r304
    return JSONResponse(
        content=payload,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
        },
    )


@app.get("/api/v4/models/{model}/capabilities")
def get_model_capabilities_v4(request: Request, model: str):
    model_id = model.strip().lower()
    payload = _build_capabilities_payload()
    model_catalog = payload["model_catalog"]
    if model_id not in model_catalog:
        raise HTTPException(status_code=404, detail=f"Unknown model: {model_id}")

    model_payload = {
        "contract_version": payload["contract_version"],
        "model_id": model_id,
        "capabilities": model_catalog[model_id],
        "availability": payload["availability"].get(
            model_id,
            {"latest_run": None, "published_runs": []},
        ),
    }
    cache_control = "public, max-age=60"
    etag = _make_etag(model_payload)
    r304 = _maybe_304(request, etag=etag, cache_control=cache_control)
    if r304 is not None:
        return r304
    return JSONResponse(
        content=model_payload,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
        },
    )


@app.get("/api/v4/{model}/runs")
def list_runs(request: Request, model: str):
    runs = _scan_manifest_runs(model)
    cache_control = "public, max-age=60"
    etag = _make_etag(runs)
    r304 = _maybe_304(request, etag=etag, cache_control=cache_control)
    if r304 is not None:
        return r304
    return JSONResponse(
        content=runs,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
        },
    )


@app.get("/api/v4/{model}/{run}/manifest")
def get_manifest(request: Request, model: str, run: str):
    resolved = _resolve_run(model, run)
    if resolved is None:
        return Response(status_code=404, content='{"error": "run not found"}', media_type="application/json")
    manifest = _load_manifest(model, resolved)
    if manifest is None:
        return Response(status_code=404, content='{"error": "manifest not found"}', media_type="application/json")

    cache_control = "public, max-age=60"
    etag = _make_etag(manifest)
    r304 = _maybe_304(request, etag=etag, cache_control=cache_control)
    if r304 is not None:
        return r304
    return JSONResponse(
        content=manifest,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
        },
    )


@app.get("/api/v4/{model}/{run}/vars")
def list_vars(model: str, run: str):
    model_id = model.strip().lower()
    resolved = _resolve_run(model_id, run)
    if resolved is None:
        return Response(status_code=404, content='{"error": "run not found"}', media_type="application/json")

    manifest = _load_manifest(model_id, resolved)
    if manifest is None:
        return Response(status_code=404, content='{"error": "manifest not found"}', media_type="application/json")

    variables = manifest.get("variables")
    if not isinstance(variables, dict):
        return []

    ordered_var_ids = _ordered_manifest_var_keys(model_id, variables)
    model_capability = list_model_capabilities().get(model_id)
    variable_catalog = getattr(model_capability, "variable_catalog", {}) if model_capability is not None else {}

    result = []
    for var_id in ordered_var_ids:
        capability = variable_catalog.get(var_id) if isinstance(variable_catalog, dict) else None
        display_name = getattr(capability, "name", None) if capability is not None else None
        result.append({"id": var_id, "display_name": display_name or var_id})
    return result


@app.get("/api/v4/{model}/{run}/{var}/frames")
def list_frames(request: Request, model: str, run: str, var: str):
    resolved = _resolve_run(model, run)
    if resolved is None:
        return Response(status_code=404, content='{"error": "run not found"}', media_type="application/json")

    manifest = _load_manifest(model, resolved)
    if manifest is None:
        return Response(status_code=404, content='{"error": "manifest not found"}', media_type="application/json")

    variables = manifest.get("variables")
    if not isinstance(variables, dict):
        return []
    var_entry = variables.get(var)
    if not isinstance(var_entry, dict):
        return []

    frame_entries = var_entry.get("frames")
    if not isinstance(frame_entries, list):
        frame_entries = []

    run_complete = _manifest_run_complete(manifest)

    version_token = _run_version_token(model, resolved)

    frames: list[dict] = []
    for item in frame_entries:
        if not isinstance(item, dict):
            continue
        fh = item.get("fh")
        if not isinstance(fh, int):
            continue

        tier0_url, tier1_url = _resolve_loop_urls_for_frame(
            model,
            resolved,
            var,
            fh,
            version_token=version_token,
            include_tier0_runtime_fallback=True,
        )

        meta = _resolve_sidecar(model, resolved, var, fh)
        frames.append(
            {
                "fh": fh,
                "has_cog": True,
                "run": resolved,
                "loop_webp_url": tier0_url,
                "loop_webp_tier0_url": tier0_url,
                "loop_webp_tier1_url": tier1_url,
                "meta": {"meta": meta},
            }
        )

    frames.sort(key=lambda row: row["fh"])
    cache_control = _frames_cache_control(run, run_complete=run_complete)
    etag = _make_etag(frames)
    r304 = _maybe_304(request, etag=etag, cache_control=cache_control)
    if r304 is not None:
        return r304

    return JSONResponse(
        content=frames,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
        },
    )


@app.get("/api/v4/{model}/{run}/{var}/loop-manifest")
def get_loop_manifest(request: Request, model: str, run: str, var: str):
    resolved = _resolve_run(model, run)
    if resolved is None:
        return Response(status_code=404, content='{"error": "run not found"}', media_type="application/json")

    manifest = _load_manifest(model, resolved)
    if manifest is None:
        return Response(status_code=404, content='{"error": "manifest not found"}', media_type="application/json")

    variables = manifest.get("variables")
    if not isinstance(variables, dict):
        return []
    var_entry = variables.get(var)
    if not isinstance(var_entry, dict):
        return []

    frame_entries = var_entry.get("frames")
    if not isinstance(frame_entries, list):
        frame_entries = []

    version_token = _run_version_token(model, resolved)

    tier_frames: dict[int, list[dict[str, Any]]] = {0: [], 1: []}
    for item in frame_entries:
        if not isinstance(item, dict):
            continue
        fh = item.get("fh")
        if not isinstance(fh, int):
            continue

        tier0_url, tier1_url = _resolve_loop_urls_for_frame(
            model,
            resolved,
            var,
            fh,
            version_token=version_token,
            include_tier0_runtime_fallback=True,
        )
        if tier0_url:
            tier_frames[0].append({"fh": fh, "url": tier0_url})
        if tier1_url:
            tier_frames[1].append({"fh": fh, "url": tier1_url})

    tier_frames[0].sort(key=lambda row: int(row["fh"]))
    tier_frames[1].sort(key=lambda row: int(row["fh"]))

    tier0_dim = LOOP_TIER_CONFIG.get(0, {}).get("max_dim", LOOP_WEBP_MAX_DIM)
    tier1_dim = LOOP_TIER_CONFIG.get(1, {}).get("max_dim", LOOP_WEBP_TIER1_MAX_DIM)
    payload = {
        "manifest_version": LOOP_MANIFEST_VERSION,
        "run": resolved,
        "model": model,
        "var": var,
        "bbox": LOOP_MANIFEST_BBOX,
        "projection": LOOP_MANIFEST_PROJECTION,
        "loop_tiers": [
            {
                "tier": 0,
                "max_dim": int(tier0_dim),
                "frames": tier_frames[0],
            },
            {
                "tier": 1,
                "max_dim": int(tier1_dim),
                "frames": tier_frames[1],
            },
        ],
    }

    cache_control = "public, max-age=60"
    etag = _make_etag(payload)
    r304 = _maybe_304(request, etag=etag, cache_control=cache_control)
    if r304 is not None:
        return r304

    return JSONResponse(
        content=payload,
        headers={
            "Cache-Control": cache_control,
            "ETag": etag,
        },
    )


@app.get("/api/v4/{model}/{run}/{var}/{fh:int}/loop.webp")
def get_loop_webp(
    model: str,
    run: str,
    var: str,
    fh: int,
    tier: int = Query(0, ge=0, le=1, description="Loop tier (0=default, 1=high-res)"),
):
    resolved = _resolve_run(model, run)
    if resolved is None:
        return Response(status_code=404, headers={"Cache-Control": CACHE_MISS})

    cog_path = _resolve_rgba_cog(model, resolved, var, fh)
    if cog_path is None:
        return Response(status_code=404, headers={"Cache-Control": CACHE_MISS})
    value_cog_path = _resolve_val_cog(model, resolved, var, fh)

    legacy_path = _legacy_loop_webp_path(model, resolved, var, fh, tier=tier)
    if legacy_path is not None:
        cache_control = CACHE_HIT if run != "latest" else CACHE_MISS
        return FileResponse(
            path=str(legacy_path),
            media_type="image/webp",
            headers={"Cache-Control": cache_control},
        )

    out_path = _loop_webp_path(model, resolved, var, fh, tier=tier)
    if out_path is None:
        return Response(status_code=404, headers={"Cache-Control": CACHE_MISS})

    if not _ensure_loop_webp(
        cog_path,
        out_path,
        model_id=model,
        var_key=var,
        tier=tier,
        value_cog_path=value_cog_path,
        run_id=resolved,
    ):
        # Graceful degradation path: avoid surfacing hard 500s to clients when
        # cache writes fail (permissions/disk), and allow tier-1 to fall back.
        if tier == 1:
            tier0_legacy = _legacy_loop_webp_path(model, resolved, var, fh, tier=0)
            if tier0_legacy is not None:
                return FileResponse(
                    path=str(tier0_legacy),
                    media_type="image/webp",
                    headers={"Cache-Control": CACHE_MISS},
                )

            tier0_out = _loop_webp_path(model, resolved, var, fh, tier=0)
            if tier0_out is not None and _ensure_loop_webp(
                cog_path,
                tier0_out,
                model_id=model,
                var_key=var,
                tier=0,
                value_cog_path=value_cog_path,
                run_id=resolved,
            ):
                return FileResponse(
                    path=str(tier0_out),
                    media_type="image/webp",
                    headers={"Cache-Control": CACHE_MISS},
                )

            tier0_bytes = _render_loop_webp_bytes(
                cog_path,
                model_id=model,
                var_key=var,
                tier=0,
                value_cog_path=value_cog_path,
                run_id=resolved,
            )
            if tier0_bytes is not None:
                return Response(content=tier0_bytes, media_type="image/webp", headers={"Cache-Control": CACHE_MISS})

        content = _render_loop_webp_bytes(
            cog_path,
            model_id=model,
            var_key=var,
            tier=tier,
            value_cog_path=value_cog_path,
            run_id=resolved,
        )
        if content is not None:
            return Response(content=content, media_type="image/webp", headers={"Cache-Control": CACHE_MISS})

        return Response(status_code=404, headers={"Cache-Control": CACHE_MISS})

    cache_control = CACHE_HIT if run != "latest" else CACHE_MISS
    return FileResponse(
        path=str(out_path),
        media_type="image/webp",
        headers={"Cache-Control": cache_control},
    )


@app.get("/api/v4/sample")
def sample(
    request: Request,
    model: str = Query(..., description="Model ID (e.g. hrrr)"),
    run: str = Query(..., description="Run ID (e.g. 20260217_20z or latest)"),
    var: str = Query(..., description="Variable ID (e.g. tmp2m)"),
    fh: int = Query(..., description="Forecast hour"),
    lat: float = Query(..., ge=-90, le=90, description="Latitude (WGS84)"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude (WGS84)"),
):
    client_id = request.client.host if request.client and request.client.host else "unknown"
    allowed, retry_after = _sample_rate_limit_allow(client_id)
    if not allowed:
        return JSONResponse(
            status_code=429,
            content={"error": "rate limit exceeded", "retryAfterSec": retry_after},
            headers={"Retry-After": str(int(max(1, retry_after)))},
        )

    val_cog = _resolve_val_cog(model, run, var, fh)
    if val_cog is None:
        return Response(status_code=404, content='{"error": "val.cog.tif not found"}', media_type="application/json")

    try:
        ds = _get_cached_dataset(val_cog)
        row, col = _sample_dataset_index(ds, lon=lon, lat=lat)
        resolved_run = _resolve_run(model, run) or run
        sidecar = _resolve_sidecar(model, run, var, fh)
        units = sidecar.get("units", "") if sidecar else ""
        valid_time = sidecar.get("valid_time", "") if sidecar else ""

        if row < 0 or row >= ds.height or col < 0 or col >= ds.width:
            payload = _sample_payload(
                model=model,
                run=resolved_run,
                var=var,
                fh=fh,
                lat=lat,
                lon=lon,
                value=None,
                units=units,
                valid_time=valid_time,
                no_data=True,
            )
            return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=300"})

        key = _sample_cache_key(model, resolved_run, var, fh, row, col)
        now = time.monotonic()
        inflight: _SampleInflight | None = None
        is_leader = False

        with _sample_lock:
            cached = _sample_cache.get(key)
            if cached is not None:
                expires_at, payload = cached
                if expires_at > now:
                    return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=300"})
                _sample_cache.pop(key, None)

            inflight = _sample_inflight.get(key)
            if inflight is None:
                inflight = _SampleInflight()
                _sample_inflight[key] = inflight
                is_leader = True

        if not is_leader:
            assert inflight is not None
            inflight.event.wait(timeout=SAMPLE_INFLIGHT_WAIT_SECONDS)
            with _sample_lock:
                cached = _sample_cache.get(key)
                if cached is not None:
                    expires_at, payload = cached
                    if expires_at > time.monotonic():
                        return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=300"})
                payload = inflight.payload
                if payload is not None:
                    return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=300"})

        value, no_data = _read_sample_value(ds, row=row, col=col, masked=False)

        payload = _sample_payload(
            model=model,
            run=resolved_run,
            var=var,
            fh=fh,
            lat=lat,
            lon=lon,
            value=value,
            units=units,
            valid_time=valid_time,
            no_data=no_data,
        )

        with _sample_lock:
            _sample_cache[key] = (time.monotonic() + SAMPLE_CACHE_TTL_SECONDS, payload)
            sample_inflight = _sample_inflight.pop(key, None)
            if sample_inflight is not None:
                sample_inflight.payload = payload
                sample_inflight.event.set()

        return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=86400"})

    except Exception:
        with _sample_lock:
            key = locals().get("key")
            if isinstance(key, str):
                sample_inflight = _sample_inflight.pop(key, None)
                if sample_inflight is not None:
                    sample_inflight.event.set()
        logger.exception(
            "Sample query failed: %s/%s/%s/fh%03d @ (%.4f, %.4f)",
            model,
            run,
            var,
            fh,
            lat,
            lon,
        )
        return Response(status_code=500, content='{"error": "internal error"}', media_type="application/json")


@app.post("/api/v4/sample/batch")
def sample_batch(request: Request, body: SampleBatchIn):
    client_id = request.client.host if request.client and request.client.host else "unknown"
    allowed, retry_after = _sample_rate_limit_allow(client_id)
    if not allowed:
        return JSONResponse(
            status_code=429,
            content={"error": "rate limit exceeded", "retryAfterSec": retry_after},
            headers={"Retry-After": str(int(max(1, retry_after)))},
        )

    val_cog = _resolve_val_cog(body.model, body.run, body.variable, body.forecast_hour)
    if val_cog is None:
        return Response(status_code=404, content='{"error": "val.cog.tif not found"}', media_type="application/json")

    resolved_run = _resolve_run(body.model, body.run) or body.run
    key = _sample_batch_cache_key(
        body.model,
        resolved_run,
        body.variable,
        body.forecast_hour,
        _sample_points_hash(body.points),
    )
    now = time.monotonic()
    inflight: _SampleInflight | None = None
    is_leader = False

    with _sample_lock:
        cached = _sample_cache.get(key)
        if cached is not None:
            expires_at, payload = cached
            if expires_at > now:
                return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=300"})
            _sample_cache.pop(key, None)

        inflight = _sample_inflight.get(key)
        if inflight is None:
            inflight = _SampleInflight()
            _sample_inflight[key] = inflight
            is_leader = True

    if not is_leader:
        assert inflight is not None
        inflight.event.wait(timeout=SAMPLE_INFLIGHT_WAIT_SECONDS)
        with _sample_lock:
            cached = _sample_cache.get(key)
            if cached is not None:
                expires_at, payload = cached
                if expires_at > time.monotonic():
                    return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=300"})
            payload = inflight.payload
            if payload is not None:
                return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=300"})

    try:
        ds = _get_cached_dataset(val_cog)
        sidecar = _resolve_sidecar(body.model, body.run, body.variable, body.forecast_hour)
        units = sidecar.get("units", "") if sidecar else ""
        payload = {
            "units": units,
            "values": _sample_batch_values(ds, points=body.points),
        }

        with _sample_lock:
            _sample_cache[key] = (time.monotonic() + SAMPLE_CACHE_TTL_SECONDS, payload)
            sample_inflight = _sample_inflight.pop(key, None)
            if sample_inflight is not None:
                sample_inflight.payload = payload
                sample_inflight.event.set()

        return JSONResponse(content=payload, headers={"Cache-Control": "private, max-age=86400"})

    except Exception:
        with _sample_lock:
            sample_inflight = _sample_inflight.pop(key, None)
            if sample_inflight is not None:
                sample_inflight.event.set()
        logger.exception(
            "Batch sample query failed: %s/%s/%s/fh%03d points=%d",
            body.model,
            body.run,
            body.variable,
            body.forecast_hour,
            len(body.points),
        )
        return Response(status_code=500, content='{"error": "internal error"}', media_type="application/json")


@app.get("/api/v4/{model}/{run}/{var}/{fh:int}/contours/{key}")
def get_contour_geojson(
    model: str,
    run: str,
    var: str,
    fh: int,
    key: str,
):
    var_dir = _resolve_frame_var_dir(model, run, var, fh)
    if var_dir is None:
        raise HTTPException(status_code=404, detail="Frame not found")

    sidecar_path = var_dir / f"fh{fh:03d}.json"
    if not sidecar_path.is_file():
        raise HTTPException(status_code=404, detail="Sidecar not found")

    try:
        sidecar = json.loads(sidecar_path.read_text())
    except Exception as exc:
        logger.exception(
            "Failed to read sidecar for contour: %s/%s/%s/fh%03d (%s)",
            model,
            run,
            var,
            fh,
            sidecar_path,
        )
        raise HTTPException(status_code=500, detail=f"Failed to read sidecar: {exc}") from exc

    contours = sidecar.get("contours")
    if not isinstance(contours, dict) or key not in contours:
        raise HTTPException(status_code=404, detail=f"Contour '{key}' not found")

    contour_meta = contours[key]
    contour_rel_path = contour_meta.get("path") if isinstance(contour_meta, dict) else None
    if not isinstance(contour_rel_path, str) or not contour_rel_path:
        raise HTTPException(status_code=500, detail=f"Contour '{key}' has invalid sidecar path")

    contour_path = var_dir / contour_rel_path
    if not contour_path.is_file():
        raise HTTPException(status_code=404, detail=f"Contour file missing: {contour_rel_path}")

    try:
        return json.loads(contour_path.read_text())
    except Exception as exc:
        logger.exception(
            "Failed to read contour GeoJSON: %s/%s/%s/fh%03d/%s (%s)",
            model,
            run,
            var,
            fh,
            key,
            contour_path,
        )
        raise HTTPException(status_code=500, detail=f"Failed to read contour GeoJSON: {exc}") from exc

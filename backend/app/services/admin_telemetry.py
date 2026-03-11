from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
from pyproj import Transformer

TELEMETRY_DB_PATH = Path(
    os.environ.get("CARTOSKY_TELEMETRY_DB_PATH")
    or os.environ.get("TWM_TELEMETRY_DB_PATH", "./data/admin_telemetry.sqlite3")
)

ALLOWED_PERF_EVENT_NAMES = {
    "viewer_first_frame",
    "frame_change",
    "loop_start",
    "scrub_latency",
    "variable_switch",
    "tile_fetch",
    "animation_stall",
}

ALLOWED_USAGE_EVENT_NAMES = {
    "model_selected",
    "variable_selected",
    "region_selected",
    "animation_play",
}

PERF_TARGETS_MS = {
    "viewer_first_frame": 1500.0,
    "frame_change": 250.0,
    "loop_start": 1000.0,
    "scrub_latency": 150.0,
    "variable_switch": 600.0,
    "tile_fetch": 800.0,
    "animation_stall": 750.0,
}

VERIFICATION_VARIABLE_IDS = {
    "tmp2m",
    "precip_total",
    "snowfall_total",
    "snowfall_kuchera_total",
}

VERIFICATION_CUMULATIVE_VARIABLE_IDS = {
    "precip_total",
    "snowfall_total",
    "snowfall_kuchera_total",
}

_db_init_lock = threading.Lock()
_db_initialized = False


def _ensure_parent_dir(path: Path) -> None:
    parent = path.parent
    if str(parent) and str(parent) != ".":
        parent.mkdir(parents=True, exist_ok=True)


def _connect() -> sqlite3.Connection:
    _ensure_parent_dir(TELEMETRY_DB_PATH)
    conn = sqlite3.connect(TELEMETRY_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    _init_db(conn)
    return conn


def _init_db(conn: sqlite3.Connection) -> None:
    global _db_initialized
    if _db_initialized:
        return
    with _db_init_lock:
        if _db_initialized:
            return
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS perf_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER NOT NULL,
                session_id TEXT NOT NULL,
                member_id INTEGER,
                event_name TEXT NOT NULL,
                duration_ms REAL NOT NULL,
                model_id TEXT,
                variable_id TEXT,
                run_id TEXT,
                region_id TEXT,
                forecast_hour INTEGER,
                device_type TEXT,
                viewport_bucket TEXT,
                page TEXT,
                meta_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_perf_events_event_created
                ON perf_events(event_name, created_at);
            CREATE INDEX IF NOT EXISTS idx_perf_events_created
                ON perf_events(created_at);
            CREATE INDEX IF NOT EXISTS idx_perf_events_model_var_created
                ON perf_events(model_id, variable_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_perf_events_device_created
                ON perf_events(device_type, created_at);

            CREATE TABLE IF NOT EXISTS usage_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER NOT NULL,
                session_id TEXT NOT NULL,
                member_id INTEGER,
                event_name TEXT NOT NULL,
                model_id TEXT,
                variable_id TEXT,
                run_id TEXT,
                region_id TEXT,
                forecast_hour INTEGER,
                device_type TEXT,
                viewport_bucket TEXT,
                page TEXT,
                meta_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_usage_events_event_created
                ON usage_events(event_name, created_at);
            CREATE INDEX IF NOT EXISTS idx_usage_events_created
                ON usage_events(created_at);

            CREATE TABLE IF NOT EXISTS synthetic_perf_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER NOT NULL,
                commit_sha TEXT,
                branch TEXT,
                environment TEXT,
                scenario TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value_ms REAL NOT NULL,
                threshold_ms REAL,
                status TEXT NOT NULL,
                details_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_synthetic_perf_runs_metric_created
                ON synthetic_perf_runs(metric_name, created_at);

            CREATE TABLE IF NOT EXISTS qa_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                model_id TEXT NOT NULL,
                variable_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                forecast_hour INTEGER NOT NULL,
                auto_status TEXT NOT NULL,
                manual_status TEXT NOT NULL,
                benchmark_site TEXT,
                reviewer_name TEXT,
                reviewer_member_id INTEGER,
                notes TEXT,
                auto_checks_json TEXT,
                coverage_fraction REAL,
                valid_pixel_count INTEGER,
                total_pixel_count INTEGER,
                range_min REAL,
                range_max REAL,
                last_checked_at INTEGER NOT NULL,
                UNIQUE(model_id, variable_id, run_id, forecast_hour)
            );

            CREATE INDEX IF NOT EXISTS idx_qa_reviews_updated
                ON qa_reviews(updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_qa_reviews_run
                ON qa_reviews(model_id, run_id, variable_id, forecast_hour);
            CREATE INDEX IF NOT EXISTS idx_qa_reviews_manual
                ON qa_reviews(manual_status, updated_at DESC);
            """
        )
        qa_cols = {str(row["name"]) for row in conn.execute("PRAGMA table_info(qa_reviews)").fetchall()}
        if "warning_summary" not in qa_cols:
            conn.execute("ALTER TABLE qa_reviews ADD COLUMN warning_summary TEXT")
        if "severity" not in qa_cols:
            conn.execute("ALTER TABLE qa_reviews ADD COLUMN severity TEXT")
        if "diagnostics_json" not in qa_cols:
            conn.execute("ALTER TABLE qa_reviews ADD COLUMN diagnostics_json TEXT")
        _db_initialized = True


def _normalize_text(value: Any, *, max_length: int = 120) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:max_length]


def _normalize_forecast_hour(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _serialize_meta(value: Any) -> str | None:
    if value is None:
        return None
    try:
        encoded = json.dumps(value, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError):
        return None
    return encoded[:4000]


def _load_json_file(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _normalize_manual_status(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"pass", "fail", "review"}:
        return normalized
    return "review"


def _value_cog_path(data_root: Path, model_id: str, run_id: str, variable_id: str, forecast_hour: int) -> Path:
    return data_root / "published" / model_id / run_id / variable_id / f"fh{forecast_hour:03d}.val.cog.tif"


def _sidecar_path(data_root: Path, model_id: str, run_id: str, variable_id: str, forecast_hour: int) -> Path:
    return data_root / "published" / model_id / run_id / variable_id / f"fh{forecast_hour:03d}.json"


def _manifest_path(data_root: Path, model_id: str, run_id: str) -> Path:
    return data_root / "manifests" / model_id / f"{run_id}.json"


def _finite_grid_stats(path: Path) -> tuple[int, int, float | None, float | None]:
    with rasterio.open(path) as dataset:
        data = dataset.read(1, masked=False)
    finite_mask = np.isfinite(data)
    valid_count = int(finite_mask.sum())
    total_count = int(data.size)
    if valid_count <= 0:
        return valid_count, total_count, None, None
    finite_values = data[finite_mask]
    return valid_count, total_count, float(np.min(finite_values)), float(np.max(finite_values))


def _pixel_to_lon_lat(path: Path, row: int, col: int) -> tuple[float | None, float | None]:
    with rasterio.open(path) as dataset:
        x, y = dataset.xy(row, col)
        src_crs = dataset.crs
    if src_crs is None:
        return None, None
    transformer = Transformer.from_crs(src_crs, "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(x, y)
    return float(lon), float(lat)


def _monotonic_diagnostics(current_path: Path, previous_path: Path, *, tolerance: float = 0.01) -> dict[str, Any] | None:
    if not current_path.exists() or not previous_path.exists():
        return None
    with rasterio.open(current_path) as current_ds:
        current = current_ds.read(1, masked=False)
    with rasterio.open(previous_path) as previous_ds:
        previous = previous_ds.read(1, masked=False)
    if current.shape != previous.shape:
        return {
            "ok": False,
            "reason": "shape_mismatch",
        }
    valid_mask = np.isfinite(current) & np.isfinite(previous)
    if not valid_mask.any():
        return None
    deltas = current[valid_mask] - previous[valid_mask]
    threshold = -abs(float(tolerance))
    decreased_mask = valid_mask & ((current - previous) < threshold)
    decreased_count = int(decreased_mask.sum())
    valid_count = int(valid_mask.sum())
    min_delta = float(np.min(deltas))
    max_increase = float(np.max(deltas))
    if decreased_count <= 0:
        return {
            "ok": True,
            "decreased_pixel_count": 0,
            "decreased_fraction": 0.0,
            "max_decrease": 0.0,
            "max_increase": round(max_increase, 3),
        }

    delta_grid = current - previous
    decrease_values = np.where(decreased_mask, delta_grid, np.inf)
    row, col = np.unravel_index(int(np.argmin(decrease_values)), decrease_values.shape)
    lon, lat = _pixel_to_lon_lat(current_path, int(row), int(col))
    return {
        "ok": False,
        "decreased_pixel_count": decreased_count,
        "decreased_fraction": round(decreased_count / max(1, valid_count), 6),
        "max_decrease": round(abs(min_delta), 3),
        "max_increase": round(max_increase, 3),
        "max_decrease_lon": round(lon, 3) if lon is not None else None,
        "max_decrease_lat": round(lat, 3) if lat is not None else None,
    }


def _severity_from_diagnostics(*, checks: dict[str, Any], diagnostics: dict[str, Any]) -> str:
    if checks.get("has_valid_pixels") is not True or checks.get("coverage_present") is not True:
        return "high"
    monotonic = diagnostics.get("monotonic")
    if isinstance(monotonic, dict) and monotonic.get("ok") is False:
        fraction = float(monotonic.get("decreased_fraction") or 0.0)
        max_decrease = float(monotonic.get("max_decrease") or 0.0)
        if fraction >= 0.05 or max_decrease >= 2.0:
            return "high"
        if fraction >= 0.01 or max_decrease >= 1.0:
            return "medium"
        return "low"
    return "none"


def _warning_summary(*, variable_id: str, checks: dict[str, Any], diagnostics: dict[str, Any]) -> str | None:
    if checks.get("has_valid_pixels") is not True:
        return "No valid pixels found in published value grid."
    if checks.get("coverage_present") is not True:
        return "Coverage fell below the minimum expected threshold."
    if checks.get("range_present") is not True:
        return "Value range metadata is missing or invalid."
    monotonic = diagnostics.get("monotonic")
    if isinstance(monotonic, dict) and monotonic.get("ok") is False:
        if monotonic.get("reason") == "shape_mismatch":
            return "Current and previous forecast hours are on different grid shapes."
        decreased_fraction = float(monotonic.get("decreased_fraction") or 0.0) * 100.0
        max_decrease = float(monotonic.get("max_decrease") or 0.0)
        lat = monotonic.get("max_decrease_lat")
        lon = monotonic.get("max_decrease_lon")
        location = f" near {lat}, {lon}" if lat is not None and lon is not None else ""
        return (
            f"Cumulative {variable_id} decreased versus the previous hour at "
            f"{decreased_fraction:.1f}% of valid pixels; max drop {max_decrease:.1f}{location}."
        )
    return None


def _build_auto_checks(
    *,
    data_root: Path,
    model_id: str,
    variable_id: str,
    run_id: str,
    forecast_hour: int,
    previous_forecast_hour: int | None = None,
) -> dict[str, Any]:
    value_path = _value_cog_path(data_root, model_id, run_id, variable_id, forecast_hour)
    sidecar = _load_json_file(_sidecar_path(data_root, model_id, run_id, variable_id, forecast_hour))

    checks: dict[str, Any] = {
        "has_valid_pixels": False,
        "range_present": False,
        "coverage_present": False,
        "monotonic": None,
    }
    metrics: dict[str, Any] = {
        "coverage_fraction": None,
        "valid_pixel_count": 0,
        "total_pixel_count": 0,
        "range_min": None,
        "range_max": None,
    }
    diagnostics: dict[str, Any] = {}
    status = "warning"

    if not value_path.exists():
        diagnostics = {
            "artifact": {
                "issue_type": "missing_value_grid",
                "value_grid_exists": False,
                "value_grid_path": str(value_path),
                "sidecar_exists": _sidecar_path(data_root, model_id, run_id, variable_id, forecast_hour).exists(),
                "sidecar_path": str(_sidecar_path(data_root, model_id, run_id, variable_id, forecast_hour)),
            }
        }
        return {
            "status": status,
            "checks": checks,
            "metrics": metrics,
            "diagnostics": diagnostics,
            "severity": "high",
            "warning_summary": f"Published value grid is missing for {model_id}/{run_id}/{variable_id}/fh{forecast_hour:03d}.",
        }

    try:
        valid_count, total_count, range_min, range_max = _finite_grid_stats(value_path)
    except Exception as exc:
        diagnostics = {
            "artifact": {
                "issue_type": "unreadable_value_grid",
                "value_grid_exists": True,
                "value_grid_path": str(value_path),
                "sidecar_exists": _sidecar_path(data_root, model_id, run_id, variable_id, forecast_hour).exists(),
                "sidecar_path": str(_sidecar_path(data_root, model_id, run_id, variable_id, forecast_hour)),
                "read_error": str(exc),
            }
        }
        return {
            "status": status,
            "checks": checks,
            "metrics": metrics,
            "diagnostics": diagnostics,
            "severity": "high",
            "warning_summary": f"Published value grid could not be read for {model_id}/{run_id}/{variable_id}/fh{forecast_hour:03d}.",
        }

    coverage_fraction = (valid_count / total_count) if total_count > 0 else 0.0
    checks["has_valid_pixels"] = valid_count > 0
    checks["coverage_present"] = coverage_fraction >= 0.01

    sidecar_min = sidecar.get("min") if isinstance(sidecar, dict) else None
    sidecar_max = sidecar.get("max") if isinstance(sidecar, dict) else None
    checks["range_present"] = (
        isinstance(sidecar_min, (int, float))
        and isinstance(sidecar_max, (int, float))
        and np.isfinite(sidecar_min)
        and np.isfinite(sidecar_max)
        and float(sidecar_max) >= float(sidecar_min)
    ) or (
        range_min is not None and range_max is not None and float(range_max) >= float(range_min)
    )

    if variable_id in VERIFICATION_CUMULATIVE_VARIABLE_IDS:
        previous_path = (
            _value_cog_path(data_root, model_id, run_id, variable_id, previous_forecast_hour)
            if previous_forecast_hour is not None
            else None
        )
        monotonic = _monotonic_diagnostics(value_path, previous_path) if previous_path is not None else None
        diagnostics["monotonic"] = monotonic
        checks["monotonic"] = monotonic.get("ok") if isinstance(monotonic, dict) else None

    status = "pass"
    for check_name, value in checks.items():
        if check_name == "monotonic" and value is None:
            continue
        if value is not True:
            status = "warning"
            break

    metrics = {
        "coverage_fraction": round(float(coverage_fraction), 6),
        "valid_pixel_count": valid_count,
        "total_pixel_count": total_count,
        "range_min": round(float(range_min), 3) if range_min is not None else None,
        "range_max": round(float(range_max), 3) if range_max is not None else None,
    }
    return {
        "status": status,
        "checks": checks,
        "metrics": metrics,
        "diagnostics": diagnostics,
        "severity": _severity_from_diagnostics(checks=checks, diagnostics=diagnostics),
        "warning_summary": _warning_summary(variable_id=variable_id, checks=checks, diagnostics=diagnostics),
    }


def sync_verification_run(*, data_root: Path, model_id: str, run_id: str) -> int:
    manifest = _load_json_file(_manifest_path(data_root, model_id, run_id))
    if not isinstance(manifest, dict):
        return 0

    variables = manifest.get("variables")
    if not isinstance(variables, dict):
        return 0

    now = int(time.time())
    synced = 0
    with _connect() as conn:
        for variable_id, variable_meta in variables.items():
            if str(variable_id) not in VERIFICATION_VARIABLE_IDS:
                continue
            if not isinstance(variable_meta, dict):
                continue
            frames = variable_meta.get("frames")
            if not isinstance(frames, list):
                continue
            available_hours = sorted(
                int(frame.get("fh"))
                for frame in frames
                if isinstance(frame, dict) and isinstance(frame.get("fh"), int)
            )
            for forecast_hour in available_hours:
                previous_forecast_hour = None
                if str(variable_id) in VERIFICATION_CUMULATIVE_VARIABLE_IDS:
                    previous_values = [fh for fh in available_hours if fh < forecast_hour]
                    if previous_values:
                        previous_forecast_hour = previous_values[-1]

                auto_result = _build_auto_checks(
                    data_root=data_root,
                    model_id=model_id,
                    variable_id=str(variable_id),
                    run_id=run_id,
                    forecast_hour=forecast_hour,
                    previous_forecast_hour=previous_forecast_hour,
                )
                conn.execute(
                    """
                    INSERT INTO qa_reviews (
                        created_at,
                        updated_at,
                        model_id,
                        variable_id,
                        run_id,
                        forecast_hour,
                        auto_status,
                        manual_status,
                        auto_checks_json,
                        coverage_fraction,
                        valid_pixel_count,
                        total_pixel_count,
                        range_min,
                        range_max,
                        warning_summary,
                        severity,
                        diagnostics_json,
                        last_checked_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(model_id, variable_id, run_id, forecast_hour)
                    DO UPDATE SET
                        updated_at=excluded.updated_at,
                        auto_status=excluded.auto_status,
                        auto_checks_json=excluded.auto_checks_json,
                        coverage_fraction=excluded.coverage_fraction,
                        valid_pixel_count=excluded.valid_pixel_count,
                        total_pixel_count=excluded.total_pixel_count,
                        range_min=excluded.range_min,
                        range_max=excluded.range_max,
                        warning_summary=excluded.warning_summary,
                        severity=excluded.severity,
                        diagnostics_json=excluded.diagnostics_json,
                        last_checked_at=excluded.last_checked_at
                    """,
                    (
                        now,
                        now,
                        str(model_id),
                        str(variable_id),
                        str(run_id),
                        int(forecast_hour),
                        str(auto_result["status"]),
                        "review",
                        _serialize_meta(auto_result["checks"]),
                        auto_result["metrics"]["coverage_fraction"],
                        auto_result["metrics"]["valid_pixel_count"],
                        auto_result["metrics"]["total_pixel_count"],
                        auto_result["metrics"]["range_min"],
                        auto_result["metrics"]["range_max"],
                        _normalize_text(auto_result["warning_summary"], max_length=240),
                        _normalize_text(auto_result["severity"], max_length=24),
                        _serialize_meta(auto_result["diagnostics"]),
                        now,
                    ),
                )
                synced += 1
    return synced


def sync_recent_verification_runs(*, data_root: Path, limit_runs_per_model: int = 2) -> int:
    manifests_root = data_root / "manifests"
    if not manifests_root.is_dir():
        return 0

    synced = 0
    for model_dir in sorted(path for path in manifests_root.iterdir() if path.is_dir()):
        run_ids = sorted(
            [path.stem for path in model_dir.glob("*.json") if path.is_file()],
            reverse=True,
        )[: max(1, int(limit_runs_per_model))]
        for run_id in run_ids:
            synced += sync_verification_run(data_root=data_root, model_id=model_dir.name, run_id=run_id)
    return synced


def sync_latest_missing_verification_runs(*, data_root: Path, limit_runs_per_model: int = 2) -> int:
    manifests_root = data_root / "manifests"
    if not manifests_root.is_dir():
        return 0

    synced = 0
    with _connect() as conn:
        for model_dir in sorted(path for path in manifests_root.iterdir() if path.is_dir()):
            run_ids = sorted(
                [path.stem for path in model_dir.glob("*.json") if path.is_file()],
                reverse=True,
            )[: max(1, int(limit_runs_per_model))]
            for run_id in run_ids:
                row = conn.execute(
                    """
                    SELECT 1
                    FROM qa_reviews
                    WHERE model_id = ? AND run_id = ?
                    LIMIT 1
                    """,
                    (model_dir.name, run_id),
                ).fetchone()
                if row is not None:
                    continue
                synced += sync_verification_run(data_root=data_root, model_id=model_dir.name, run_id=run_id)
    return synced


def verification_rows_count() -> int:
    with _connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS total FROM qa_reviews").fetchone()
    return int(row["total"] or 0)


def ensure_verification_seeded(*, data_root: Path, limit_runs_per_model: int = 2) -> int:
    if verification_rows_count() > 0:
        return 0
    return sync_recent_verification_runs(data_root=data_root, limit_runs_per_model=limit_runs_per_model)


def refresh_missing_verification_diagnostics(*, data_root: Path, limit_runs: int = 50) -> int:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT model_id, run_id
            FROM qa_reviews
            WHERE auto_status = 'warning'
              AND (
                warning_summary IS NULL OR warning_summary = ''
                OR severity IS NULL OR severity = ''
                OR diagnostics_json IS NULL OR diagnostics_json = '' OR diagnostics_json = '{}'
              )
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (max(1, int(limit_runs)),),
        ).fetchall()

    refreshed = 0
    for row in rows:
        refreshed += sync_verification_run(
            data_root=data_root,
            model_id=str(row["model_id"]),
            run_id=str(row["run_id"]),
        )
    return refreshed


def ensure_verification_ready(*, data_root: Path, seed_limit_runs_per_model: int = 2, refresh_limit_runs: int = 50) -> int:
    seeded = ensure_verification_seeded(data_root=data_root, limit_runs_per_model=seed_limit_runs_per_model)
    latest = sync_latest_missing_verification_runs(data_root=data_root, limit_runs_per_model=seed_limit_runs_per_model)
    refreshed = refresh_missing_verification_diagnostics(data_root=data_root, limit_runs=refresh_limit_runs)
    return seeded + latest + refreshed


def get_verification_summary(
    *,
    since_ts: int,
    model_id: str | None = None,
    variable_id: str | None = None,
) -> dict[str, Any]:
    clauses = ["updated_at >= ?"]
    params: list[Any] = [since_ts]
    if model_id:
        clauses.append("model_id = ?")
        params.append(model_id)
    if variable_id:
        clauses.append("variable_id = ?")
        params.append(variable_id)

    where_sql = " WHERE " + " AND ".join(clauses)
    with _connect() as conn:
        row = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN auto_status = 'pass' THEN 1 ELSE 0 END) AS auto_pass_rows,
                SUM(CASE WHEN manual_status = 'review' AND auto_status = 'warning' THEN 1 ELSE 0 END) AS manual_review_rows,
                SUM(CASE WHEN auto_status = 'warning' OR manual_status = 'fail' THEN 1 ELSE 0 END) AS flagged_rows
            FROM qa_reviews
            {where_sql}
            """,
            params,
        ).fetchone()

    return {
        "total_rows": int(row["total_rows"] or 0),
        "auto_pass_rows": int(row["auto_pass_rows"] or 0),
        "manual_review_rows": int(row["manual_review_rows"] or 0),
        "flagged_rows": int(row["flagged_rows"] or 0),
    }


def get_verification_results(
    *,
    since_ts: int,
    model_id: str | None = None,
    variable_id: str | None = None,
    manual_status: str | None = None,
    flagged_only: bool = False,
    attention_only: bool = False,
    limit: int = 200,
) -> list[dict[str, Any]]:
    clauses = ["updated_at >= ?"]
    params: list[Any] = [since_ts]
    if model_id:
        clauses.append("model_id = ?")
        params.append(model_id)
    if variable_id:
        clauses.append("variable_id = ?")
        params.append(variable_id)
    normalized_manual_status = _normalize_manual_status(manual_status) if manual_status else None
    if normalized_manual_status:
        clauses.append("manual_status = ?")
        params.append(normalized_manual_status)
    if flagged_only:
        clauses.append("(auto_status = 'warning' OR manual_status = 'fail')")
    if attention_only:
        clauses.append("manual_status = 'review'")
        clauses.append("auto_status = 'warning'")

    params.append(max(1, min(500, int(limit))))
    where_sql = " WHERE " + " AND ".join(clauses)

    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
                id,
                created_at,
                updated_at,
                model_id,
                variable_id,
                run_id,
                forecast_hour,
                auto_status,
                manual_status,
                benchmark_site,
                reviewer_name,
                reviewer_member_id,
                notes,
                auto_checks_json,
                coverage_fraction,
                valid_pixel_count,
                total_pixel_count,
                range_min,
                range_max,
                warning_summary,
                severity,
                diagnostics_json,
                last_checked_at
            FROM qa_reviews
            {where_sql}
            ORDER BY updated_at DESC, model_id ASC, run_id DESC, variable_id ASC, forecast_hour ASC
            LIMIT ?
            """,
            params,
        ).fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        auto_checks = {}
        diagnostics = {}
        if row["auto_checks_json"]:
            try:
                parsed = json.loads(str(row["auto_checks_json"]))
                if isinstance(parsed, dict):
                    auto_checks = parsed
            except json.JSONDecodeError:
                auto_checks = {}
        if row["diagnostics_json"]:
            try:
                parsed = json.loads(str(row["diagnostics_json"]))
                if isinstance(parsed, dict):
                    diagnostics = parsed
            except json.JSONDecodeError:
                diagnostics = {}
        results.append(
            {
                "id": int(row["id"]),
                "created_at": int(row["created_at"]),
                "updated_at": int(row["updated_at"]),
                "model_id": str(row["model_id"]),
                "variable_id": str(row["variable_id"]),
                "run_id": str(row["run_id"]),
                "forecast_hour": int(row["forecast_hour"]),
                "auto_status": str(row["auto_status"]),
                "manual_status": str(row["manual_status"]),
                "benchmark_site": _normalize_text(row["benchmark_site"], max_length=120),
                "reviewer_name": _normalize_text(row["reviewer_name"], max_length=120),
                "reviewer_member_id": int(row["reviewer_member_id"]) if row["reviewer_member_id"] is not None else None,
                "notes": _normalize_text(row["notes"], max_length=2000),
                "auto_checks": auto_checks,
                "diagnostics": diagnostics,
                "coverage_fraction": float(row["coverage_fraction"]) if row["coverage_fraction"] is not None else None,
                "valid_pixel_count": int(row["valid_pixel_count"] or 0),
                "total_pixel_count": int(row["total_pixel_count"] or 0),
                "range_min": float(row["range_min"]) if row["range_min"] is not None else None,
                "range_max": float(row["range_max"]) if row["range_max"] is not None else None,
                "warning_summary": _normalize_text(row["warning_summary"], max_length=240),
                "severity": _normalize_text(row["severity"], max_length=24) or "none",
                "last_checked_at": int(row["last_checked_at"]),
            }
        )
    return results


def get_verification_result(review_id: int) -> dict[str, Any] | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT
                id,
                created_at,
                updated_at,
                model_id,
                variable_id,
                run_id,
                forecast_hour,
                auto_status,
                manual_status,
                benchmark_site,
                reviewer_name,
                reviewer_member_id,
                notes,
                auto_checks_json,
                coverage_fraction,
                valid_pixel_count,
                total_pixel_count,
                range_min,
                range_max,
                warning_summary,
                severity,
                diagnostics_json,
                last_checked_at
            FROM qa_reviews
            WHERE id = ?
            """,
            (int(review_id),),
        ).fetchone()
    if row is None:
        return None

    auto_checks = {}
    diagnostics = {}
    if row["auto_checks_json"]:
        try:
            parsed = json.loads(str(row["auto_checks_json"]))
            if isinstance(parsed, dict):
                auto_checks = parsed
        except json.JSONDecodeError:
            auto_checks = {}
    if row["diagnostics_json"]:
        try:
            parsed = json.loads(str(row["diagnostics_json"]))
            if isinstance(parsed, dict):
                diagnostics = parsed
        except json.JSONDecodeError:
            diagnostics = {}

    return {
        "id": int(row["id"]),
        "created_at": int(row["created_at"]),
        "updated_at": int(row["updated_at"]),
        "model_id": str(row["model_id"]),
        "variable_id": str(row["variable_id"]),
        "run_id": str(row["run_id"]),
        "forecast_hour": int(row["forecast_hour"]),
        "auto_status": str(row["auto_status"]),
        "manual_status": str(row["manual_status"]),
        "benchmark_site": _normalize_text(row["benchmark_site"], max_length=120),
        "reviewer_name": _normalize_text(row["reviewer_name"], max_length=120),
        "reviewer_member_id": int(row["reviewer_member_id"]) if row["reviewer_member_id"] is not None else None,
        "notes": _normalize_text(row["notes"], max_length=2000),
        "auto_checks": auto_checks,
        "diagnostics": diagnostics,
        "coverage_fraction": float(row["coverage_fraction"]) if row["coverage_fraction"] is not None else None,
        "valid_pixel_count": int(row["valid_pixel_count"] or 0),
        "total_pixel_count": int(row["total_pixel_count"] or 0),
        "range_min": float(row["range_min"]) if row["range_min"] is not None else None,
        "range_max": float(row["range_max"]) if row["range_max"] is not None else None,
        "warning_summary": _normalize_text(row["warning_summary"], max_length=240),
        "severity": _normalize_text(row["severity"], max_length=24) or "none",
        "last_checked_at": int(row["last_checked_at"]),
    }


def update_verification_review(
    *,
    review_id: int,
    manual_status: str,
    benchmark_site: str | None,
    notes: str | None,
    reviewer_name: str | None,
    reviewer_member_id: int | None,
) -> dict[str, Any] | None:
    now = int(time.time())
    normalized_manual_status = _normalize_manual_status(manual_status)
    with _connect() as conn:
        conn.execute(
            """
            UPDATE qa_reviews
            SET
                updated_at = ?,
                manual_status = ?,
                benchmark_site = ?,
                notes = ?,
                reviewer_name = ?,
                reviewer_member_id = ?
            WHERE id = ?
            """,
            (
                now,
                normalized_manual_status,
                _normalize_text(benchmark_site, max_length=120),
                _normalize_text(notes, max_length=2000),
                _normalize_text(reviewer_name, max_length=120),
                reviewer_member_id,
                int(review_id),
            ),
        )
        if conn.total_changes <= 0:
            return None

    return get_verification_result(int(review_id))


def record_perf_event(payload: dict[str, Any], *, member_id: int | None = None) -> None:
    event_name = _normalize_text(payload.get("event_name") or payload.get("name"), max_length=64)
    if event_name not in ALLOWED_PERF_EVENT_NAMES:
        raise ValueError("Unsupported performance event")

    duration_ms = float(payload.get("duration_ms"))
    if duration_ms < 0 or duration_ms > 600000:
        raise ValueError("Invalid performance duration")

    created_at = int(time.time())
    session_id = _normalize_text(payload.get("session_id"), max_length=128) or "anonymous"

    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO perf_events (
                created_at,
                session_id,
                member_id,
                event_name,
                duration_ms,
                model_id,
                variable_id,
                run_id,
                region_id,
                forecast_hour,
                device_type,
                viewport_bucket,
                page,
                meta_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                session_id,
                member_id,
                event_name,
                duration_ms,
                _normalize_text(payload.get("model_id"), max_length=32),
                _normalize_text(payload.get("variable_id"), max_length=64),
                _normalize_text(payload.get("run_id"), max_length=32),
                _normalize_text(payload.get("region_id"), max_length=32),
                _normalize_forecast_hour(payload.get("forecast_hour")),
                _normalize_text(payload.get("device_type"), max_length=24),
                _normalize_text(payload.get("viewport_bucket"), max_length=24),
                _normalize_text(payload.get("page"), max_length=120),
                _serialize_meta(payload.get("meta")),
            ),
        )


def record_usage_event(payload: dict[str, Any], *, member_id: int | None = None) -> None:
    event_name = _normalize_text(payload.get("event_name") or payload.get("name"), max_length=64)
    if event_name not in ALLOWED_USAGE_EVENT_NAMES:
        raise ValueError("Unsupported usage event")

    created_at = int(time.time())
    session_id = _normalize_text(payload.get("session_id"), max_length=128) or "anonymous"

    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO usage_events (
                created_at,
                session_id,
                member_id,
                event_name,
                model_id,
                variable_id,
                run_id,
                region_id,
                forecast_hour,
                device_type,
                viewport_bucket,
                page,
                meta_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                session_id,
                member_id,
                event_name,
                _normalize_text(payload.get("model_id"), max_length=32),
                _normalize_text(payload.get("variable_id"), max_length=64),
                _normalize_text(payload.get("run_id"), max_length=32),
                _normalize_text(payload.get("region_id"), max_length=32),
                _normalize_forecast_hour(payload.get("forecast_hour")),
                _normalize_text(payload.get("device_type"), max_length=24),
                _normalize_text(payload.get("viewport_bucket"), max_length=24),
                _normalize_text(payload.get("page"), max_length=120),
                _serialize_meta(payload.get("meta")),
            ),
        )


def _compute_percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    position = max(0.0, min(1.0, percentile)) * (len(ordered) - 1)
    lower_index = int(position)
    upper_index = min(len(ordered) - 1, lower_index + 1)
    weight = position - lower_index
    return ordered[lower_index] + (ordered[upper_index] - ordered[lower_index]) * weight


def _build_perf_filters(
    *,
    since_ts: int,
    metric: str | None = None,
    device_type: str | None = None,
    model_id: str | None = None,
    variable_id: str | None = None,
) -> tuple[str, list[Any]]:
    clauses = ["created_at >= ?"]
    params: list[Any] = [since_ts]
    if metric:
        clauses.append("event_name = ?")
        params.append(metric)
    if device_type:
        clauses.append("device_type = ?")
        params.append(device_type)
    if model_id:
        clauses.append("model_id = ?")
        params.append(model_id)
    if variable_id:
        clauses.append("variable_id = ?")
        params.append(variable_id)
    return " WHERE " + " AND ".join(clauses), params


def _metric_summary(values: Iterable[float], *, target_ms: float | None = None) -> dict[str, Any]:
    samples = [float(value) for value in values]
    if not samples:
        return {
            "count": 0,
            "avg_ms": None,
            "min_ms": None,
            "max_ms": None,
            "p50_ms": None,
            "p95_ms": None,
            "target_ms": target_ms,
        }
    avg_ms = sum(samples) / len(samples)
    return {
        "count": len(samples),
        "avg_ms": round(avg_ms, 1),
        "min_ms": round(min(samples), 1),
        "max_ms": round(max(samples), 1),
        "p50_ms": round(_compute_percentile(samples, 0.50) or 0.0, 1),
        "p95_ms": round(_compute_percentile(samples, 0.95) or 0.0, 1),
        "target_ms": target_ms,
    }


def get_perf_summary(
    *,
    since_ts: int,
    device_type: str | None = None,
    model_id: str | None = None,
    variable_id: str | None = None,
) -> dict[str, Any]:
    where_sql, params = _build_perf_filters(
        since_ts=since_ts,
        device_type=device_type,
        model_id=model_id,
        variable_id=variable_id,
    )
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT event_name, duration_ms
            FROM perf_events
            {where_sql}
            ORDER BY created_at ASC
            """,
            params,
        ).fetchall()

    values_by_metric: dict[str, list[float]] = {name: [] for name in ALLOWED_PERF_EVENT_NAMES}
    for row in rows:
        values_by_metric[str(row["event_name"])].append(float(row["duration_ms"]))

    return {
        "metrics": {
            metric_name: _metric_summary(values, target_ms=PERF_TARGETS_MS.get(metric_name))
            for metric_name, values in sorted(values_by_metric.items())
        }
    }


def get_perf_timeseries(
    *,
    since_ts: int,
    metric: str,
    bucket: str,
    device_type: str | None = None,
    model_id: str | None = None,
    variable_id: str | None = None,
) -> list[dict[str, Any]]:
    if metric not in ALLOWED_PERF_EVENT_NAMES:
        raise ValueError("Unsupported performance metric")
    if bucket not in {"hour", "day"}:
        raise ValueError("Unsupported timeseries bucket")

    bucket_expr = "%Y-%m-%dT%H:00:00Z" if bucket == "hour" else "%Y-%m-%dT00:00:00Z"
    where_sql, params = _build_perf_filters(
        since_ts=since_ts,
        metric=metric,
        device_type=device_type,
        model_id=model_id,
        variable_id=variable_id,
    )
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT strftime('{bucket_expr}', created_at, 'unixepoch') AS bucket_start,
                   duration_ms
            FROM perf_events
            {where_sql}
            ORDER BY created_at ASC
            """,
            params,
        ).fetchall()

    buckets: dict[str, list[float]] = {}
    for row in rows:
        key = str(row["bucket_start"])
        buckets.setdefault(key, []).append(float(row["duration_ms"]))

    return [
        {
            "bucket_start": bucket_start,
            **_metric_summary(values, target_ms=PERF_TARGETS_MS.get(metric)),
        }
        for bucket_start, values in sorted(buckets.items())
    ]


def get_perf_breakdown(
    *,
    since_ts: int,
    metric: str,
    breakdown_by: str,
    limit: int = 8,
    device_type: str | None = None,
    model_id: str | None = None,
    variable_id: str | None = None,
) -> list[dict[str, Any]]:
    if metric not in ALLOWED_PERF_EVENT_NAMES:
        raise ValueError("Unsupported performance metric")
    column_by_breakdown = {
        "model": "model_id",
        "variable": "variable_id",
        "device": "device_type",
    }
    column = column_by_breakdown.get(breakdown_by)
    if column is None:
        raise ValueError("Unsupported breakdown")

    where_sql, params = _build_perf_filters(
        since_ts=since_ts,
        metric=metric,
        device_type=device_type,
        model_id=model_id,
        variable_id=variable_id,
    )
    with _connect() as conn:
        rows = conn.execute(
            f"""
            SELECT COALESCE({column}, 'unknown') AS bucket_key,
                   duration_ms
            FROM perf_events
            {where_sql}
            ORDER BY created_at ASC
            """,
            params,
        ).fetchall()

    values_by_bucket: dict[str, list[float]] = {}
    for row in rows:
        key = str(row["bucket_key"] or "unknown")
        values_by_bucket.setdefault(key, []).append(float(row["duration_ms"]))

    ranked = sorted(
        values_by_bucket.items(),
        key=lambda item: (len(item[1]), item[0]),
        reverse=True,
    )[: max(1, limit)]

    return [
        {
            "key": key,
            **_metric_summary(values, target_ms=PERF_TARGETS_MS.get(metric)),
        }
        for key, values in ranked
    ]


def get_usage_summary(*, since_ts: int) -> dict[str, Any]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT event_name, COUNT(*) AS total
            FROM usage_events
            WHERE created_at >= ?
            GROUP BY event_name
            ORDER BY total DESC, event_name ASC
            """,
            (since_ts,),
        ).fetchall()
    return {
        "events": [
            {
                "event_name": str(row["event_name"]),
                "count": int(row["total"]),
            }
            for row in rows
        ]
    }

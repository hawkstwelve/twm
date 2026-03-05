from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.builder import pipeline as pipeline_module


def test_build_sidecar_defaults_to_full_quality() -> None:
    sidecar = pipeline_module.build_sidecar_json(
        model="hrrr",
        run_id="20260305_17z",
        var_id="snowfall_kuchera_total",
        fh=3,
        run_date=datetime(2026, 3, 5, 17, tzinfo=timezone.utc),
        colorize_meta={"kind": "continuous", "units": "in", "min": 0.0, "max": 10.0},
        var_spec={"type": "continuous", "range": [0.0, 10.0], "colors": ["#000000", "#ffffff"]},
    )

    assert sidecar["quality"] == "full"
    assert sidecar["quality_flags"] == []


def test_build_sidecar_writes_degraded_quality_flags() -> None:
    sidecar = pipeline_module.build_sidecar_json(
        model="hrrr",
        run_id="20260305_17z",
        var_id="snowfall_kuchera_total",
        fh=3,
        run_date=datetime(2026, 3, 5, 17, tzinfo=timezone.utc),
        colorize_meta={"kind": "continuous", "units": "in", "min": 0.0, "max": 10.0},
        var_spec={"type": "continuous", "range": [0.0, 10.0], "colors": ["#000000", "#ffffff"]},
        quality="degraded",
        quality_flags=["slr_fallback_10to1", "apcp_cumulative_fallback"],
    )

    assert sidecar["quality"] == "degraded"
    assert sidecar["quality_flags"] == ["slr_fallback_10to1", "apcp_cumulative_fallback"]

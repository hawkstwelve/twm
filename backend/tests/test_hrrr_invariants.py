from __future__ import annotations

import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.main import _serialize_model_capability
from app.models.hrrr import HRRR_MODEL


def test_hrrr_run_discovery_invariants() -> None:
    capabilities = HRRR_MODEL.capabilities
    assert capabilities is not None
    assert capabilities.run_discovery == {
        "probe_var_key": "tmp2m",
        "probe_enabled": True,
        "cycle_cadence_hours": 1,
        "probe_attempts": 4,
        "fallback_lag_hours": 2,
    }


def test_hrrr_target_fhs_invariants() -> None:
    assert HRRR_MODEL.target_fhs(0) == list(range(0, 49))
    assert HRRR_MODEL.target_fhs(6) == list(range(0, 49))
    assert HRRR_MODEL.target_fhs(12) == list(range(0, 49))
    assert HRRR_MODEL.target_fhs(18) == list(range(0, 49))
    assert HRRR_MODEL.target_fhs(1) == list(range(0, 19))
    assert HRRR_MODEL.target_fhs(23) == list(range(0, 19))


def test_hrrr_buildable_var_set_and_defaults_invariants() -> None:
    capabilities = HRRR_MODEL.capabilities
    assert capabilities is not None

    buildable_var_keys = {
        var_key
        for var_key, capability in capabilities.variable_catalog.items()
        if capability.buildable
    }
    assert buildable_var_keys == {
        "tmp2m",
        "dp2m",
        "tmp850",
        "snowfall_total",
        "precip_total",
        "wspd10m",
        "wgst10m",
        "radar_ptype",
    }

    assert capabilities.ui_defaults["default_var_key"] == "radar_ptype"
    assert capabilities.ui_defaults["default_run"] == "latest"
    assert capabilities.canonical_region == "conus"
    assert capabilities.grid_meters_by_region == {
        "conus": 3000.0,
        "pnw": 3000.0,
    }


def test_hrrr_capabilities_schema_snapshot_invariants() -> None:
    capabilities = HRRR_MODEL.capabilities
    assert capabilities is not None
    payload = _serialize_model_capability("hrrr", capabilities)

    assert set(payload.keys()) == {
        "model_id",
        "name",
        "product",
        "canonical_region",
        "defaults",
        "constraints",
        "run_discovery",
        "variables",
    }
    assert payload["model_id"] == "hrrr"
    assert payload["name"] == "HRRR"
    assert payload["product"] == "sfc"
    assert payload["canonical_region"] == "conus"

    tmp2m = payload["variables"]["tmp2m"]
    assert set(tmp2m.keys()) == {
        "var_key",
        "display_name",
        "kind",
        "units",
        "order",
        "default_fh",
        "buildable",
        "color_map_id",
        "constraints",
        "derived",
        "derive_strategy_id",
    }
    assert tmp2m["var_key"] == "tmp2m"
    assert tmp2m["kind"] == "continuous"
    assert tmp2m["buildable"] is True

    radar_ptype = payload["variables"]["radar_ptype"]
    assert radar_ptype["buildable"] is True
    assert radar_ptype["derived"] is True
    assert radar_ptype["derive_strategy_id"] == "radar_ptype_combo"

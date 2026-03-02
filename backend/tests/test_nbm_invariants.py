from __future__ import annotations

import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.main import _serialize_model_capability
from app.models.nbm import NBM_MODEL


def test_nbm_target_fhs_invariants() -> None:
    assert NBM_MODEL.target_fhs(0) == list(range(0, 121, 6))
    assert NBM_MODEL.target_fhs(6) == list(range(0, 121, 6))
    assert NBM_MODEL.target_fhs(12) == list(range(0, 121, 6))
    assert NBM_MODEL.target_fhs(18) == list(range(0, 121, 6))


def test_nbm_buildable_var_set_and_defaults_invariants() -> None:
    capabilities = NBM_MODEL.capabilities
    assert capabilities is not None

    buildable_var_keys = {
        var_key
        for var_key, capability in capabilities.variable_catalog.items()
        if capability.buildable
    }
    assert buildable_var_keys == {
        "tmp2m",
        "precip_total",
        "snowfall_total",
        "wspd10m",
    }

    assert capabilities.ui_defaults["default_var_key"] == "tmp2m"
    assert capabilities.ui_defaults["default_run"] == "latest"
    assert capabilities.canonical_region == "conus"
    assert capabilities.grid_meters_by_region == {
        "conus": 13000.0,
        "pnw": 13000.0,
    }
    assert capabilities.run_discovery == {
        "probe_var_key": "tmp2m",
        "probe_enabled": True,
        "probe_attempts": 4,
        "cycle_cadence_hours": 6,
        "fallback_lag_hours": 5,
    }


def test_nbm_capabilities_schema_snapshot_invariants() -> None:
    capabilities = NBM_MODEL.capabilities
    assert capabilities is not None
    payload = _serialize_model_capability("nbm", capabilities)

    assert payload["model_id"] == "nbm"
    assert payload["name"] == "NBM"
    assert payload["product"] == "co"
    assert payload["canonical_region"] == "conus"

    tmp2m = payload["variables"]["tmp2m"]
    assert tmp2m["buildable"] is True
    assert tmp2m["derived"] is False
    assert tmp2m["kind"] == "continuous"
    assert tmp2m["units"] == "F"
    assert tmp2m["display_name"] == "Surface Temp"
    assert tmp2m["order"] == 1

    precip_total = payload["variables"]["precip_total"]
    assert precip_total["buildable"] is True
    assert precip_total["derived"] is True
    assert precip_total["derive_strategy_id"] == "precip_total_cumulative"
    assert precip_total["kind"] == "continuous"
    assert precip_total["units"] == "in"
    assert precip_total["default_fh"] == 6
    assert precip_total["constraints"] == {"min_fh": 6}
    assert precip_total["display_name"] == "Total Precip"
    assert precip_total["order"] == 2

    snowfall_total = payload["variables"]["snowfall_total"]
    assert snowfall_total["buildable"] is True
    assert snowfall_total["derived"] is True
    assert snowfall_total["derive_strategy_id"] == "snowfall_total_10to1_cumulative"
    assert snowfall_total["kind"] == "continuous"
    assert snowfall_total["units"] == "in"
    assert snowfall_total["default_fh"] == 6
    assert snowfall_total["constraints"] == {"min_fh": 6}
    assert snowfall_total["display_name"] == "Total Snowfall (10:1)"
    assert snowfall_total["order"] == 3

    wspd10m = payload["variables"]["wspd10m"]
    assert wspd10m["buildable"] is True
    assert wspd10m["derived"] is True
    assert wspd10m["derive_strategy_id"] == "wspd10m"
    assert wspd10m["kind"] == "continuous"
    assert wspd10m["units"] == "mph"
    assert wspd10m["display_name"] == "10m Wind Speed"
    assert wspd10m["order"] == 4

    u10 = payload["variables"]["10u"]
    assert u10["buildable"] is False

    v10 = payload["variables"]["10v"]
    assert v10["buildable"] is False

    si10 = payload["variables"]["10si"]
    assert si10["buildable"] is False

    apcp_step = payload["variables"]["apcp_step"]
    assert apcp_step["buildable"] is False

    csnow = payload["variables"]["csnow"]
    assert csnow["buildable"] is False

    snowfall_spec = NBM_MODEL.get_var("snowfall_total")
    assert snowfall_spec is not None
    assert snowfall_spec.selectors.hints["apcp_component"] == "apcp_step"
    assert snowfall_spec.selectors.hints["snow_component"] == "csnow"
    assert snowfall_spec.selectors.hints["step_hours"] == "6"

    apcp_component_spec = NBM_MODEL.get_var("apcp_step")
    assert apcp_component_spec is not None
    assert apcp_component_spec.selectors.search == [":APCP:surface:"]
    assert apcp_component_spec.selectors.filter_by_keys["shortName"] == "apcp"

    csnow_component_spec = NBM_MODEL.get_var("csnow")
    assert csnow_component_spec is not None
    assert csnow_component_spec.selectors.search == [":CSNOW:surface:"]
    assert csnow_component_spec.selectors.filter_by_keys["shortName"] == "csnow"


def test_nbm_aliases_normalize() -> None:
    assert NBM_MODEL.normalize_var_id("tmp2m") == "tmp2m"
    assert NBM_MODEL.normalize_var_id("t2m") == "tmp2m"
    assert NBM_MODEL.normalize_var_id("2t") == "tmp2m"
    assert NBM_MODEL.normalize_var_id("precip_total") == "precip_total"
    assert NBM_MODEL.normalize_var_id("apcp") == "precip_total"
    assert NBM_MODEL.normalize_var_id("qpf") == "precip_total"
    assert NBM_MODEL.normalize_var_id("snowfall_total") == "snowfall_total"
    assert NBM_MODEL.normalize_var_id("asnow") == "snowfall_total"
    assert NBM_MODEL.normalize_var_id("total_snow") == "snowfall_total"
    assert NBM_MODEL.normalize_var_id("wspd10m") == "wspd10m"
    assert NBM_MODEL.normalize_var_id("wind10m") == "10si"
    assert NBM_MODEL.normalize_var_id("10si") == "10si"
    assert NBM_MODEL.normalize_var_id("u10") == "10u"
    assert NBM_MODEL.normalize_var_id("v10") == "10v"

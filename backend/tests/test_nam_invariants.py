from __future__ import annotations

import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.main import _serialize_model_capability
from app.models.nam import NAM_MODEL


def test_nam_target_fhs_invariants() -> None:
    assert NAM_MODEL.target_fhs(0) == list(range(0, 61))
    assert NAM_MODEL.target_fhs(6) == list(range(0, 61))
    assert NAM_MODEL.target_fhs(12) == list(range(0, 61))
    assert NAM_MODEL.target_fhs(18) == list(range(0, 61))


def test_nam_buildable_var_set_and_defaults_invariants() -> None:
    capabilities = NAM_MODEL.capabilities
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
        "wspd10m",
        "wgst10m",
        "precip_total",
        "snowfall_total",
        "radar_ptype",
    }

    assert capabilities.ui_defaults["default_var_key"] == "tmp2m"
    assert capabilities.ui_defaults["default_run"] == "latest"
    assert capabilities.canonical_region == "conus"
    assert capabilities.grid_meters_by_region == {
        "conus": 5000.0,
        "pnw": 5000.0,
    }


def test_nam_capabilities_schema_snapshot_invariants() -> None:
    capabilities = NAM_MODEL.capabilities
    assert capabilities is not None
    payload = _serialize_model_capability("nam", capabilities)

    assert payload["model_id"] == "nam"
    assert payload["name"] == "NAM"
    assert payload["product"] == "conusnest.hiresf"
    assert payload["canonical_region"] == "conus"

    tmp2m = payload["variables"]["tmp2m"]
    assert tmp2m["buildable"] is True
    assert tmp2m["derived"] is False
    assert tmp2m["kind"] == "continuous"
    assert tmp2m["units"] == "F"

    dp2m = payload["variables"]["dp2m"]
    assert dp2m["buildable"] is True
    assert dp2m["derived"] is False
    assert dp2m["kind"] == "continuous"
    assert dp2m["units"] == "F"

    tmp850 = payload["variables"]["tmp850"]
    assert tmp850["buildable"] is True
    assert tmp850["derived"] is False
    assert tmp850["kind"] == "continuous"
    assert tmp850["units"] == "C"

    wspd10m = payload["variables"]["wspd10m"]
    assert wspd10m["buildable"] is True
    assert wspd10m["derived"] is True
    assert wspd10m["derive_strategy_id"] == "wspd10m"
    assert wspd10m["kind"] == "continuous"
    assert wspd10m["units"] == "mph"

    wgst10m = payload["variables"]["wgst10m"]
    assert wgst10m["buildable"] is True
    assert wgst10m["derived"] is False
    assert wgst10m["kind"] == "continuous"
    assert wgst10m["units"] == "mph"

    precip_total = payload["variables"]["precip_total"]
    assert precip_total["buildable"] is True
    assert precip_total["derived"] is True
    assert precip_total["derive_strategy_id"] == "precip_total_cumulative"
    assert precip_total["kind"] == "continuous"
    assert precip_total["units"] == "in"
    assert precip_total["default_fh"] == 1
    assert precip_total["constraints"] == {"min_fh": 1}

    snowfall_total = payload["variables"]["snowfall_total"]
    assert snowfall_total["buildable"] is True
    assert snowfall_total["derived"] is True
    assert snowfall_total["derive_strategy_id"] == "snowfall_total_10to1_cumulative"
    assert snowfall_total["kind"] == "continuous"
    assert snowfall_total["units"] == "in"
    assert snowfall_total["default_fh"] == 1
    assert snowfall_total["constraints"] == {"min_fh": 1}

    radar_ptype = payload["variables"]["radar_ptype"]
    assert radar_ptype["buildable"] is True
    assert radar_ptype["derived"] is True
    assert radar_ptype["derive_strategy_id"] == "radar_ptype_combo"
    assert radar_ptype["kind"] == "discrete"
    assert radar_ptype["units"] == "dBZ"
    assert radar_ptype["default_fh"] == 1
    radar_ptype_spec = NAM_MODEL.get_var("radar_ptype")
    assert radar_ptype_spec is not None
    assert radar_ptype_spec.selectors.hints["min_visible_dbz"] == "15.0"
    assert radar_ptype_spec.selectors.hints["min_mask_value"] == "0.5"
    assert radar_ptype_spec.selectors.hints["despeckle_min_neighbors"] == "3"

    u10 = payload["variables"]["10u"]
    assert u10["buildable"] is False

    v10 = payload["variables"]["10v"]
    assert v10["buildable"] is False

    si10 = payload["variables"]["10si"]
    assert si10["buildable"] is False

    apcp_step = payload["variables"]["apcp_step"]
    assert apcp_step["buildable"] is False

    refc = payload["variables"]["refc"]
    assert refc["buildable"] is False

    crain = payload["variables"]["crain"]
    assert crain["buildable"] is False

    csnow = payload["variables"]["csnow"]
    assert csnow["buildable"] is False

    cicep = payload["variables"]["cicep"]
    assert cicep["buildable"] is False

    cfrzr = payload["variables"]["cfrzr"]
    assert cfrzr["buildable"] is False


def test_nam_aliases_normalize() -> None:
    assert NAM_MODEL.normalize_var_id("tmp2m") == "tmp2m"
    assert NAM_MODEL.normalize_var_id("t2m") == "tmp2m"
    assert NAM_MODEL.normalize_var_id("2t") == "tmp2m"
    assert NAM_MODEL.normalize_var_id("dp2m") == "dp2m"
    assert NAM_MODEL.normalize_var_id("d2m") == "dp2m"
    assert NAM_MODEL.normalize_var_id("2d") == "dp2m"
    assert NAM_MODEL.normalize_var_id("tmp850") == "tmp850"
    assert NAM_MODEL.normalize_var_id("t850") == "tmp850"
    assert NAM_MODEL.normalize_var_id("temp850") == "tmp850"
    assert NAM_MODEL.normalize_var_id("wgst10m") == "wgst10m"
    assert NAM_MODEL.normalize_var_id("gust") == "wgst10m"
    assert NAM_MODEL.normalize_var_id("gust10m") == "wgst10m"
    assert NAM_MODEL.normalize_var_id("precip_total") == "precip_total"
    assert NAM_MODEL.normalize_var_id("apcp") == "precip_total"
    assert NAM_MODEL.normalize_var_id("qpf") == "precip_total"
    assert NAM_MODEL.normalize_var_id("snowfall_total") == "snowfall_total"
    assert NAM_MODEL.normalize_var_id("asnow") == "snowfall_total"
    assert NAM_MODEL.normalize_var_id("snow10") == "snowfall_total"
    assert NAM_MODEL.normalize_var_id("refc") == "refc"
    assert NAM_MODEL.normalize_var_id("cref") == "refc"
    assert NAM_MODEL.normalize_var_id("radar_ptype") == "radar_ptype"
    assert NAM_MODEL.normalize_var_id("radarptype") == "radar_ptype"
    assert NAM_MODEL.normalize_var_id("u10") == "10u"
    assert NAM_MODEL.normalize_var_id("v10") == "10v"
    assert NAM_MODEL.normalize_var_id("10si") == "10si"
    assert NAM_MODEL.normalize_var_id("wind10m") == "10si"
    assert NAM_MODEL.normalize_var_id("wspd10m") == "wspd10m"

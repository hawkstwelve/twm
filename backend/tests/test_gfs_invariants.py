from __future__ import annotations

import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.main import _serialize_model_capability
from app.models.gfs import GFS_MODEL


def test_gfs_target_fhs_invariants() -> None:
    assert GFS_MODEL.target_fhs(0) == list(range(0, 121, 6))
    assert GFS_MODEL.target_fhs(6) == list(range(0, 121, 6))
    assert GFS_MODEL.target_fhs(12) == list(range(0, 121, 6))
    assert GFS_MODEL.target_fhs(18) == list(range(0, 121, 6))


def test_gfs_buildable_var_set_and_defaults_invariants() -> None:
    capabilities = GFS_MODEL.capabilities
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
        "precip_ptype",
        "precip_total",
        "snowfall_total",
    }

    assert capabilities.ui_defaults["default_var_key"] == "tmp2m"
    assert capabilities.ui_defaults["default_run"] == "latest"
    assert capabilities.canonical_region == "conus"
    assert capabilities.grid_meters_by_region == {
        "conus": 25000.0,
        "pnw": 25000.0,
    }


def test_gfs_capabilities_schema_snapshot_invariants() -> None:
    capabilities = GFS_MODEL.capabilities
    assert capabilities is not None
    payload = _serialize_model_capability("gfs", capabilities)

    precip_ptype = payload["variables"]["precip_ptype"]
    assert precip_ptype["buildable"] is True
    assert precip_ptype["derived"] is True
    assert precip_ptype["derive_strategy_id"] == "precip_ptype_blend"
    assert precip_ptype["units"] == "in/hr"

    precip_total = payload["variables"]["precip_total"]
    assert precip_total["buildable"] is True
    assert precip_total["derived"] is True
    assert precip_total["derive_strategy_id"] == "precip_total_cumulative"
    assert precip_total["kind"] == "continuous"
    assert precip_total["constraints"]["min_fh"] == 6

    tmp850 = payload["variables"]["tmp850"]
    assert tmp850["buildable"] is True
    assert tmp850["derived"] is False
    assert tmp850["units"] == "C"

    dp2m = payload["variables"]["dp2m"]
    assert dp2m["buildable"] is True
    assert dp2m["derived"] is False
    assert dp2m["units"] == "F"

    wgst10m = payload["variables"]["wgst10m"]
    assert wgst10m["buildable"] is True
    assert wgst10m["derived"] is False
    assert wgst10m["units"] == "mph"

    snowfall_total = payload["variables"]["snowfall_total"]
    assert snowfall_total["buildable"] is True
    assert snowfall_total["derived"] is False
    assert snowfall_total["units"] == "in"
    assert snowfall_total["constraints"]["min_fh"] == 6
    assert snowfall_total["default_fh"] == 6

    qpf6h = payload["variables"]["qpf6h"]
    assert qpf6h["buildable"] is False


def test_gfs_precip_total_aliases_normalize() -> None:
    assert GFS_MODEL.normalize_var_id("apcp") == "precip_total"
    assert GFS_MODEL.normalize_var_id("qpf") == "precip_total"
    assert GFS_MODEL.normalize_var_id("total_precip") == "precip_total"


def test_gfs_temp850_and_gust_aliases_normalize() -> None:
    assert GFS_MODEL.normalize_var_id("tmp850") == "tmp850"
    assert GFS_MODEL.normalize_var_id("t850") == "tmp850"
    assert GFS_MODEL.normalize_var_id("t850mb") == "tmp850"
    assert GFS_MODEL.normalize_var_id("wgst10m") == "wgst10m"
    assert GFS_MODEL.normalize_var_id("gust") == "wgst10m"
    assert GFS_MODEL.normalize_var_id("gust10m") == "wgst10m"


def test_gfs_dewpoint_and_snow_aliases_normalize() -> None:
    assert GFS_MODEL.normalize_var_id("dp2m") == "dp2m"
    assert GFS_MODEL.normalize_var_id("d2m") == "dp2m"
    assert GFS_MODEL.normalize_var_id("2d") == "dp2m"
    assert GFS_MODEL.normalize_var_id("dpt2m") == "dp2m"

    assert GFS_MODEL.normalize_var_id("snowfall_total") == "snowfall_total"
    assert GFS_MODEL.normalize_var_id("asnow") == "snowfall_total"
    assert GFS_MODEL.normalize_var_id("snow10") == "snowfall_total"

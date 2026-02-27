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
        "wspd10m",
        "precip_ptype",
        "precip_total",
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

    qpf6h = payload["variables"]["qpf6h"]
    assert qpf6h["buildable"] is False


def test_gfs_precip_total_aliases_normalize() -> None:
    assert GFS_MODEL.normalize_var_id("apcp") == "precip_total"
    assert GFS_MODEL.normalize_var_id("qpf") == "precip_total"
    assert GFS_MODEL.normalize_var_id("total_precip") == "precip_total"

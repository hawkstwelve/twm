from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
from rasterio.crs import CRS
from rasterio.transform import Affine

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.builder import derive as derive_module


class _Plugin:
    def normalize_var_id(self, var_key: str) -> str:
        return str(var_key)

    def get_var_capability(self, var_key: str):
        del var_key
        return None

    def get_var(self, var_key: str):
        search_by_var = {
            "apcp_step": [":APCP:surface:[0-9]+-[0-9]+ hour acc[^:]*:$"],
            "tmp850": [":TMP:850 mb:"],
            "csnow": [":CSNOW:surface:"],
            "crain": [":CRAIN:surface:"],
            "cicep": [":CICEP:surface:"],
            "cfrzr": [":CFRZR:surface:"],
        }
        search = search_by_var.get(str(var_key))
        if search is None:
            return None
        return SimpleNamespace(
            selectors=SimpleNamespace(
                search=search,
                filter_by_keys={},
                hints={},
            )
        )


def _kuchera_var_spec() -> SimpleNamespace:
    return SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
                "kuchera_levels_hpa": "850",
                "kuchera_require_rh": "false",
                "kuchera_min_levels": "1",
                "kuchera_use_ptype_gate": "true",
            }
        )
    )


def test_ptype_scaling_detects_0_to_1_and_0_to_100() -> None:
    frac_data = np.array([[0.0, 0.25], [0.5, 1.0]], dtype=np.float32)
    pct_data = np.array([[0.0, 25.0], [50.0, 100.0]], dtype=np.float32)

    frac_norm = derive_module._normalize_ptype_probability(frac_data)
    pct_norm = derive_module._normalize_ptype_probability(pct_data)

    np.testing.assert_allclose(frac_norm, frac_data, rtol=1e-6, atol=1e-6)
    np.testing.assert_allclose(pct_norm, frac_data, rtol=1e-6, atol=1e-6)


def test_apcp_frozen_is_never_greater_than_apcp_step() -> None:
    apcp_step = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
    frozen_frac = np.array([[0.0, 0.5], [1.0, 0.25]], dtype=np.float32)

    apcp_frozen = derive_module._apply_kuchera_ptype_gate(apcp_step, frozen_frac)

    assert np.all(apcp_frozen <= apcp_step + 1e-6)
    ones_mask = frozen_frac == 1.0
    assert np.allclose(apcp_frozen[ones_mask], apcp_step[ones_mask], rtol=0.0, atol=1e-6)


def test_kuchera_ptype_gate_masks_rain_only_step(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    apcp = np.array([[2.0, 1.0], [0.5, 3.0]], dtype=np.float32)
    temp_850 = np.full((2, 2), -10.0, dtype=np.float32)
    zeros = np.zeros((2, 2), dtype=np.float32)
    ones = np.ones((2, 2), dtype=np.float32)
    exact_apcp_pattern = ":APCP:surface:0-1 hour acc fcst:"

    def _fake_fetch_variable(
        *,
        model_id,
        product,
        search_pattern,
        run_date,
        fh,
        herbie_kwargs=None,
        return_meta=False,
    ):
        del model_id, product, run_date, fh, herbie_kwargs
        pattern = str(search_pattern)
        if pattern == exact_apcp_pattern:
            meta = {"inventory_line": exact_apcp_pattern, "search_pattern": pattern}
            return (apcp, crs, transform, meta) if return_meta else (apcp, crs, transform)
        if pattern == ":TMP:850 mb:":
            meta = {"inventory_line": "", "search_pattern": pattern}
            return (temp_850, crs, transform, meta) if return_meta else (temp_850, crs, transform)
        if pattern == ":CSNOW:surface:":
            meta = {"inventory_line": "", "search_pattern": pattern}
            return (zeros, crs, transform, meta) if return_meta else (zeros, crs, transform)
        if pattern == ":CRAIN:surface:":
            meta = {"inventory_line": "", "search_pattern": pattern}
            return (ones, crs, transform, meta) if return_meta else (ones, crs, transform)
        if pattern == ":CICEP:surface:":
            meta = {"inventory_line": "", "search_pattern": pattern}
            return (zeros, crs, transform, meta) if return_meta else (zeros, crs, transform)
        if pattern == ":CFRZR:surface:":
            meta = {"inventory_line": "", "search_pattern": pattern}
            return (zeros, crs, transform, meta) if return_meta else (zeros, crs, transform)
        raise AssertionError(f"unexpected search pattern: {pattern}")

    monkeypatch.setattr(derive_module, "fetch_variable", _fake_fetch_variable)
    monkeypatch.setattr(
        derive_module,
        "_kuchera_inventory_lines",
        lambda *, model_id, product, run_date, fh, search_pattern: [exact_apcp_pattern],
    )
    monkeypatch.setattr(
        derive_module,
        "_resolve_cumulative_step_fhs",
        lambda *, hints, fh, default_step_hours=6: [1],
    )

    data, _, _ = derive_module._derive_snowfall_kuchera_total_cumulative(
        model_id="hrrr",
        var_key="snowfall_kuchera_total",
        product="sfc",
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=1,
        var_spec_model=_kuchera_var_spec(),
        var_capability=None,
        model_plugin=_Plugin(),
    )

    np.testing.assert_allclose(data, np.zeros((2, 2), dtype=np.float32), rtol=1e-6, atol=1e-6)

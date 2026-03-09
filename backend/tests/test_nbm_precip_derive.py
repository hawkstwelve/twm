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


def test_precip_total_mixed_cadence_uses_hourly_then_6hourly_steps(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    seen_fhs: list[int] = []

    def _fake_fetch_component(**kwargs):
        fh = int(kwargs["fh"])
        var_key = str(kwargs["var_key"])
        return_meta = bool(kwargs.get("return_meta", False))
        assert var_key == "apcp_step"
        seen_fhs.append(fh)
        data = np.ones((2, 2), dtype=np.float32)
        if return_meta:
            return data, crs, transform, {"search_pattern": "", "inventory_line": "", "fh": fh}
        return data, crs, transform

    monkeypatch.setattr(derive_module, "_fetch_component", _fake_fetch_component)
    monkeypatch.setattr(derive_module, "_kuchera_inventory_lines", lambda **kwargs: [])

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
                "step_transition_fh": "36",
                "step_hours_after_fh": "6",
            }
        )
    )
    var_capability = SimpleNamespace(conversion="kgm2_to_in")

    data, out_crs, out_transform = derive_module._derive_precip_total_cumulative(
        model_id="nbm",
        var_key="precip_total",
        product="co",
        run_date=datetime(2026, 3, 2, 0, 0),
        fh=42,
        var_spec_model=var_spec_model,
        var_capability=var_capability,
        model_plugin=object(),
    )

    assert out_crs == crs
    assert out_transform == transform
    assert seen_fhs == [*range(1, 37), 42]
    # 37 steps of 1 kg/m^2 (== 1 mm) converted to inches.
    expected_inches = 37.0 * 0.03937007874015748
    np.testing.assert_allclose(data, np.full((2, 2), expected_inches, dtype=np.float32), rtol=1e-6, atol=1e-6)


def test_precip_total_inventory_cumulative_differencing_prevents_gfs_overcount(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    fetch_patterns: list[str] = []

    def _fake_fetch_variable(*, model_id, product, search_pattern, run_date, fh, herbie_kwargs=None, return_meta=False):
        del model_id, product, run_date, herbie_kwargs
        pattern = str(search_pattern)
        fetch_patterns.append(f"{int(fh)}:{pattern}")
        data_by_pattern = {
            ":APCP:surface:0-3 hour acc fcst:": np.full((2, 2), 3.0, dtype=np.float32),
            ":APCP:surface:0-6 hour acc fcst:": np.full((2, 2), 6.0, dtype=np.float32),
        }
        data = data_by_pattern[pattern]
        meta = {"inventory_line": pattern, "search_pattern": pattern, "fh": int(fh)}
        if return_meta:
            return data, crs, transform, meta
        return data, crs, transform

    def _fake_inventory_lines(*, model_id, product, run_date, fh, search_pattern):
        del model_id, product, run_date, search_pattern
        return {
            3: [":APCP:surface:0-3 hour acc fcst:"],
            6: [":APCP:surface:0-6 hour acc fcst:"],
        }[int(fh)]

    monkeypatch.setattr(derive_module, "fetch_variable", _fake_fetch_variable)
    monkeypatch.setattr(derive_module, "_kuchera_inventory_lines", _fake_inventory_lines)
    monkeypatch.setattr(derive_module, "_fetch_component", lambda **kwargs: (_ for _ in ()).throw(AssertionError("selector fallback should not be used")))

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "3",
            }
        )
    )
    var_capability = SimpleNamespace(conversion="kgm2_to_in")

    data, out_crs, out_transform = derive_module._derive_precip_total_cumulative(
        model_id="gfs",
        var_key="precip_total",
        product="pgrb2.0p25",
        run_date=datetime(2026, 3, 2, 0, 0),
        fh=6,
        var_spec_model=var_spec_model,
        var_capability=var_capability,
        model_plugin=object(),
    )

    assert out_crs == crs
    assert out_transform == transform
    assert fetch_patterns == [
        "3::APCP:surface:0-3 hour acc fcst:",
        "6::APCP:surface:0-6 hour acc fcst:",
    ]
    expected_inches = 6.0 * 0.03937007874015748
    np.testing.assert_allclose(data, np.full((2, 2), expected_inches, dtype=np.float32), rtol=1e-6, atol=1e-6)


def test_precip_total_nbm_late_step_prefers_exact_36_to_42_window(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    fetch_patterns: list[str] = []

    def _fake_fetch_variable(*, model_id, product, search_pattern, run_date, fh, herbie_kwargs=None, return_meta=False):
        del model_id, product, run_date, herbie_kwargs
        pattern = str(search_pattern)
        fetch_patterns.append(f"{int(fh)}:{pattern}")
        data_by_pattern = {
            ":APCP:surface:35-36 hour acc fcst:": np.full((2, 2), 1.0, dtype=np.float32),
            ":APCP:surface:36-42 hour acc fcst:": np.full((2, 2), 6.0, dtype=np.float32),
        }
        data = data_by_pattern[pattern]
        meta = {"inventory_line": pattern, "search_pattern": pattern, "fh": int(fh)}
        if return_meta:
            return data, crs, transform, meta
        return data, crs, transform

    def _fake_inventory_lines(*, model_id, product, run_date, fh, search_pattern):
        del model_id, product, run_date, search_pattern
        return {
            36: [":APCP:surface:35-36 hour acc fcst:"],
            42: [":APCP:surface:41-42 hour acc fcst:", ":APCP:surface:36-42 hour acc fcst:"],
        }[int(fh)]

    monkeypatch.setattr(derive_module, "fetch_variable", _fake_fetch_variable)
    monkeypatch.setattr(derive_module, "_kuchera_inventory_lines", _fake_inventory_lines)
    monkeypatch.setattr(derive_module, "_fetch_component", lambda **kwargs: (_ for _ in ()).throw(AssertionError("selector fallback should not be used")))
    monkeypatch.setattr(derive_module, "_resolve_cumulative_step_fhs", lambda *, hints, fh, default_step_hours=6: [36, 42])

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
                "step_transition_fh": "36",
                "step_hours_after_fh": "6",
            }
        )
    )
    var_capability = SimpleNamespace(conversion="kgm2_to_in")

    data, out_crs, out_transform = derive_module._derive_precip_total_cumulative(
        model_id="nbm",
        var_key="precip_total",
        product="co",
        run_date=datetime(2026, 3, 2, 0, 0),
        fh=42,
        var_spec_model=var_spec_model,
        var_capability=var_capability,
        model_plugin=object(),
    )

    assert out_crs == crs
    assert out_transform == transform
    assert fetch_patterns == [
        "36::APCP:surface:35-36 hour acc fcst:",
        "42::APCP:surface:36-42 hour acc fcst:",
    ]
    expected_inches = 7.0 * 0.03937007874015748
    np.testing.assert_allclose(data, np.full((2, 2), expected_inches, dtype=np.float32), rtol=1e-6, atol=1e-6)


def test_snowfall_total_mixed_cadence_uses_hourly_then_6hourly_steps(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    seen_fhs: list[int] = []

    def _fake_fetch_component(**kwargs):
        fh = int(kwargs["fh"])
        var_key = str(kwargs["var_key"])
        assert var_key == "asnow_step"
        seen_fhs.append(fh)
        return np.ones((2, 2), dtype=np.float32), crs, transform

    monkeypatch.setattr(derive_module, "_fetch_component", _fake_fetch_component)

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "asnow_step",
                "step_hours": "1",
                "step_transition_fh": "36",
                "step_hours_after_fh": "6",
            }
        )
    )
    var_capability = SimpleNamespace(conversion="m_to_in")

    data, out_crs, out_transform = derive_module._derive_precip_total_cumulative(
        model_id="nbm",
        var_key="snowfall_total",
        product="co",
        run_date=datetime(2026, 3, 2, 0, 0),
        fh=42,
        var_spec_model=var_spec_model,
        var_capability=var_capability,
        model_plugin=object(),
    )

    assert out_crs == crs
    assert out_transform == transform
    assert seen_fhs == [*range(1, 37), 42]
    # 37 steps of 1 meter converted to inches.
    expected_inches = 37.0 * 39.37007874015748
    np.testing.assert_allclose(data, np.full((2, 2), expected_inches, dtype=np.float32), rtol=1e-6, atol=1e-6)

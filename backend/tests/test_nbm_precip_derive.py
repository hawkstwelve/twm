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


def test_precip_total_mixed_cadence_uses_hourly_then_3hourly_steps(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    seen_fhs: list[int] = []

    def _fake_fetch_component(**kwargs):
        fh = int(kwargs["fh"])
        var_key = str(kwargs["var_key"])
        assert var_key == "apcp_step"
        seen_fhs.append(fh)
        return np.ones((2, 2), dtype=np.float32), crs, transform

    monkeypatch.setattr(derive_module, "_fetch_component", _fake_fetch_component)

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
                "step_transition_fh": "36",
                "step_hours_after_fh": "3",
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
    assert seen_fhs == [*range(1, 37), 39, 42]
    # 38 steps of 1 kg/m^2 (== 1 mm) converted to inches.
    expected_inches = 38.0 * 0.03937007874015748
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

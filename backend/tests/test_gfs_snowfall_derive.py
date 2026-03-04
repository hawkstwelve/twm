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


def test_resolve_cumulative_step_fhs_three_hourly() -> None:
    step_fhs = derive_module._resolve_cumulative_step_fhs(
        hints={"step_hours": "3"},
        fh=12,
        default_step_hours=6,
    )
    assert step_fhs == [3, 6, 9, 12]


def test_resolve_cumulative_step_fhs_with_transition() -> None:
    step_fhs = derive_module._resolve_cumulative_step_fhs(
        hints={
            "step_hours": "3",
            "step_transition_fh": "12",
            "step_hours_after_fh": "6",
        },
        fh=30,
        default_step_hours=6,
    )
    assert step_fhs == [3, 6, 9, 12, 18, 24, 30]


def test_snowfall_derive_uses_soft_interval_mask_for_3h_steps(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()

    apcp_by_fh = {
        3: np.array([[2.0, 2.0], [2.0, np.nan]], dtype=np.float32),
    }
    csnow_by_fh = {
        0: np.array([[0.2, np.nan], [1.2, 0.4]], dtype=np.float32),
        3: np.array([[0.8, 0.6], [0.5, -0.1]], dtype=np.float32),
    }

    seen_csnow_fhs: list[int] = []

    def _fake_fetch_component(**kwargs):
        fh = int(kwargs["fh"])
        var_key = str(kwargs["var_key"])
        if var_key == "apcp_step":
            return apcp_by_fh[fh], crs, transform
        if var_key == "csnow":
            seen_csnow_fhs.append(fh)
            return csnow_by_fh[fh], crs, transform
        raise AssertionError(f"Unexpected component var_key={var_key!r}")

    monkeypatch.setattr(derive_module, "_fetch_component", _fake_fetch_component)

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "snow_component": "csnow",
                "step_hours": "3",
                "slr": "10",
                "snow_mask_threshold": "0.5",
                "min_step_lwe_kgm2": "0.01",
            }
        )
    )

    data, out_crs, out_transform = derive_module._derive_snowfall_total_10to1_cumulative(
        model_id="gfs",
        var_key="snowfall_total",
        product="pgrb2.0p25",
        run_date=datetime(2026, 2, 28, 0, 0),
        fh=3,
        var_spec_model=var_spec_model,
        var_capability=None,
        model_plugin=object(),
    )

    assert out_crs == crs
    assert out_transform == transform
    assert seen_csnow_fhs == [0, 3]

    expected = np.array(
        [
            [0.39370078, 0.47244096],
            [0.39370078, np.nan],
        ],
        dtype=np.float32,
    )
    assert data.dtype == np.float32
    assert data.shape == (2, 2)
    np.testing.assert_allclose(data, expected, rtol=1e-5, atol=1e-5, equal_nan=True)


def test_snowfall_derive_skips_missing_csnow_samples_and_preserves_nan(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()

    apcp_by_fh = {
        3: np.array([[2.0, 2.0], [2.0, 2.0]], dtype=np.float32),
    }

    def _fake_fetch_component(**kwargs):
        fh = int(kwargs["fh"])
        var_key = str(kwargs["var_key"])
        if var_key == "apcp_step":
            return apcp_by_fh[fh], crs, transform
        if var_key == "csnow":
            raise RuntimeError("csnow sample unavailable")
        raise AssertionError(f"Unexpected component var_key={var_key!r}")

    monkeypatch.setattr(derive_module, "_fetch_component", _fake_fetch_component)

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "snow_component": "csnow",
                "step_hours": "3",
                "slr": "10",
                "min_step_lwe_kgm2": "0.01",
            }
        )
    )

    data, out_crs, out_transform = derive_module._derive_snowfall_total_10to1_cumulative(
        model_id="gfs",
        var_key="snowfall_total",
        product="pgrb2.0p25",
        run_date=datetime(2026, 2, 28, 0, 0),
        fh=3,
        var_spec_model=var_spec_model,
        var_capability=None,
        model_plugin=object(),
    )

    assert out_crs == crs
    assert out_transform == transform
    assert data.dtype == np.float32
    assert data.shape == (2, 2)
    assert np.isnan(data).all()

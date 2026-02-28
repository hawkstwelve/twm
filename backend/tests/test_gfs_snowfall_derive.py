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


def test_snowfall_derive_is_cumulative_and_masks_invalid_csnow(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()

    apcp_by_fh = {
        6: np.array([[2.0, 2.0], [2.0, 2.0]], dtype=np.float32),
        12: np.array([[1.0, 1.0], [1.0, 1.0]], dtype=np.float32),
        18: np.array([[3.0, 3.0], [3.0, 3.0]], dtype=np.float32),
        24: np.array([[4.0, 4.0], [4.0, 4.0]], dtype=np.float32),
    }
    csnow_by_fh = {
        6: np.array([[1.0, 393661.4], [0.0, 0.0]], dtype=np.float32),
        12: np.array([[0.0, 393661.4], [0.0, 0.0]], dtype=np.float32),
        18: np.array([[0.4, 393661.4], [1.0, 0.0]], dtype=np.float32),
        24: np.array([[0.6, 393661.4], [1.0, 0.0]], dtype=np.float32),
    }

    def _fake_fetch_component(**kwargs):
        fh = int(kwargs["fh"])
        var_key = str(kwargs["var_key"])
        if var_key == "apcp_step":
            return apcp_by_fh[fh], crs, transform
        if var_key == "csnow":
            return csnow_by_fh[fh], crs, transform
        raise AssertionError(f"Unexpected component var_key={var_key!r}")

    monkeypatch.setattr(derive_module, "_fetch_component", _fake_fetch_component)

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "snow_component": "csnow",
                "step_hours": "6",
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
        fh=24,
        var_spec_model=var_spec_model,
        var_capability=None,
        model_plugin=object(),
    )

    assert out_crs == crs
    assert out_transform == transform

    expected = np.array(
        [
            [2.3622048, np.nan],
            [2.7559056, 0.0],
        ],
        dtype=np.float32,
    )
    np.testing.assert_allclose(data, expected, rtol=1e-5, atol=1e-5, equal_nan=True)

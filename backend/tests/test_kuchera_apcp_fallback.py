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


def _kuchera_test_var_spec() -> SimpleNamespace:
    return SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "6",
                "kuchera_levels_hpa": "925,850,700,600,500",
                "kuchera_require_rh": "true",
                "kuchera_min_levels": "4",
            }
        )
    )


def _build_fetch_stub(
    *,
    apcp_by_fh: dict[int, np.ndarray],
    inventory_by_fh: dict[int, str],
):
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    temp_850 = np.full((2, 2), -12.0, dtype=np.float32)
    rh_850 = np.full((2, 2), 90.0, dtype=np.float32)

    def _fake_fetch_component(**kwargs):
        fh = int(kwargs["fh"])
        var_key = str(kwargs["var_key"])
        product = str(kwargs["product"])
        return_meta = bool(kwargs.get("return_meta", False))

        if var_key == "apcp_step":
            data = apcp_by_fh[fh]
            meta = {
                "inventory_line": inventory_by_fh[fh],
                "fh": fh,
                "product": product,
            }
        elif var_key == "tmp850":
            data = temp_850
            meta = {"inventory_line": "", "fh": fh, "product": product}
        elif var_key == "rh850":
            data = rh_850
            meta = {"inventory_line": "", "fh": fh, "product": product}
        else:
            raise ValueError(f"missing component {var_key}")

        if return_meta:
            return data, crs, transform, meta
        return data, crs, transform

    return _fake_fetch_component, crs, transform


def test_kuchera_apcp_cumulative_fallback_differences_to_step(monkeypatch, caplog) -> None:
    apcp_by_fh = {
        6: np.array([[1.2, 2.4], [0.6, 0.2]], dtype=np.float32),
        12: np.array([[3.2, 1.0], [0.4, 0.3]], dtype=np.float32),
    }
    inventory_by_fh = {
        6: ":APCP:surface:0-6 hour acc fcst:",
        12: ":APCP:surface:0-12 hour acc fcst:",
    }
    fake_fetch, crs, transform = _build_fetch_stub(
        apcp_by_fh=apcp_by_fh,
        inventory_by_fh=inventory_by_fh,
    )
    monkeypatch.setattr(derive_module, "_fetch_component", fake_fetch)

    with caplog.at_level("INFO"):
        data, out_crs, out_transform = derive_module._derive_snowfall_kuchera_total_cumulative(
            model_id="gfs",
            var_key="snowfall_kuchera_total",
            product="pgrb2.0p25",
            run_date=datetime(2026, 3, 4, 0, 0),
            fh=12,
            var_spec_model=_kuchera_test_var_spec(),
            var_capability=None,
            model_plugin=object(),
        )

    assert out_crs == crs
    assert out_transform == transform

    expected_step_12 = np.array([[2.0, 0.0], [0.0, 0.1]], dtype=np.float32)
    expected_total = apcp_by_fh[6] + expected_step_12
    expected_inches = expected_total * 0.03937007874015748 * 10.0
    np.testing.assert_allclose(data, expected_inches, rtol=1e-6, atol=1e-6, equal_nan=True)
    assert "KUCHERA_APCP step_fh=6" in caplog.text
    assert "KUCHERA_APCP step_fh=12" in caplog.text
    assert "mode=cumulative fallback=true" in caplog.text
    assert 'KUCHERA_APCP_FALLBACK step_fh=12 prev_fh=6 reason="cumulative 0-12"' in caplog.text


def test_kuchera_apcp_interval_step_is_used_directly(monkeypatch) -> None:
    apcp_by_fh = {
        6: np.array([[1.2, 2.4], [0.6, 0.2]], dtype=np.float32),
        12: np.array([[2.0, 0.5], [0.1, 0.4]], dtype=np.float32),
    }
    inventory_by_fh = {
        6: ":APCP:surface:0-6 hour acc fcst:",
        12: ":APCP:surface:6-12 hour acc fcst:",
    }
    fake_fetch, crs, transform = _build_fetch_stub(
        apcp_by_fh=apcp_by_fh,
        inventory_by_fh=inventory_by_fh,
    )
    monkeypatch.setattr(derive_module, "_fetch_component", fake_fetch)

    data, out_crs, out_transform = derive_module._derive_snowfall_kuchera_total_cumulative(
        model_id="gfs",
        var_key="snowfall_kuchera_total",
        product="pgrb2.0p25",
        run_date=datetime(2026, 3, 4, 0, 0),
        fh=12,
        var_spec_model=_kuchera_test_var_spec(),
        var_capability=None,
        model_plugin=object(),
    )

    assert out_crs == crs
    assert out_transform == transform
    expected_total = apcp_by_fh[6] + apcp_by_fh[12]
    expected_inches = expected_total * 0.03937007874015748 * 10.0
    np.testing.assert_allclose(data, expected_inches, rtol=1e-6, atol=1e-6, equal_nan=True)

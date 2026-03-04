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


def test_kuchera_slr_cold_profile_exceeds_ten() -> None:
    levels = [850, 700, 600, 500]
    temp_stack = [
        np.full((2, 2), -10.0, dtype=np.float32),
        np.full((2, 2), -14.0, dtype=np.float32),
        np.full((2, 2), -18.0, dtype=np.float32),
        np.full((2, 2), -22.0, dtype=np.float32),
    ]
    rh_stack = [np.full((2, 2), 90.0, dtype=np.float32) for _ in levels]

    slr = derive_module._compute_kuchera_slr(
        levels_hpa=levels,
        temp_stack_c=temp_stack,
        rh_stack_pct=rh_stack,
        require_rh=True,
    )

    assert slr.dtype == np.float32
    assert float(np.nanmean(slr)) > 10.0


def test_kuchera_slr_near_freezing_profile_near_ten() -> None:
    levels = [850, 700, 600, 500]
    temp_stack = [
        np.full((2, 2), -4.0, dtype=np.float32),
        np.full((2, 2), -5.0, dtype=np.float32),
        np.full((2, 2), -6.0, dtype=np.float32),
        np.full((2, 2), -7.0, dtype=np.float32),
    ]
    rh_stack = [np.full((2, 2), 92.0, dtype=np.float32) for _ in levels]

    slr = derive_module._compute_kuchera_slr(
        levels_hpa=levels,
        temp_stack_c=temp_stack,
        rh_stack_pct=rh_stack,
        require_rh=True,
    )

    assert slr.dtype == np.float32
    assert 9.0 <= float(np.nanmean(slr)) <= 11.0


def test_kuchera_falls_back_to_ten_to_one_with_insufficient_levels(monkeypatch, caplog) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    apcp_by_fh = {
        1: np.full((2, 2), 1.0, dtype=np.float32),
        2: np.full((2, 2), 1.0, dtype=np.float32),
    }
    temp_850 = np.full((2, 2), -12.0, dtype=np.float32)
    rh_850 = np.full((2, 2), 90.0, dtype=np.float32)

    def _fake_fetch_component(**kwargs):
        fh = int(kwargs["fh"])
        var_key = str(kwargs["var_key"])
        return_meta = bool(kwargs.get("return_meta", False))
        if var_key == "apcp_step":
            data = apcp_by_fh[fh]
            if return_meta:
                if fh == 1:
                    inventory_line = ":APCP:surface:0-1 hour acc fcst:"
                else:
                    inventory_line = ":APCP:surface:1-2 hour acc fcst:"
                return data, crs, transform, {"inventory_line": inventory_line}
            return data, crs, transform
        if var_key == "tmp850":
            if return_meta:
                return temp_850, crs, transform, {"inventory_line": ""}
            return temp_850, crs, transform
        if var_key == "rh850":
            if return_meta:
                return rh_850, crs, transform, {"inventory_line": ""}
            return rh_850, crs, transform
        raise ValueError(f"missing component {var_key}")

    monkeypatch.setattr(derive_module, "_fetch_component", _fake_fetch_component)

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
                "kuchera_levels_hpa": "925,850,700,600,500",
                "kuchera_require_rh": "true",
                "kuchera_min_levels": "4",
            }
        )
    )

    with caplog.at_level("INFO"):
        data, out_crs, out_transform = derive_module._derive_snowfall_kuchera_total_cumulative(
            model_id="gfs",
            var_key="snowfall_kuchera_total",
            product="pgrb2.0p25",
            run_date=datetime(2026, 3, 4, 0, 0),
            fh=2,
            var_spec_model=var_spec_model,
            var_capability=None,
            model_plugin=object(),
        )

    assert out_crs == crs
    assert out_transform == transform
    expected = np.full((2, 2), 2.0 * 0.03937007874015748 * 10.0, dtype=np.float32)
    np.testing.assert_allclose(data, expected, rtol=1e-6, atol=1e-6, equal_nan=True)
    assert "kuchera_profile insufficient_levels=1/4 fallback=10to1" in caplog.text
    assert "snow_ratio method=kuchera fh=2 levels=[925, 850, 700, 600, 500] fallback=10to1" in caplog.text


def test_kuchera_can_use_distinct_profile_product(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    apcp = np.full((2, 2), 1.0, dtype=np.float32)
    temp = np.full((2, 2), -14.0, dtype=np.float32)
    rh = np.full((2, 2), 90.0, dtype=np.float32)

    seen_products: list[tuple[str, str, int]] = []

    def _fake_fetch_component(**kwargs):
        product = str(kwargs["product"])
        var_key = str(kwargs["var_key"])
        fh = int(kwargs["fh"])
        return_meta = bool(kwargs.get("return_meta", False))
        seen_products.append((product, var_key, fh))
        if var_key == "apcp_step":
            if return_meta:
                return apcp, crs, transform, {"inventory_line": ":APCP:surface:0-1 hour acc fcst:"}
            return apcp, crs, transform
        if var_key.startswith("tmp"):
            if return_meta:
                return temp, crs, transform, {"inventory_line": ""}
            return temp, crs, transform
        if var_key.startswith("rh"):
            if return_meta:
                return rh, crs, transform, {"inventory_line": ""}
            return rh, crs, transform
        raise AssertionError(f"unexpected component {var_key}")

    monkeypatch.setattr(derive_module, "_fetch_component", _fake_fetch_component)

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
                "kuchera_profile_product": "prs",
                "kuchera_levels_hpa": "850,700,600,500",
                "kuchera_require_rh": "true",
                "kuchera_min_levels": "4",
            }
        )
    )
    data, out_crs, out_transform = derive_module._derive_snowfall_kuchera_total_cumulative(
        model_id="hrrr",
        var_key="snowfall_kuchera_total",
        product="sfc",
        run_date=datetime(2026, 3, 4, 20, 0),
        fh=1,
        var_spec_model=var_spec_model,
        var_capability=None,
        model_plugin=object(),
    )

    assert out_crs == crs
    assert out_transform == transform
    assert np.isfinite(data).all()
    assert ("sfc", "apcp_step", 1) in seen_products
    assert ("prs", "tmp850", 1) in seen_products
    assert ("prs", "rh850", 1) in seen_products

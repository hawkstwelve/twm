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


def test_kuchera_ratio_formula_warm_branch() -> None:
    max_t_k = np.array([[273.16]], dtype=np.float32)
    ratio = derive_module._kuchera_ratio_from_maxt_low500_k(max_t_k)
    np.testing.assert_allclose(ratio, np.array([[8.0]], dtype=np.float32), rtol=0.0, atol=1e-6)


def test_kuchera_ratio_formula_cold_branch() -> None:
    max_t_k = np.array([[270.16]], dtype=np.float32)
    ratio = derive_module._kuchera_ratio_from_maxt_low500_k(max_t_k)
    np.testing.assert_allclose(ratio, np.array([[13.0]], dtype=np.float32), rtol=0.0, atol=1e-6)


def test_kuchera_ratio_formula_clamps_to_bounds() -> None:
    max_t_k = np.array([[300.0, 230.0]], dtype=np.float32)
    ratio = derive_module._kuchera_ratio_from_maxt_low500_k(max_t_k)
    np.testing.assert_allclose(
        ratio,
        np.array([[5.0, 30.0]], dtype=np.float32),
        rtol=0.0,
        atol=1e-6,
    )


def test_kuchera_maxt_uses_remaining_levels_when_one_missing() -> None:
    levels = [850, 700, 600, 500]
    temp_stack = [
        np.full((2, 2), -12.0, dtype=np.float32),
        np.full((2, 2), -9.0, dtype=np.float32),
        np.full((2, 2), -11.0, dtype=np.float32),
        np.full((2, 2), -15.0, dtype=np.float32),
    ]

    max_t_k = derive_module._kuchera_maxt_low500_from_temp_stack_k(temp_stack)
    np.testing.assert_allclose(max_t_k, np.full((2, 2), 264.15, dtype=np.float32), rtol=0.0, atol=1e-4)

    slr = derive_module._compute_kuchera_slr(
        levels_hpa=levels,
        temp_stack_c=temp_stack,
    )
    assert slr.dtype == np.float32
    np.testing.assert_allclose(slr, np.full((2, 2), 19.01, dtype=np.float32), rtol=0.0, atol=1e-3)


def test_kuchera_slr_does_not_require_rh_inputs() -> None:
    levels = [925, 850, 700]
    temp_stack = [
        np.full((2, 2), -8.0, dtype=np.float32),
        np.full((2, 2), -10.0, dtype=np.float32),
        np.full((2, 2), -12.0, dtype=np.float32),
    ]

    slr = derive_module._compute_kuchera_slr(
        levels_hpa=levels,
        temp_stack_c=temp_stack,
    )

    assert slr.dtype == np.float32
    assert np.isfinite(slr).all()


def test_kuchera_can_use_distinct_profile_product_without_rh_fetch(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    apcp = np.full((2, 2), 1.0, dtype=np.float32)
    temp = np.full((2, 2), -12.0, dtype=np.float32)
    exact_apcp_pattern = derive_module._apcp_exact_window_pattern(0, 1)

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
        raise AssertionError(f"unexpected component {var_key}")

    monkeypatch.setattr(derive_module, "_fetch_component", _fake_fetch_component)
    monkeypatch.setattr(
        derive_module,
        "_kuchera_inventory_lines",
        lambda *, model_id, product, run_date, fh, search_pattern: [exact_apcp_pattern.rstrip("$")],
    )

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
        pattern_no_anchor = pattern[:-1] if pattern.endswith("$") else pattern
        if pattern == exact_apcp_pattern or pattern_no_anchor == exact_apcp_pattern.rstrip("$"):
            meta = {"inventory_line": pattern_no_anchor, "search_pattern": pattern}
            return (apcp, crs, transform, meta) if return_meta else (apcp, crs, transform)
        raise AssertionError(f"unexpected search_pattern: {pattern}")

    monkeypatch.setattr(derive_module, "fetch_variable", _fake_fetch_variable)

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
                "kuchera_profile_product": "prs",
                "kuchera_levels_hpa": "925,850,700,600",
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
    temp_fetches = [(product, var_key, fh) for product, var_key, fh in seen_products if var_key.startswith("tmp")]
    assert temp_fetches
    assert all(product == "prs" for product, _, _ in temp_fetches)
    assert ("prs", "tmp925", 1) in temp_fetches
    assert ("prs", "tmp850", 1) in temp_fetches
    assert ("prs", "tmp700", 1) in temp_fetches
    assert ("prs", "tmp600", 1) in temp_fetches
    assert not any(var_key == "tmp500" for _, var_key, _ in temp_fetches)
    assert len(temp_fetches) <= 4
    assert not any(var_key.startswith("rh") for _, var_key, _ in seen_products)

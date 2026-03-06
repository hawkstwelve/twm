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
            "apcp_step": [r":APCP:surface:[0-9]+-[0-9]+ hour acc[^:]*:$"],
            "tmp850": [":TMP:850 mb:"],
            "tmp700": [":TMP:700 mb:"],
            "tmp600": [":TMP:600 mb:"],
            "tmp500": [":TMP:500 mb:"],
            "pres_sfc": [":PRES:surface:"],
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


def _kuchera_var_spec(*, use_sfc_pressure_mask: bool = True) -> SimpleNamespace:
    return SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
                "kuchera_levels_hpa": "850,700,600,500",
                "kuchera_require_rh": "false",
                "kuchera_min_levels": "1",
                "kuchera_use_ptype_gate": "true",
                "kuchera_use_sfc_pressure_mask": "true" if use_sfc_pressure_mask else "false",
            }
        )
    )


def _make_fake_fetch(
    *,
    apcp: np.ndarray,
    temp_850: np.ndarray,
    temp_700: np.ndarray,
    temp_600: np.ndarray,
    temp_500: np.ndarray,
    sfc_pressure: np.ndarray,
    crs: CRS,
    transform: Affine,
):
    """Build a fake fetch_variable function for monkeypatching."""
    exact_apcp_pattern = ":APCP:surface:0-1 hour acc fcst:"
    csnow = np.ones(apcp.shape, dtype=np.float32)
    zeros = np.zeros(apcp.shape, dtype=np.float32)

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
        lookup = {
            exact_apcp_pattern: apcp,
            ":TMP:850 mb:": temp_850,
            ":TMP:700 mb:": temp_700,
            ":TMP:600 mb:": temp_600,
            ":TMP:500 mb:": temp_500,
            ":PRES:surface:": sfc_pressure,
            ":CSNOW:surface:": csnow,
            ":CRAIN:surface:": zeros,
            ":CICEP:surface:": zeros,
            ":CFRZR:surface:": zeros,
        }
        data = lookup.get(pattern)
        if data is None:
            raise AssertionError(f"unexpected search pattern: {pattern}")
        meta = {"inventory_line": pattern, "search_pattern": pattern}
        return (data, crs, transform, meta) if return_meta else (data, crs, transform)

    return _fake_fetch_variable, exact_apcp_pattern


def test_sfc_pressure_mask_raises_slr_for_high_terrain(monkeypatch) -> None:
    """At a high-elevation pixel (sfc_pressure < 850 hPa), the warm
    extrapolated 850 hPa temperature should be masked out, yielding a
    higher SLR than without the mask."""

    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    shape = (1, 2)

    # Column 0: high terrain (sfc ~700 hPa → 70000 Pa) — 850 hPa is underground
    # Column 1: low terrain  (sfc ~960 hPa → 96000 Pa) — all levels above ground
    sfc_pressure = np.array([[70000.0, 96000.0]], dtype=np.float32)

    apcp = np.full(shape, 5.0, dtype=np.float32)  # 5 kg/m² LWE

    # 850 hPa: warm (extrapolated below ground at col 0) → +5°C
    temp_850 = np.array([[5.0, -8.0]], dtype=np.float32)
    # 700 hPa: cold (above terrain everywhere) → -15°C
    temp_700 = np.full(shape, -15.0, dtype=np.float32)
    # 600 hPa: colder
    temp_600 = np.full(shape, -20.0, dtype=np.float32)
    # 500 hPa: coldest
    temp_500 = np.full(shape, -28.0, dtype=np.float32)

    fake_fetch, exact_apcp_pattern = _make_fake_fetch(
        apcp=apcp,
        temp_850=temp_850,
        temp_700=temp_700,
        temp_600=temp_600,
        temp_500=temp_500,
        sfc_pressure=sfc_pressure,
        crs=crs,
        transform=transform,
    )

    monkeypatch.setattr(derive_module, "fetch_variable", fake_fetch)
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

    # Run WITH surface pressure mask
    data_masked, _, _ = derive_module._derive_snowfall_kuchera_total_cumulative(
        model_id="hrrr",
        var_key="snowfall_kuchera_total",
        product="sfc",
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=1,
        var_spec_model=_kuchera_var_spec(use_sfc_pressure_mask=True),
        var_capability=None,
        model_plugin=_Plugin(),
    )

    # Run WITHOUT surface pressure mask
    data_unmasked, _, _ = derive_module._derive_snowfall_kuchera_total_cumulative(
        model_id="hrrr",
        var_key="snowfall_kuchera_total",
        product="sfc",
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=1,
        var_spec_model=_kuchera_var_spec(use_sfc_pressure_mask=False),
        var_capability=None,
        model_plugin=_Plugin(),
    )

    # At column 0 (high terrain): masked version should produce MORE snow
    # because the warm below-ground 850 hPa temp is excluded, yielding higher SLR.
    assert data_masked[0, 0] > data_unmasked[0, 0], (
        f"High-terrain pixel should have MORE snow with mask: "
        f"masked={data_masked[0, 0]:.4f} vs unmasked={data_unmasked[0, 0]:.4f}"
    )

    # At column 1 (low terrain): values should be identical since 850 hPa
    # is above ground (85000 Pa < 96000 Pa).
    np.testing.assert_allclose(
        data_masked[0, 1],
        data_unmasked[0, 1],
        rtol=1e-5,
        atol=1e-5,
        err_msg="Low-terrain pixel should be unchanged by mask",
    )


def test_sfc_pressure_mask_excludes_correct_levels(monkeypatch) -> None:
    """Verify that at a pixel where sfc_pressure is between 700 and 850 hPa,
    only the 850 hPa level is masked, and 700/600/500 remain."""

    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    shape = (1, 1)

    # Surface at 750 hPa → 75000 Pa: 850 hPa underground, 700/600/500 above
    sfc_pressure = np.array([[75000.0]], dtype=np.float32)
    apcp = np.array([[4.0]], dtype=np.float32)

    # 850 hPa: warm (underground) → +2°C (would drag SLR down if included)
    temp_850 = np.array([[2.0]], dtype=np.float32)
    # 700 hPa: above ground → -12°C
    temp_700 = np.array([[-12.0]], dtype=np.float32)
    # 600 hPa: -18°C
    temp_600 = np.array([[-18.0]], dtype=np.float32)
    # 500 hPa: -25°C
    temp_500 = np.array([[-25.0]], dtype=np.float32)

    fake_fetch, exact_apcp_pattern = _make_fake_fetch(
        apcp=apcp,
        temp_850=temp_850,
        temp_700=temp_700,
        temp_600=temp_600,
        temp_500=temp_500,
        sfc_pressure=sfc_pressure,
        crs=crs,
        transform=transform,
    )

    monkeypatch.setattr(derive_module, "fetch_variable", fake_fetch)
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

    data_masked, _, _ = derive_module._derive_snowfall_kuchera_total_cumulative(
        model_id="hrrr",
        var_key="snowfall_kuchera_total",
        product="sfc",
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=1,
        var_spec_model=_kuchera_var_spec(use_sfc_pressure_mask=True),
        var_capability=None,
        model_plugin=_Plugin(),
    )

    # With mask: max_T comes from 700 hPa = -12°C = 261.15 K
    # Cold branch: SLR = 12 + 1 * (271.16 - 261.15) = 22.01
    # Snow = 4.0 kg/m² * 22.01 * 0.03937 = 3.466 inches
    max_t_k_expected = -12.0 + 273.15  # 261.15 K
    expected_slr = 12.0 + 1.0 * (271.16 - max_t_k_expected)
    expected_snow = 4.0 * expected_slr * 0.03937007874015748

    np.testing.assert_allclose(data_masked[0, 0], expected_snow, rtol=1e-4, atol=1e-4)


def test_sfc_pressure_mask_all_levels_underground_falls_back_to_10to1(monkeypatch) -> None:
    """If surface pressure is so low that ALL profile levels are underground,
    the SLR should fall back to the default 10:1 ratio."""

    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    shape = (1, 1)

    # Surface at 400 hPa → all of 850/700/600/500 are underground
    sfc_pressure = np.array([[40000.0]], dtype=np.float32)
    apcp = np.array([[3.0]], dtype=np.float32)

    temp_850 = np.array([[10.0]], dtype=np.float32)
    temp_700 = np.array([[5.0]], dtype=np.float32)
    temp_600 = np.array([[0.0]], dtype=np.float32)
    temp_500 = np.array([[-5.0]], dtype=np.float32)

    fake_fetch, exact_apcp_pattern = _make_fake_fetch(
        apcp=apcp,
        temp_850=temp_850,
        temp_700=temp_700,
        temp_600=temp_600,
        temp_500=temp_500,
        sfc_pressure=sfc_pressure,
        crs=crs,
        transform=transform,
    )

    monkeypatch.setattr(derive_module, "fetch_variable", fake_fetch)
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
        var_spec_model=_kuchera_var_spec(use_sfc_pressure_mask=True),
        var_capability=None,
        model_plugin=_Plugin(),
    )

    # All levels masked → all temps are NaN → no valid levels → fallback 10:1
    # Snow = 3.0 * 10.0 * 0.03937 = 1.1811 inches
    expected_snow = 3.0 * 10.0 * 0.03937007874015748
    np.testing.assert_allclose(data[0, 0], expected_snow, rtol=1e-4, atol=1e-4)


def test_sfc_pressure_mask_graceful_fallback_on_fetch_failure(monkeypatch) -> None:
    """If the surface pressure field cannot be fetched, the derivation should
    proceed without masking (graceful degradation)."""

    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    shape = (1, 1)

    apcp = np.array([[5.0]], dtype=np.float32)
    temp_850 = np.array([[2.0]], dtype=np.float32)  # warm 850 hPa
    temp_700 = np.array([[-12.0]], dtype=np.float32)
    temp_600 = np.array([[-18.0]], dtype=np.float32)
    temp_500 = np.array([[-25.0]], dtype=np.float32)

    exact_apcp_pattern = ":APCP:surface:0-1 hour acc fcst:"
    csnow = np.ones(shape, dtype=np.float32)
    zeros = np.zeros(shape, dtype=np.float32)

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
        if pattern == ":PRES:surface:":
            raise RuntimeError("surface pressure unavailable")
        lookup = {
            exact_apcp_pattern: apcp,
            ":TMP:850 mb:": temp_850,
            ":TMP:700 mb:": temp_700,
            ":TMP:600 mb:": temp_600,
            ":TMP:500 mb:": temp_500,
            ":CSNOW:surface:": csnow,
            ":CRAIN:surface:": zeros,
            ":CICEP:surface:": zeros,
            ":CFRZR:surface:": zeros,
        }
        data = lookup.get(pattern)
        if data is None:
            raise AssertionError(f"unexpected search pattern: {pattern}")
        meta = {"inventory_line": pattern, "search_pattern": pattern}
        return (data, crs, transform, meta) if return_meta else (data, crs, transform)

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

    # Should NOT raise — proceeds without filtering
    data, _, _ = derive_module._derive_snowfall_kuchera_total_cumulative(
        model_id="hrrr",
        var_key="snowfall_kuchera_total",
        product="sfc",
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=1,
        var_spec_model=_kuchera_var_spec(use_sfc_pressure_mask=True),
        var_capability=None,
        model_plugin=_Plugin(),
    )

    # Without mask, max_T = 850 hPa = 2°C = 275.15 K → warm branch
    # SLR = 12 + 2*(271.16 - 275.15) = 12 - 7.98 = 5.0 (clamped to min)
    # Snow = 5.0 * 5.0 * 0.03937 = 0.984 inches
    assert np.isfinite(data[0, 0])
    expected_snow = 5.0 * 5.0 * 0.03937007874015748
    np.testing.assert_allclose(data[0, 0], expected_snow, rtol=1e-4, atol=1e-4)

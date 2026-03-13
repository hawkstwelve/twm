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


class _FakePlugin:
    def normalize_var_id(self, var_key: str) -> str:
        return var_key

    def get_var_capability(self, var_key: str):
        del var_key
        return None

    def get_var(self, var_key: str):
        search_by_var = {
            "apcp_step": [":APCP:surface:"],
            "csnow": [":CSNOW:surface:"],
        }
        search = search_by_var.get(var_key)
        if search is None:
            return None
        return SimpleNamespace(
            selectors=SimpleNamespace(
                search=search,
                filter_by_keys={},
                hints={},
            )
        )


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


def test_snowfall_derive_enforces_configured_threshold_for_3h_steps(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()

    apcp_by_fh = {
        3: np.array([[2.0, 2.0], [2.0, np.nan]], dtype=np.float32),
    }
    csnow_by_fh = {
        0: np.array([[0.2, np.nan], [1.2, 0.4]], dtype=np.float32),
        1: np.array([[0.8, 0.8], [0.8, 0.7]], dtype=np.float32),
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
                "snow_interval_sample_mode": "three_point",
                "slr": "10",
                "snow_mask_threshold": "0.5",
                "min_step_lwe_kgm2": "0.01",
            }
        )
    )

    data, out_crs, out_transform = derive_module._derive_snowfall_total_10to1_cumulative(
        model_id="test",
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
    assert seen_csnow_fhs == [0, 1, 3]

    expected = np.array(
        [
            [0.78740156, 0.78740156],
            [0.78740156, np.nan],
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
                "snow_interval_sample_mode": "three_point",
                "slr": "10",
                "min_step_lwe_kgm2": "0.01",
            }
        )
    )

    data, out_crs, out_transform = derive_module._derive_snowfall_total_10to1_cumulative(
        model_id="test",
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


def test_snowfall_derive_inventory_differences_gfs_cumulative_apcp(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    fetch_patterns: list[str] = []

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
        del model_id, product, run_date, herbie_kwargs
        pattern = str(search_pattern)
        fetch_patterns.append(f"{int(fh)}:{pattern}")
        data_by_pattern = {
            ":APCP:surface:0-3 hour acc fcst:$": np.full((2, 2), 3.0, dtype=np.float32),
            ":APCP:surface:0-6 hour acc fcst:$": np.full((2, 2), 6.0, dtype=np.float32),
            ":CSNOW:surface:": np.ones((2, 2), dtype=np.float32),
        }
        data = data_by_pattern[pattern]
        inventory_line = {
            ":APCP:surface:0-3 hour acc fcst:$": ":APCP:surface:0-3 hour acc fcst:",
            ":APCP:surface:0-6 hour acc fcst:$": ":APCP:surface:0-6 hour acc fcst:",
            ":CSNOW:surface:": "",
        }[pattern]
        meta = {"inventory_line": inventory_line, "search_pattern": pattern, "fh": int(fh)}
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
    plugin = _FakePlugin()

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "snow_component": "csnow",
                "step_hours": "3",
                "snow_interval_sample_mode": "three_point",
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
        run_date=datetime(2026, 3, 2, 0, 0),
        fh=6,
        var_spec_model=var_spec_model,
        var_capability=None,
        model_plugin=plugin,
    )

    assert out_crs == crs
    assert out_transform == transform
    assert fetch_patterns == [
        "3::APCP:surface:0-3 hour acc fcst:$",
        "0::CSNOW:surface:",
        "3::CSNOW:surface:",
        "6::APCP:surface:0-6 hour acc fcst:$",
        "3::CSNOW:surface:",
        "6::CSNOW:surface:",
    ]
    expected_inches = 6.0 * 0.03937007874015748 * 10.0
    np.testing.assert_allclose(data, np.full((2, 2), expected_inches, dtype=np.float32), rtol=1e-6, atol=1e-6)


def test_snowfall_derive_reuses_prior_cumulative_for_final_gfs_step(monkeypatch, caplog) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    fetch_patterns: list[str] = []

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
        del model_id, product, run_date, herbie_kwargs
        pattern = str(search_pattern)
        fetch_patterns.append(f"{int(fh)}:{pattern}")
        data_by_pattern = {
            ":APCP:surface:0-6 hour acc fcst:$": np.full((2, 2), 6.0, dtype=np.float32),
            ":CSNOW:surface:": np.ones((2, 2), dtype=np.float32),
        }
        data = data_by_pattern[pattern]
        inventory_line = {
            ":APCP:surface:0-6 hour acc fcst:$": ":APCP:surface:0-6 hour acc fcst:",
            ":CSNOW:surface:": "",
        }[pattern]
        meta = {"inventory_line": inventory_line, "search_pattern": pattern, "fh": int(fh)}
        if return_meta:
            return data, crs, transform, meta
        return data, crs, transform

    def _fake_inventory_lines(*, model_id, product, run_date, fh, search_pattern):
        del model_id, product, run_date, search_pattern
        return {
            6: [":APCP:surface:0-6 hour acc fcst:"],
        }[int(fh)]

    def _fake_prior_cumulative(*, model_id, run_date, var_key, fh, ctx, scale_divisor=0.03937007874015748):
        del model_id, run_date, ctx, scale_divisor
        if int(fh) != 3:
            return None
        if str(var_key) == "snowfall_total":
            return np.full((2, 2), 3.0, dtype=np.float32), crs, transform
        if str(var_key) == "precip_total":
            return np.full((2, 2), 3.0, dtype=np.float32), crs, transform
        return None

    monkeypatch.setattr(derive_module, "fetch_variable", _fake_fetch_variable)
    monkeypatch.setattr(derive_module, "_kuchera_inventory_lines", _fake_inventory_lines)
    monkeypatch.setattr(derive_module, "_kuchera_load_prior_cumulative", _fake_prior_cumulative)
    plugin = _FakePlugin()

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "snow_component": "csnow",
                "step_hours": "3",
                "snow_interval_sample_mode": "three_point",
                "slr": "10",
                "snow_mask_threshold": "0.5",
                "min_step_lwe_kgm2": "0.01",
            }
        )
    )

    with caplog.at_level("INFO"):
        data, out_crs, out_transform = derive_module._derive_snowfall_total_10to1_cumulative(
            model_id="gfs",
            var_key="snowfall_total",
            product="pgrb2.0p25",
            run_date=datetime(2026, 3, 2, 0, 0),
            fh=6,
            var_spec_model=var_spec_model,
            var_capability=None,
            model_plugin=plugin,
        )

    assert out_crs == crs
    assert out_transform == transform
    assert fetch_patterns == [
        "6::APCP:surface:0-6 hour acc fcst:$",
        "3::CSNOW:surface:",
        "6::CSNOW:surface:",
    ]
    assert "reused_prev_cumulative=true" in caplog.text
    expected_inches = 6.0 * 0.03937007874015748 * 10.0
    np.testing.assert_allclose(data, np.full((2, 2), expected_inches, dtype=np.float32), rtol=1e-6, atol=1e-6)


def test_snowfall_derive_reuses_prior_cumulative_across_gfs_late_cadence_transition(monkeypatch, caplog) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    fetch_patterns: list[str] = []

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
        del model_id, product, run_date, herbie_kwargs
        pattern = str(search_pattern)
        fetch_patterns.append(f"{int(fh)}:{pattern}")
        data_by_pattern = {
            ":APCP:surface:240-246 hour acc fcst:$": np.full((2, 2), 6.0, dtype=np.float32),
            ":CSNOW:surface:": np.ones((2, 2), dtype=np.float32),
        }
        data = data_by_pattern[pattern]
        inventory_line = {
            ":APCP:surface:240-246 hour acc fcst:$": ":APCP:surface:240-246 hour acc fcst:",
            ":CSNOW:surface:": "",
        }[pattern]
        meta = {"inventory_line": inventory_line, "search_pattern": pattern, "fh": int(fh)}
        if return_meta:
            return data, crs, transform, meta
        return data, crs, transform

    def _fake_inventory_lines(*, model_id, product, run_date, fh, search_pattern):
        del model_id, product, run_date, search_pattern
        return {
            246: [":APCP:surface:240-246 hour acc fcst:"],
        }[int(fh)]

    def _fake_prior_cumulative(*, model_id, run_date, var_key, fh, ctx, scale_divisor=0.03937007874015748):
        del model_id, run_date, ctx, scale_divisor
        if int(fh) != 240:
            return None
        if str(var_key) == "snowfall_total":
            return np.full((2, 2), 240.0, dtype=np.float32), crs, transform
        if str(var_key) == "precip_total":
            return np.full((2, 2), 240.0, dtype=np.float32), crs, transform
        return None

    monkeypatch.setattr(derive_module, "fetch_variable", _fake_fetch_variable)
    monkeypatch.setattr(derive_module, "_kuchera_inventory_lines", _fake_inventory_lines)
    monkeypatch.setattr(derive_module, "_kuchera_load_prior_cumulative", _fake_prior_cumulative)
    plugin = _FakePlugin()

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "snow_component": "csnow",
                "step_hours": "3",
                "step_transition_fh": "240",
                "step_hours_after_fh": "6",
                "snow_interval_sample_mode": "three_point",
                "slr": "10",
                "snow_mask_threshold": "0.5",
                "min_step_lwe_kgm2": "0.01",
            }
        )
    )

    with caplog.at_level("INFO"):
        data, out_crs, out_transform = derive_module._derive_snowfall_total_10to1_cumulative(
            model_id="gfs",
            var_key="snowfall_total",
            product="pgrb2.0p25",
            run_date=datetime(2026, 3, 2, 12, 0),
            fh=246,
            var_spec_model=var_spec_model,
            var_capability=None,
            model_plugin=plugin,
        )

    assert out_crs == crs
    assert out_transform == transform
    assert fetch_patterns == [
        "246::APCP:surface:240-246 hour acc fcst:$",
        "240::CSNOW:surface:",
        "246::CSNOW:surface:",
    ]
    assert "reused_prev_cumulative=true" in caplog.text
    assert "computed_steps=1" in caplog.text
    expected_inches = 246.0 * 0.03937007874015748 * 10.0
    np.testing.assert_allclose(data, np.full((2, 2), expected_inches, dtype=np.float32), rtol=1e-6, atol=1e-6)

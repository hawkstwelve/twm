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
            "tmp850": [":TMP:850 mb:"],
            "tmp700": [":TMP:700 mb:"],
            "tmp600": [":TMP:600 mb:"],
            "tmp500": [":TMP:500 mb:"],
            "rh850": [":RH:850 mb:"],
            "rh700": [":RH:700 mb:"],
            "rh600": [":RH:600 mb:"],
            "rh500": [":RH:500 mb:"],
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


def test_derive_bundle_reuses_fetch_and_warp_cache(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    plugin = _FakePlugin()
    fetch_calls: list[tuple[str, int]] = []
    warp_calls: list[tuple[str, int]] = []

    value_by_key: dict[tuple[str, int], float] = {
        (":APCP:surface:", 1): 2.0,
        (":APCP:surface:", 2): 3.0,
        (":CSNOW:surface:", 0): 0.2,
        (":CSNOW:surface:", 1): 0.3,
        (":CSNOW:surface:", 2): 0.4,
    }
    token_to_key: dict[float, tuple[str, int]] = {}

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
        key = (str(search_pattern), int(fh))
        fetch_calls.append(key)
        value = value_by_key[key]
        token_to_key[round(value, 4)] = key
        data = np.full((2, 2), value, dtype=np.float32)
        if return_meta:
            return data, crs, transform, {"inventory_line": f"{search_pattern}:{fh}"}
        return data, crs, transform

    def _fake_warp_to_target_grid(
        data: np.ndarray,
        src_crs,
        src_transform,
        *,
        model: str,
        region: str,
        resampling: str = "bilinear",
        src_nodata: float | None = None,
        dst_nodata: float = float("nan"),
    ) -> tuple[np.ndarray, Affine]:
        del src_crs, src_transform, model, region, resampling, src_nodata, dst_nodata
        token = round(float(data[0, 0]), 4)
        warp_calls.append(token_to_key[token])
        return data.astype(np.float32, copy=False), transform

    monkeypatch.setattr(derive_module, "fetch_variable", _fake_fetch_variable)
    monkeypatch.setattr(derive_module, "warp_to_target_grid", _fake_warp_to_target_grid)

    precip_spec = SimpleNamespace(
        derive="precip_total_cumulative",
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
            }
        ),
    )
    snowfall_spec = SimpleNamespace(
        derive="snowfall_total_10to1_cumulative",
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "snow_component": "csnow",
                "step_hours": "1",
                "slr": "10",
                "min_step_lwe_kgm2": "0.01",
            }
        ),
    )
    target_grid = {"region": "conus", "id": "gfs:conus:25000.0m"}
    fetch_ctx = derive_module.FetchContext(coverage="conus")

    precip_data, _, _ = derive_module.derive_variable(
        model_id="gfs",
        var_key="precip_total",
        product="pgrb2.0p25",
        run_date=datetime(2026, 3, 4, 0, 0),
        fh=2,
        var_spec_model=precip_spec,
        var_capability=SimpleNamespace(conversion="kgm2_to_in"),
        model_plugin=plugin,
        fetch_ctx=fetch_ctx,
        derive_component_target_grid=target_grid,
        derive_component_resampling="bilinear",
    )
    snowfall_data, _, _ = derive_module.derive_variable(
        model_id="gfs",
        var_key="snowfall_total",
        product="pgrb2.0p25",
        run_date=datetime(2026, 3, 4, 0, 0),
        fh=2,
        var_spec_model=snowfall_spec,
        var_capability=None,
        model_plugin=plugin,
        fetch_ctx=fetch_ctx,
        derive_component_target_grid=target_grid,
        derive_component_resampling="bilinear",
    )

    expected_component_steps = {
        (":APCP:surface:", 1),
        (":APCP:surface:", 2),
        (":CSNOW:surface:", 0),
        (":CSNOW:surface:", 1),
        (":CSNOW:surface:", 2),
    }
    assert set(fetch_calls) == expected_component_steps
    assert set(warp_calls) == expected_component_steps
    assert len(fetch_calls) == len(expected_component_steps)
    assert len(warp_calls) == len(expected_component_steps)

    assert fetch_ctx.stats["hits"] == 0
    assert fetch_ctx.stats["misses"] == len(expected_component_steps)
    assert fetch_ctx.warp_stats["misses"] == len(expected_component_steps)
    assert fetch_ctx.warp_stats["hits"] >= 3

    expected_precip_inches = 5.0 * 0.03937007874015748
    expected_snow_inches = 1.55 * 0.03937007874015748 * 10.0
    np.testing.assert_allclose(
        precip_data,
        np.full((2, 2), expected_precip_inches, dtype=np.float32),
        rtol=1e-6,
        atol=1e-6,
        equal_nan=True,
    )
    np.testing.assert_allclose(
        snowfall_data,
        np.full((2, 2), expected_snow_inches, dtype=np.float32),
        rtol=1e-6,
        atol=1e-6,
        equal_nan=True,
    )


def test_derive_bundle_reuses_apcp_warp_cache_with_kuchera(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    plugin = _FakePlugin()
    fetch_calls: list[tuple[str, int]] = []
    warp_calls: list[tuple[str, int]] = []

    value_by_key: dict[tuple[str, int], float] = {
        (":APCP:surface:", 1): 2.0,
        (":APCP:surface:", 2): 3.0,
        (":CSNOW:surface:", 0): 0.2,
        (":CSNOW:surface:", 1): 0.3,
        (":CSNOW:surface:", 2): 0.4,
        (":TMP:850 mb:", 1): -12.0,
        (":TMP:700 mb:", 1): -12.2,
        (":TMP:600 mb:", 1): -12.4,
        (":TMP:500 mb:", 1): -12.6,
        (":TMP:850 mb:", 2): -12.1,
        (":TMP:700 mb:", 2): -12.3,
        (":TMP:600 mb:", 2): -12.5,
        (":TMP:500 mb:", 2): -12.7,
        (":RH:850 mb:", 1): 90.0,
        (":RH:700 mb:", 1): 90.2,
        (":RH:600 mb:", 1): 90.4,
        (":RH:500 mb:", 1): 90.6,
        (":RH:850 mb:", 2): 90.1,
        (":RH:700 mb:", 2): 90.3,
        (":RH:600 mb:", 2): 90.5,
        (":RH:500 mb:", 2): 90.7,
    }
    token_to_key: dict[float, tuple[str, int]] = {}

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
        key = (str(search_pattern), int(fh))
        fetch_calls.append(key)
        value = value_by_key[key]
        token_to_key[round(value, 4)] = key
        data = np.full((2, 2), value, dtype=np.float32)
        if return_meta:
            return data, crs, transform, {"inventory_line": f"{search_pattern}:{fh}"}
        return data, crs, transform

    def _fake_warp_to_target_grid(
        data: np.ndarray,
        src_crs,
        src_transform,
        *,
        model: str,
        region: str,
        resampling: str = "bilinear",
        src_nodata: float | None = None,
        dst_nodata: float = float("nan"),
    ) -> tuple[np.ndarray, Affine]:
        del src_crs, src_transform, model, region, resampling, src_nodata, dst_nodata
        token = round(float(data[0, 0]), 4)
        warp_calls.append(token_to_key[token])
        return data.astype(np.float32, copy=False), transform

    monkeypatch.setattr(derive_module, "fetch_variable", _fake_fetch_variable)
    monkeypatch.setattr(derive_module, "warp_to_target_grid", _fake_warp_to_target_grid)

    snowfall_10to1_spec = SimpleNamespace(
        derive="snowfall_total_10to1_cumulative",
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "snow_component": "csnow",
                "step_hours": "1",
                "slr": "10",
                "min_step_lwe_kgm2": "0.01",
            }
        ),
    )
    snowfall_kuchera_spec = SimpleNamespace(
        derive="snowfall_kuchera_total_cumulative",
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
                "kuchera_levels_hpa": "850,700,600,500",
                "kuchera_require_rh": "true",
                "kuchera_min_levels": "4",
            }
        ),
    )
    target_grid = {"region": "conus", "id": "gfs:conus:25000.0m"}
    fetch_ctx = derive_module.FetchContext(coverage="conus")

    snowfall_10to1_data, _, _ = derive_module.derive_variable(
        model_id="gfs",
        var_key="snowfall_total",
        product="pgrb2.0p25",
        run_date=datetime(2026, 3, 4, 0, 0),
        fh=2,
        var_spec_model=snowfall_10to1_spec,
        var_capability=None,
        model_plugin=plugin,
        fetch_ctx=fetch_ctx,
        derive_component_target_grid=target_grid,
        derive_component_resampling="bilinear",
    )
    snowfall_kuchera_data, _, _ = derive_module.derive_variable(
        model_id="gfs",
        var_key="snowfall_kuchera_total",
        product="pgrb2.0p25",
        run_date=datetime(2026, 3, 4, 0, 0),
        fh=2,
        var_spec_model=snowfall_kuchera_spec,
        var_capability=None,
        model_plugin=plugin,
        fetch_ctx=fetch_ctx,
        derive_component_target_grid=target_grid,
        derive_component_resampling="bilinear",
    )

    assert fetch_calls.count((":APCP:surface:", 1)) == 1
    assert fetch_calls.count((":APCP:surface:", 2)) == 1
    assert fetch_ctx.warp_stats["hits"] > 0
    assert len(warp_calls) == len(fetch_calls)

    expected_snowfall_10to1_inches = 1.55 * 0.03937007874015748 * 10.0
    np.testing.assert_allclose(
        snowfall_10to1_data,
        np.full((2, 2), expected_snowfall_10to1_inches, dtype=np.float32),
        rtol=1e-6,
        atol=1e-6,
        equal_nan=True,
    )
    assert np.isfinite(snowfall_kuchera_data).all()
    assert float(np.nanmean(snowfall_kuchera_data)) > expected_snowfall_10to1_inches

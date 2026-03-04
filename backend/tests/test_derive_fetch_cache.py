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


def test_snowfall_derive_fetch_cache_reuses_repeated_component_fetches(monkeypatch) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    plugin = _FakePlugin()
    calls: list[tuple[str, int]] = []

    csnow_value_by_fh = {
        0: 0.2,
        1: 0.5,
        2: 0.8,
    }

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
        calls.append((str(search_pattern), int(fh)))
        if search_pattern == ":APCP:surface:":
            data = np.full((2, 2), 1.0, dtype=np.float32)
            if return_meta:
                return data, crs, transform, {"inventory_line": ":APCP:surface:0-1 hour acc fcst:"}
            return data, crs, transform
        if search_pattern == ":CSNOW:surface:":
            data = np.full((2, 2), csnow_value_by_fh[int(fh)], dtype=np.float32)
            if return_meta:
                return data, crs, transform, {"inventory_line": ""}
            return data, crs, transform
        raise AssertionError(f"Unexpected search pattern: {search_pattern!r}")

    monkeypatch.setattr(derive_module, "fetch_variable", _fake_fetch_variable)

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "snow_component": "csnow",
                "step_hours": "1",
                "slr": "10",
                "min_step_lwe_kgm2": "0.01",
            }
        )
    )

    no_ctx_data, _, _ = derive_module._derive_snowfall_total_10to1_cumulative(
        model_id="gfs",
        var_key="snowfall_total",
        product="pgrb2.0p25",
        run_date=datetime(2026, 3, 4, 0, 0),
        fh=2,
        var_spec_model=var_spec_model,
        var_capability=None,
        model_plugin=plugin,
    )
    no_ctx_calls = list(calls)
    assert no_ctx_calls.count((":CSNOW:surface:", 1)) == 2

    calls.clear()
    fetch_ctx = derive_module.FetchContext()
    with_ctx_data, _, _ = derive_module._derive_snowfall_total_10to1_cumulative(
        model_id="gfs",
        var_key="snowfall_total",
        product="pgrb2.0p25",
        run_date=datetime(2026, 3, 4, 0, 0),
        fh=2,
        var_spec_model=var_spec_model,
        var_capability=None,
        model_plugin=plugin,
        ctx=fetch_ctx,
    )

    assert calls.count((":CSNOW:surface:", 1)) == 1
    assert fetch_ctx.stats["hits"] > 0
    assert fetch_ctx.stats["misses"] > 0
    np.testing.assert_allclose(with_ctx_data, no_ctx_data, rtol=1e-6, atol=1e-6, equal_nan=True)

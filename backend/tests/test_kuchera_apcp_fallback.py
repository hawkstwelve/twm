from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from rasterio.crs import CRS
from rasterio.transform import Affine

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.builder import derive as derive_module

_APCP_SELECTOR_REGEX = r":APCP:surface:[0-9]+-[0-9]+ hour acc[^:]*:$"


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
        del model_id, run_date, herbie_kwargs
        step_fh = int(fh)
        pattern = str(search_pattern)
        expected_apcp_pattern = str(inventory_by_fh[step_fh])
        if pattern.startswith(":APCP:surface:"):
            if pattern != expected_apcp_pattern:
                raise AssertionError(
                    f"unexpected APCP pattern for fh={step_fh}: {pattern} != {expected_apcp_pattern}"
                )
            data = apcp_by_fh[step_fh]
            meta = {"inventory_line": expected_apcp_pattern, "search_pattern": pattern, "fh": step_fh, "product": product}
            return (data, crs, transform, meta) if return_meta else (data, crs, transform)

        if pattern == ":TMP:850 mb:":
            meta = {"inventory_line": "", "search_pattern": pattern, "fh": step_fh, "product": product}
            return (temp_850, crs, transform, meta) if return_meta else (temp_850, crs, transform)
        if pattern == ":RH:850 mb:":
            meta = {"inventory_line": "", "search_pattern": pattern, "fh": step_fh, "product": product}
            return (rh_850, crs, transform, meta) if return_meta else (rh_850, crs, transform)

        raise AssertionError(f"unexpected search_pattern: {pattern}")

    def _fake_inventory_lines(*, model_id, product, run_date, fh, search_pattern):
        del model_id, product, run_date, search_pattern
        return [str(inventory_by_fh[int(fh)])]

    return _fake_fetch_component, _fake_fetch_variable, _fake_inventory_lines, crs, transform


def test_kuchera_apcp_cumulative_fallback_differences_to_step(monkeypatch, caplog) -> None:
    apcp_by_fh = {
        6: np.array([[1.2, 2.4], [0.6, 0.2]], dtype=np.float32),
        12: np.array([[3.2, 1.0], [0.4, 0.3]], dtype=np.float32),
    }
    inventory_by_fh = {
        6: ":APCP:surface:0-6 hour acc fcst:",
        12: ":APCP:surface:0-12 hour acc fcst:",
    }
    fake_fetch, fake_fetch_variable, fake_inventory_lines, crs, transform = _build_fetch_stub(
        apcp_by_fh=apcp_by_fh,
        inventory_by_fh=inventory_by_fh,
    )
    monkeypatch.setattr(derive_module, "_fetch_component", fake_fetch)
    monkeypatch.setattr(derive_module, "fetch_variable", fake_fetch_variable)
    monkeypatch.setattr(derive_module, "_kuchera_inventory_lines", fake_inventory_lines)

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
    fake_fetch, fake_fetch_variable, fake_inventory_lines, crs, transform = _build_fetch_stub(
        apcp_by_fh=apcp_by_fh,
        inventory_by_fh=inventory_by_fh,
    )
    monkeypatch.setattr(derive_module, "_fetch_component", fake_fetch)
    monkeypatch.setattr(derive_module, "fetch_variable", fake_fetch_variable)
    monkeypatch.setattr(derive_module, "_kuchera_inventory_lines", fake_inventory_lines)

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


@pytest.mark.parametrize(
    "model_id,step_fhs,inventory_by_fh,apcp_pattern_data,expected_logs,unexpected_pattern,expect_regex_fetch,expected_inventory_queries",
    [
        (
            "gfs",
            [1],
            {1: [":APCP:surface:0-1 hour acc fcst:"]},
            {":APCP:surface:0-1 hour acc fcst:": np.full((2, 2), 1.0, dtype=np.float32)},
            ["exact_guess_used=true", "inventory_selected=false", 'selected_window="0-1"', "selector_fallback=false"],
            _APCP_SELECTOR_REGEX,
            False,
            1,
        ),
        (
            "nam",
            [29, 30],
            {
                29: [":APCP:surface:0-29 hour acc fcst:"],
                30: [":APCP:surface:0-30 hour acc fcst:", ":APCP:surface:27-30 hour acc fcst:"],
            },
            {
                ":APCP:surface:0-29 hour acc fcst:": np.full((2, 2), 5.0, dtype=np.float32),
                ":APCP:surface:27-30 hour acc fcst:": np.full((2, 2), 1.0, dtype=np.float32),
            },
            ["step_fh=30", "exact_guess_used=false", "inventory_selected=true", 'selected_window="27-30"'],
            ":APCP:surface:29-30 hour acc fcst:",
            False,
            2,
        ),
        (
            "gfs",
            [1],
            {1: [":APCP:surface:0-2 hour acc fcst:"]},
            {},
            ["exact_guess_used=false", "inventory_selected=false", "selector_fallback=true", 'reason="inventory_no_matching_window"'],
            ":APCP:surface:0-1 hour acc fcst:",
            True,
            1,
        ),
    ],
)
def test_kuchera_inventory_driven_apcp_selection(
    monkeypatch,
    caplog,
    model_id: str,
    step_fhs: list[int],
    inventory_by_fh: dict[int, list[str]],
    apcp_pattern_data: dict[str, np.ndarray],
    expected_logs: list[str],
    unexpected_pattern: str,
    expect_regex_fetch: bool,
    expected_inventory_queries: int,
) -> None:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    temp = np.full((2, 2), -12.0, dtype=np.float32)
    rh = np.full((2, 2), 90.0, dtype=np.float32)
    fetch_patterns: list[str] = []
    inventory_queries: list[tuple[int, str]] = []

    class _Plugin:
        def normalize_var_id(self, var_key: str) -> str:
            return str(var_key)

        def get_var_capability(self, var_key: str):
            del var_key
            return None

        def get_var(self, var_key: str):
            by_var = {
                "apcp_step": [_APCP_SELECTOR_REGEX],
                "tmp850": [":TMP:850 mb:"],
                "rh850": [":RH:850 mb:"],
            }
            search = by_var.get(str(var_key))
            if search is None:
                return None
            return SimpleNamespace(
                selectors=SimpleNamespace(
                    search=search,
                    filter_by_keys={},
                    hints={},
                )
            )

    def _fake_fetch_variable(*, model_id, product, search_pattern, run_date, fh, herbie_kwargs=None, return_meta=False):
        del model_id, product, run_date, herbie_kwargs
        pattern = str(search_pattern)
        fetch_patterns.append(pattern)
        if pattern in apcp_pattern_data:
            data = apcp_pattern_data[pattern]
            meta = {"inventory_line": pattern, "search_pattern": pattern, "fh": int(fh)}
        elif pattern == _APCP_SELECTOR_REGEX:
            data = np.full((2, 2), 0.8, dtype=np.float32)
            meta = {"inventory_line": ":APCP:surface:0-1 hour acc fcst:", "search_pattern": pattern, "fh": int(fh)}
        elif pattern == ":TMP:850 mb:":
            data = temp
            meta = {"inventory_line": "", "search_pattern": pattern, "fh": int(fh)}
        elif pattern == ":RH:850 mb:":
            data = rh
            meta = {"inventory_line": "", "search_pattern": pattern, "fh": int(fh)}
        else:
            raise AssertionError(f"unexpected fetch pattern {pattern}")
        if return_meta:
            return data, crs, transform, meta
        return data, crs, transform

    def _fake_inventory_lines(*, model_id, product, run_date, fh, search_pattern):
        del model_id, product, run_date
        inventory_queries.append((int(fh), str(search_pattern)))
        return list(inventory_by_fh.get(int(fh), []))

    monkeypatch.setattr(derive_module, "fetch_variable", _fake_fetch_variable)
    monkeypatch.setattr(derive_module, "_kuchera_inventory_lines", _fake_inventory_lines)
    monkeypatch.setattr(derive_module, "_resolve_cumulative_step_fhs", lambda *, hints, fh, default_step_hours=6: list(step_fhs))

    var_spec_model = SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
                "kuchera_levels_hpa": "850",
                "kuchera_require_rh": "true",
                "kuchera_min_levels": "1",
            }
        )
    )

    with caplog.at_level("INFO"):
        data, out_crs, out_transform = derive_module._derive_snowfall_kuchera_total_cumulative(
            model_id=model_id,
            var_key="snowfall_kuchera_total",
            product="pgrb2.0p25",
            run_date=datetime(2026, 3, 5, 0, 0),
            fh=max(step_fhs),
            var_spec_model=var_spec_model,
            var_capability=None,
            model_plugin=_Plugin(),
        )

    assert out_crs == crs
    assert out_transform == transform
    assert np.isfinite(data).all()
    assert fetch_patterns.count(unexpected_pattern) == 0
    assert (fetch_patterns.count(_APCP_SELECTOR_REGEX) > 0) is expect_regex_fetch
    assert len(inventory_queries) == expected_inventory_queries
    assert all(query_pattern == ":APCP:surface:" for _, query_pattern in inventory_queries)
    for expected in expected_logs:
        assert expected in caplog.text

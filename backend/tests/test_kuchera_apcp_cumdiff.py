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

_APCP_SELECTOR_REGEX = r":APCP:surface:[0-9]+-[0-9]+ hour acc[^:]*:$"
_TO_INCHES = np.float32(10.0 * 0.03937007874015748)


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


def _var_spec() -> SimpleNamespace:
    return SimpleNamespace(
        selectors=SimpleNamespace(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
                "kuchera_levels_hpa": "850",
                "kuchera_require_rh": "false",
                # Force deterministic 10:1 SLR fallback.
                "kuchera_min_levels": "99",
            }
        )
    )


def _run_case(
    monkeypatch,
    *,
    step_fhs: list[int],
    inventory_lines_by_fh: dict[int, list[str]],
    apcp_by_pattern: dict[str, np.ndarray],
) -> np.ndarray:
    crs = CRS.from_epsg(4326)
    transform = Affine.identity()
    tmp850 = np.full((2, 2), -12.0, dtype=np.float32)

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

        if pattern == ":TMP:850 mb:":
            meta = {"inventory_line": "", "search_pattern": pattern}
            return (tmp850, crs, transform, meta) if return_meta else (tmp850, crs, transform)

        if pattern in apcp_by_pattern:
            data = apcp_by_pattern[pattern]
            meta = {"inventory_line": pattern, "search_pattern": pattern}
            return (data, crs, transform, meta) if return_meta else (data, crs, transform)

        if pattern == _APCP_SELECTOR_REGEX:
            raise AssertionError("selector-regex fallback not expected for acceptance vectors")

        raise AssertionError(f"unexpected pattern: {pattern}")

    monkeypatch.setattr(derive_module, "fetch_variable", _fake_fetch_variable)
    monkeypatch.setattr(
        derive_module,
        "_kuchera_inventory_lines",
        lambda *, model_id, product, run_date, fh, search_pattern: list(inventory_lines_by_fh.get(int(fh), [])),
    )
    monkeypatch.setattr(
        derive_module,
        "_resolve_cumulative_step_fhs",
        lambda *, hints, fh, default_step_hours=6: list(step_fhs),
    )

    data, _, _ = derive_module._derive_snowfall_kuchera_total_cumulative(
        model_id="hrrr",
        var_key="snowfall_kuchera_total",
        product="sfc",
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=max(step_fhs),
        var_spec_model=_var_spec(),
        var_capability=None,
        model_plugin=_Plugin(),
    )
    return data


def test_acceptance_1_mixed_step_then_cumulative(monkeypatch) -> None:
    data = _run_case(
        monkeypatch,
        step_fhs=[1, 2, 3],
        inventory_lines_by_fh={
            1: [":APCP:surface:0-1 hour acc fcst:"],
            2: [":APCP:surface:1-2 hour acc fcst:"],
            3: [":APCP:surface:0-3 hour acc fcst:"],
        },
        apcp_by_pattern={
            ":APCP:surface:0-1 hour acc fcst:": np.array([[2.0, 1.0], [0.5, 0.0]], dtype=np.float32),
            ":APCP:surface:1-2 hour acc fcst:": np.array([[3.0, 2.0], [1.0, 0.0]], dtype=np.float32),
            ":APCP:surface:0-3 hour acc fcst:": np.array([[8.0, 4.0], [2.5, 0.0]], dtype=np.float32),
        },
    )

    expected_lwe = np.array([[8.0, 4.0], [2.5, 0.0]], dtype=np.float32)
    np.testing.assert_allclose(data, expected_lwe * _TO_INCHES, rtol=1e-6, atol=1e-6)


def test_acceptance_2_all_cumulative(monkeypatch) -> None:
    data = _run_case(
        monkeypatch,
        step_fhs=[1, 2, 3],
        inventory_lines_by_fh={
            1: [":APCP:surface:0-1 hour acc fcst:"],
            2: [":APCP:surface:0-2 hour acc fcst:"],
            3: [":APCP:surface:0-3 hour acc fcst:"],
        },
        apcp_by_pattern={
            ":APCP:surface:0-1 hour acc fcst:": np.full((2, 2), 1.0, dtype=np.float32),
            ":APCP:surface:0-2 hour acc fcst:": np.full((2, 2), 3.0, dtype=np.float32),
            ":APCP:surface:0-3 hour acc fcst:": np.full((2, 2), 6.0, dtype=np.float32),
        },
    )

    expected_lwe = np.full((2, 2), 6.0, dtype=np.float32)
    np.testing.assert_allclose(data, expected_lwe * _TO_INCHES, rtol=1e-6, atol=1e-6)


def test_acceptance_3_cumulative_then_step_resumes(monkeypatch) -> None:
    data = _run_case(
        monkeypatch,
        step_fhs=[1, 2, 3, 4],
        inventory_lines_by_fh={
            1: [":APCP:surface:0-1 hour acc fcst:"],
            2: [":APCP:surface:0-2 hour acc fcst:"],
            3: [":APCP:surface:2-3 hour acc fcst:"],
            4: [":APCP:surface:3-4 hour acc fcst:"],
        },
        apcp_by_pattern={
            ":APCP:surface:0-1 hour acc fcst:": np.full((2, 2), 2.0, dtype=np.float32),
            ":APCP:surface:0-2 hour acc fcst:": np.full((2, 2), 5.0, dtype=np.float32),
            ":APCP:surface:2-3 hour acc fcst:": np.full((2, 2), 1.0, dtype=np.float32),
            ":APCP:surface:3-4 hour acc fcst:": np.full((2, 2), 0.5, dtype=np.float32),
        },
    )

    expected_lwe = np.full((2, 2), 6.5, dtype=np.float32)
    np.testing.assert_allclose(data, expected_lwe * _TO_INCHES, rtol=1e-6, atol=1e-6)


def test_acceptance_4_negative_diff_clipped_to_zero(monkeypatch) -> None:
    data = _run_case(
        monkeypatch,
        step_fhs=[1, 2],
        inventory_lines_by_fh={
            1: [":APCP:surface:0-1 hour acc fcst:"],
            2: [":APCP:surface:0-2 hour acc fcst:"],
        },
        apcp_by_pattern={
            ":APCP:surface:0-1 hour acc fcst:": np.full((2, 2), 5.0, dtype=np.float32),
            ":APCP:surface:0-2 hour acc fcst:": np.full((2, 2), 4.0, dtype=np.float32),
        },
    )

    expected_lwe = np.full((2, 2), 5.0, dtype=np.float32)
    np.testing.assert_allclose(data, expected_lwe * _TO_INCHES, rtol=1e-6, atol=1e-6)


def test_acceptance_5_nan_mask_any_step_contributed_semantics(monkeypatch) -> None:
    data = _run_case(
        monkeypatch,
        step_fhs=[1, 2],
        inventory_lines_by_fh={
            1: [":APCP:surface:0-1 hour acc fcst:"],
            2: [":APCP:surface:1-2 hour acc fcst:"],
        },
        apcp_by_pattern={
            ":APCP:surface:0-1 hour acc fcst:": np.array(
                [[1.0, np.nan], [2.0, 3.0]],
                dtype=np.float32,
            ),
            ":APCP:surface:1-2 hour acc fcst:": np.array(
                [[1.0, 1.0], [np.nan, 1.0]],
                dtype=np.float32,
            ),
        },
    )

    expected_lwe = np.array(
        [[2.0, 1.0], [2.0, 4.0]],
        dtype=np.float32,
    )
    np.testing.assert_allclose(data, expected_lwe * _TO_INCHES, rtol=1e-6, atol=1e-6)


def test_acceptance_6_single_forecast_hour(monkeypatch) -> None:
    data = _run_case(
        monkeypatch,
        step_fhs=[1],
        inventory_lines_by_fh={
            1: [":APCP:surface:0-1 hour acc fcst:"],
        },
        apcp_by_pattern={
            ":APCP:surface:0-1 hour acc fcst:": np.array(
                [[4.5, 2.0], [0.0, 1.0]],
                dtype=np.float32,
            ),
        },
    )

    expected_lwe = np.array(
        [[4.5, 2.0], [0.0, 1.0]],
        dtype=np.float32,
    )
    np.testing.assert_allclose(data, expected_lwe * _TO_INCHES, rtol=1e-6, atol=1e-6)

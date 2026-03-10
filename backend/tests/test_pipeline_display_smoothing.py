import numpy as np

from app.services.builder.colorize import float_to_rgba
from app.services.builder.pipeline import _prepare_display_data_for_colorize, _warp_resampling_for_variable


def test_gfs_continuous_vars_skip_display_smoothing() -> None:
    data = np.zeros((9, 9), dtype=np.float32)
    data[4, 4] = 100.0
    spec = {"type": "continuous", "display_smoothing_sigma": 0.8}

    for var_key in ("tmp2m", "dp2m", "tmp850", "wspd10m", "wgst10m", "precip_total", "snowfall_total", "qpf6h"):
        display = _prepare_display_data_for_colorize(
            data,
            spec,
            model_id="gfs",
            var_key=var_key,
        )
        np.testing.assert_array_equal(display, data)


def test_non_gfs_continuous_still_applies_display_smoothing() -> None:
    data = np.zeros((9, 9), dtype=np.float32)
    data[4, 4] = 100.0
    spec = {"type": "continuous", "display_smoothing_sigma": 0.8}

    display = _prepare_display_data_for_colorize(
        data,
        spec,
        model_id="hrrr",
        var_key="tmp2m",
    )

    assert not np.array_equal(display, data)
    assert 0.0 < float(display[4, 4]) < 100.0


def test_discrete_kind_remains_passthrough() -> None:
    data = np.arange(16, dtype=np.float32).reshape(4, 4)
    spec = {"type": "indexed", "display_smoothing_sigma": 0.8}

    display = _prepare_display_data_for_colorize(
        data,
        spec,
        model_id="gfs",
        var_key="precip_ptype",
    )
    np.testing.assert_array_equal(display, data)


def test_precip_and_snow_use_nearest_warp_resampling_across_models() -> None:
    for model_id in ("hrrr", "nam", "gfs"):
        assert _warp_resampling_for_variable(
            model_id=model_id,
            var_key="snowfall_total",
            kind="continuous",
        ) == "nearest"
        assert _warp_resampling_for_variable(
            model_id=model_id,
            var_key="precip_total",
            kind="continuous",
        ) == "nearest"


def test_continuous_transparent_pixels_zero_rgb() -> None:
    data = np.array([[0.0, 0.05, 0.2]], dtype=np.float32)

    rgba, _ = float_to_rgba(data, "snowfall_total")

    assert tuple(int(v) for v in rgba[:, 0, 0]) == (0, 0, 0, 0)
    assert tuple(int(v) for v in rgba[:, 0, 1]) == (0, 0, 0, 0)
    assert int(rgba[3, 0, 2]) == 255

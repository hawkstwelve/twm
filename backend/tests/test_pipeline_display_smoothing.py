import numpy as np

from app.services.builder.pipeline import _prepare_display_data_for_colorize


def test_gfs_continuous_vars_skip_display_smoothing() -> None:
    data = np.zeros((9, 9), dtype=np.float32)
    data[4, 4] = 100.0
    spec = {"type": "continuous", "display_smoothing_sigma": 0.8}

    for var_key in ("tmp2m", "tmp850", "wspd10m", "wgst10m", "precip_total", "qpf6h"):
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

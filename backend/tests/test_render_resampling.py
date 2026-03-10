from types import SimpleNamespace

from app.services import render_resampling


def _set_capabilities(monkeypatch, variable_catalog):
    capabilities = {
        "gfs": SimpleNamespace(
            variable_catalog=variable_catalog,
            grid_meters_by_region={"conus": 25000.0},
            canonical_region="conus",
        ),
        "hrrr": SimpleNamespace(
            variable_catalog=variable_catalog,
            grid_meters_by_region={"conus": 3000.0},
            canonical_region="conus",
        ),
        "nam": SimpleNamespace(
            variable_catalog=variable_catalog,
            grid_meters_by_region={"conus": 5000.0},
            canonical_region="conus",
        ),
        "nbm": SimpleNamespace(
            variable_catalog=variable_catalog,
            grid_meters_by_region={"conus": 13000.0},
            canonical_region="conus",
        ),
    }
    monkeypatch.setattr(render_resampling, "list_model_capabilities", lambda: capabilities)
    render_resampling._lookup_kind_from_capabilities.cache_clear()
    render_resampling._lookup_variable_catalog_entry.cache_clear()
    render_resampling._lookup_model_grid_km.cache_clear()
    render_resampling.display_resampling_override.cache_clear()
    render_resampling._warned_unknown_kind.clear()
    render_resampling._unknown_kind_hits.clear()


def test_continuous_kind_maps_to_bilinear(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {"tmp2m": SimpleNamespace(kind="continuous", color_map_id="tmp2m")},
    )

    assert render_resampling.resampling_name_for_kind(model_id="gfs", var_key="tmp2m") == "bilinear"
    assert render_resampling.rio_tiler_resampling_kwargs(model_id="gfs", var_key="tmp2m") == {
        "resampling_method": "bilinear",
        "reproject_method": "bilinear",
    }


def test_discrete_kind_maps_to_nearest(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {"precip_ptype": SimpleNamespace(kind="indexed", color_map_id="precip_ptype")},
    )

    assert render_resampling.resampling_name_for_kind(model_id="gfs", var_key="precip_ptype") == "nearest"
    assert render_resampling.rasterio_resampling_for_loop(model_id="gfs", var_key="precip_ptype").name == "nearest"


def test_unknown_kind_falls_back_to_bilinear_and_warns(monkeypatch, caplog):
    _set_capabilities(
        monkeypatch,
        {"tmp2m": SimpleNamespace(kind=None, color_map_id="tmp2m")},
    )

    caplog.set_level("WARNING")
    first = render_resampling.resampling_name_for_kind(model_id="gfs", var_key="tmp2m")
    second = render_resampling.resampling_name_for_kind(model_id="gfs", var_key="tmp2m")

    assert first == "bilinear"
    assert second == "bilinear"
    assert render_resampling._unknown_kind_hits[("gfs", "tmp2m")] == 2
    assert "defaulting resampling to bilinear" in caplog.text


def test_value_render_gate_true_for_gfs_continuous(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {"tmp2m": SimpleNamespace(kind="continuous", color_map_id="tmp2m")},
    )

    assert render_resampling.use_value_render_for_variable(model_id="gfs", var_key="tmp2m") is True
    assert render_resampling.variable_color_map_id("gfs", "tmp2m") == "tmp2m"


def test_value_render_gate_false_for_hrrr_continuous(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {"tmp2m": SimpleNamespace(kind="continuous", color_map_id="tmp2m")},
    )

    assert render_resampling.use_value_render_for_variable(model_id="hrrr", var_key="tmp2m") is False


def test_targeted_accumulations_use_value_render_for_hrrr_nam_and_nbm(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {
            "snowfall_total": SimpleNamespace(kind="continuous", color_map_id="snowfall_total"),
            "snowfall_kuchera_total": SimpleNamespace(kind="continuous", color_map_id="snowfall_total"),
            "precip_total": SimpleNamespace(kind="continuous", color_map_id="precip_total"),
        },
    )

    for model_id in ("hrrr", "nam", "nbm"):
        for var_key in ("snowfall_total", "snowfall_kuchera_total", "precip_total"):
            assert render_resampling.use_value_render_for_variable(model_id=model_id, var_key=var_key) is True
            assert render_resampling.render_resampling_name(model_id=model_id, var_key=var_key) == "bilinear"


def test_loop_fixed_size_for_gfs_continuous(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {"tmp2m": SimpleNamespace(kind="continuous", color_map_id="tmp2m")},
    )

    src_h, src_w = 148, 261
    out_h0, out_w0, fixed0 = render_resampling.compute_loop_output_shape(
        model_id="gfs",
        var_key="tmp2m",
        src_h=src_h,
        src_w=src_w,
        max_dim=1600,
        fixed_width=1600,
    )
    out_h1, out_w1, fixed1 = render_resampling.compute_loop_output_shape(
        model_id="gfs",
        var_key="tmp2m",
        src_h=src_h,
        src_w=src_w,
        max_dim=2400,
        fixed_width=2400,
    )

    assert fixed0 is True
    assert out_w0 == 1600
    assert out_h0 == int(round(src_h * (1600 / src_w)))
    assert fixed1 is True
    assert out_w1 == 2400
    assert out_h1 == int(round(src_h * (2400 / src_w)))


def test_loop_fixed_size_not_applied_for_hrrr(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {"tmp2m": SimpleNamespace(kind="continuous", color_map_id="tmp2m")},
    )

    src_h, src_w = 148, 261
    out_h, out_w, fixed = render_resampling.compute_loop_output_shape(
        model_id="hrrr",
        var_key="tmp2m",
        src_h=src_h,
        src_w=src_w,
        max_dim=1600,
        fixed_width=1600,
    )

    assert fixed is False
    assert out_w == src_w
    assert out_h == src_h


def test_display_resampling_override_for_precip_and_snow(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {
            "snowfall_total": SimpleNamespace(kind="continuous", color_map_id="snowfall_total"),
            "precip_total": SimpleNamespace(kind="continuous", color_map_id="precip_total"),
        },
    )

    assert render_resampling.display_resampling_override("hrrr", "snowfall_total") == "nearest"
    assert render_resampling.display_resampling_override("gfs", "precip_total") == "nearest"
    assert render_resampling.resampling_name_for_kind(model_id="hrrr", var_key="snowfall_total") == "nearest"
    assert render_resampling.rio_tiler_resampling_kwargs(model_id="gfs", var_key="precip_total") == {
        "resampling_method": "bilinear",
        "reproject_method": "bilinear",
    }

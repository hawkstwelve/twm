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


def test_radar_ptype_keeps_crisp_backend_loop_resampling(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {"radar_ptype": SimpleNamespace(kind="indexed", color_map_id="radar_ptype")},
    )

    for model_id in ("hrrr", "nam"):
        assert render_resampling.resampling_name_for_kind(model_id=model_id, var_key="radar_ptype") == "nearest"
        assert render_resampling.rasterio_resampling_for_loop(model_id=model_id, var_key="radar_ptype").name == "nearest"


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


def test_targeted_accumulations_use_larger_loop_widths(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {
            "snowfall_total": SimpleNamespace(kind="continuous", color_map_id="snowfall_total"),
            "snowfall_kuchera_total": SimpleNamespace(kind="continuous", color_map_id="snowfall_total"),
            "precip_total": SimpleNamespace(kind="continuous", color_map_id="precip_total"),
            "tmp2m": SimpleNamespace(kind="continuous", color_map_id="tmp2m"),
        },
    )

    for model_id in ("hrrr", "nam", "nbm"):
        for var_key in ("snowfall_total", "snowfall_kuchera_total", "precip_total"):
            assert render_resampling.loop_fixed_width_for_tier(
                model_id=model_id,
                var_key=var_key,
                tier=0,
                default_width=1600,
            ) == 2300
            assert render_resampling.loop_fixed_width_for_tier(
                model_id=model_id,
                var_key=var_key,
                tier=1,
                default_width=2400,
            ) == 3400

    assert render_resampling.loop_fixed_width_for_tier(
        model_id="gfs",
        var_key="tmp2m",
        tier=0,
        default_width=1600,
    ) == 1600


def test_radar_ptype_uses_larger_tier0_loop_budget(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {"radar_ptype": SimpleNamespace(kind="indexed", color_map_id="radar_ptype")},
    )

    for model_id in ("hrrr", "nam"):
        assert render_resampling.loop_fixed_width_for_tier(
            model_id=model_id,
            var_key="radar_ptype",
            tier=0,
            default_width=1600,
        ) == 3072
        assert render_resampling.loop_fixed_width_for_tier(
            model_id=model_id,
            var_key="radar_ptype",
            tier=1,
            default_width=2400,
        ) == 3200
        assert render_resampling.loop_max_dim_for_tier(
            model_id=model_id,
            var_key="radar_ptype",
            tier=0,
            default_max_dim=1600,
        ) == 2048
        assert render_resampling.loop_quality_for_tier(
            model_id=model_id,
            var_key="radar_ptype",
            tier=0,
            default_quality=82,
        ) == 92
        assert render_resampling.loop_max_dim_for_tier(
            model_id=model_id,
            var_key="radar_ptype",
            tier=1,
            default_max_dim=2400,
        ) == 2400
        assert render_resampling.loop_quality_for_tier(
            model_id=model_id,
            var_key="radar_ptype",
            tier=1,
            default_quality=86,
        ) == 90


def test_loop_fixed_size_applied_for_all_continuous_models(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {"tmp2m": SimpleNamespace(kind="continuous", color_map_id="tmp2m")},
    )

    src_h, src_w = 148, 261
    for model_id, fixed_width in (("gfs", 1600), ("hrrr", 1600), ("nam", 1600), ("nbm", 1600)):
        out_h, out_w, fixed = render_resampling.compute_loop_output_shape(
            model_id=model_id,
            var_key="tmp2m",
            src_h=src_h,
            src_w=src_w,
            max_dim=1600,
            fixed_width=fixed_width,
        )

        assert fixed is True
        assert out_w == fixed_width
        assert out_h == int(round(src_h * (fixed_width / src_w)))


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

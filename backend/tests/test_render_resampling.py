from types import SimpleNamespace

from app.services import render_resampling


def _set_capabilities(monkeypatch, variable_catalog):
    capabilities = {"gfs": SimpleNamespace(variable_catalog=variable_catalog)}
    monkeypatch.setattr(render_resampling, "list_model_capabilities", lambda: capabilities)
    render_resampling._lookup_kind_from_capabilities.cache_clear()
    render_resampling._warned_unknown_kind.clear()
    render_resampling._unknown_kind_hits.clear()


def test_continuous_kind_maps_to_bilinear(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {"tmp2m": SimpleNamespace(kind="continuous")},
    )

    assert render_resampling.resampling_name_for_kind(model_id="gfs", var_key="tmp2m") == "bilinear"
    assert render_resampling.rio_tiler_resampling_kwargs(model_id="gfs", var_key="tmp2m") == {
        "resampling_method": "bilinear",
        "reproject_method": "bilinear",
    }


def test_discrete_kind_maps_to_nearest(monkeypatch):
    _set_capabilities(
        monkeypatch,
        {"precip_ptype": SimpleNamespace(kind="indexed")},
    )

    assert render_resampling.resampling_name_for_kind(model_id="gfs", var_key="precip_ptype") == "nearest"
    assert render_resampling.rasterio_resampling_for_loop(model_id="gfs", var_key="precip_ptype").name == "nearest"


def test_unknown_kind_falls_back_to_bilinear_and_warns(monkeypatch, caplog):
    _set_capabilities(
        monkeypatch,
        {"tmp2m": SimpleNamespace(kind=None)},
    )

    caplog.set_level("WARNING")
    first = render_resampling.resampling_name_for_kind(model_id="gfs", var_key="tmp2m")
    second = render_resampling.resampling_name_for_kind(model_id="gfs", var_key="tmp2m")

    assert first == "bilinear"
    assert second == "bilinear"
    assert render_resampling._unknown_kind_hits[("gfs", "tmp2m")] == 2
    assert "defaulting resampling to bilinear" in caplog.text

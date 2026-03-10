import io
import sys
from pathlib import Path

import numpy as np
from PIL import Image

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services import tile_server


class _FakeReader:
    def __init__(self, *, input: str):
        self.input = input

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def _png_bytes(width: int, height: int, rgba: tuple[int, int, int, int]) -> bytes:
    image = Image.new("RGBA", (width, height), rgba)
    buf = io.BytesIO()
    image.save(buf, format="PNG")
    return buf.getvalue()


def test_gfs_continuous_uses_value_cog_and_colorize(monkeypatch):
    calls: dict[str, object] = {}
    value_path = Path("/tmp/fake/fh000.val.cog.tif")

    monkeypatch.setattr(tile_server, "use_value_render_for_variable", lambda **kwargs: True)
    monkeypatch.setattr(tile_server, "variable_color_map_id", lambda model, var: "tmp2m")
    monkeypatch.setattr(tile_server, "_resolve_value_cog_path", lambda *args, **kwargs: value_path)
    monkeypatch.setattr(
        tile_server,
        "_resolve_cog_path",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("RGBA path should not be used")),
    )
    monkeypatch.setattr(tile_server, "Reader", _FakeReader)
    monkeypatch.setattr(tile_server, "_tile_is_fully_masked", lambda tile: False)

    fake_data = np.array([[[12.0, 18.0], [24.0, 30.0]]], dtype=np.float32)
    fake_mask = np.array([[255, 255], [255, 255]], dtype=np.uint8)

    class _FakeValueTile:
        data = fake_data
        mask = fake_mask

    def _fake_read_tile_compat(cog, *, x, y, z, indexes, resampling_method, reproject_method):
        calls["reader_input"] = str(getattr(cog, "input", ""))
        calls["indexes"] = indexes
        calls["resampling_method"] = resampling_method
        return _FakeValueTile()

    monkeypatch.setattr(tile_server, "_read_tile_compat", _fake_read_tile_compat)

    def _fake_colorize(data, color_map_id, *, meta_var_key=None, spec_override=None):
        calls["colorize_called"] = True
        calls["color_map_id"] = color_map_id
        calls["meta_var_key"] = meta_var_key
        assert data.shape == (2, 2)
        rgba = np.zeros((4, 2, 2), dtype=np.uint8)
        rgba[0, :, :] = 255
        rgba[3, :, :] = 255
        return rgba, {}

    monkeypatch.setattr(tile_server, "float_to_rgba", _fake_colorize)

    response = tile_server.get_tile("gfs", "20260224_12z", "tmp2m", 0, 0, 0, 0)

    assert response.status_code == 200
    assert response.media_type == "image/png"
    assert calls["reader_input"] == str(value_path)
    assert calls["indexes"] == (1,)
    assert calls["colorize_called"] is True
    assert calls["color_map_id"] == "tmp2m"
    assert calls["meta_var_key"] == "tmp2m"

    image = Image.open(io.BytesIO(response.body))
    arr = np.asarray(image)
    assert arr.shape == (2, 2, 4)
    assert arr.dtype == np.uint8


def test_non_gfs_continuous_stays_on_rgba_path(monkeypatch):
    rgba_path = Path("/tmp/fake/fh000.rgba.cog.tif")
    rgba_png = _png_bytes(2, 2, (0, 128, 255, 255))
    calls: dict[str, object] = {}

    monkeypatch.setattr(tile_server, "use_value_render_for_variable", lambda **kwargs: False)
    monkeypatch.setattr(tile_server, "_resolve_cog_path", lambda *args, **kwargs: rgba_path)
    monkeypatch.setattr(
        tile_server,
        "_resolve_value_cog_path",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Value COG path should not be used")),
    )
    monkeypatch.setattr(tile_server, "Reader", _FakeReader)
    monkeypatch.setattr(tile_server, "_tile_is_fully_masked", lambda tile: False)
    monkeypatch.setattr(
        tile_server,
        "float_to_rgba",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Colorize should not run on RGBA path")),
    )

    class _FakeRgbaTile:
        pass

    def _fake_read_tile_compat(cog, *, x, y, z, indexes=(1, 2, 3, 4), resampling_method, reproject_method):
        calls["reader_input"] = str(getattr(cog, "input", ""))
        calls["indexes"] = indexes
        return _FakeRgbaTile()

    monkeypatch.setattr(tile_server, "_read_tile_compat", _fake_read_tile_compat)
    monkeypatch.setattr(tile_server, "_render_png_compat", lambda tile: rgba_png)

    response = tile_server.get_tile("hrrr", "20260224_12z", "tmp2m", 0, 0, 0, 0)

    assert response.status_code == 200
    assert response.media_type == "image/png"
    assert response.body == rgba_png
    assert calls["reader_input"] == str(rgba_path)
    assert calls["indexes"] == (1, 2, 3, 4)


def test_targeted_hrrr_snowfall_uses_value_cog_and_bilinear_sampling(monkeypatch):
    calls: dict[str, object] = {}
    value_path = Path("/tmp/fake/hrrr/fh000.val.cog.tif")

    monkeypatch.setattr(tile_server, "use_value_render_for_variable", lambda **kwargs: True)
    monkeypatch.setattr(tile_server, "variable_color_map_id", lambda model, var: "snowfall_total")
    monkeypatch.setattr(tile_server, "_resolve_value_cog_path", lambda *args, **kwargs: value_path)
    monkeypatch.setattr(
        tile_server,
        "_resolve_cog_path",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("RGBA path should not be used")),
    )
    monkeypatch.setattr(tile_server, "Reader", _FakeReader)
    monkeypatch.setattr(tile_server, "_tile_is_fully_masked", lambda tile: False)

    fake_data = np.array([[[1.5, 2.5], [3.5, 4.5]]], dtype=np.float32)
    fake_mask = np.array([[255, 255], [255, 255]], dtype=np.uint8)

    class _FakeValueTile:
        data = fake_data
        mask = fake_mask

    def _fake_read_tile_compat(cog, *, x, y, z, indexes, resampling_method, reproject_method):
        calls["reader_input"] = str(getattr(cog, "input", ""))
        calls["indexes"] = indexes
        calls["resampling_method"] = resampling_method
        calls["reproject_method"] = reproject_method
        return _FakeValueTile()

    monkeypatch.setattr(tile_server, "_read_tile_compat", _fake_read_tile_compat)

    def _fake_colorize(data, color_map_id, *, meta_var_key=None, spec_override=None):
        calls["colorize_called"] = True
        calls["color_map_id"] = color_map_id
        calls["meta_var_key"] = meta_var_key
        rgba = np.zeros((4, 2, 2), dtype=np.uint8)
        rgba[2, :, :] = 255
        rgba[3, :, :] = 255
        return rgba, {}

    monkeypatch.setattr(tile_server, "float_to_rgba", _fake_colorize)

    response = tile_server.get_tile("hrrr", "20260224_12z", "snowfall_total", 0, 0, 0, 0)

    assert response.status_code == 200
    assert response.media_type == "image/png"
    assert calls["reader_input"] == str(value_path)
    assert calls["indexes"] == (1,)
    assert calls["resampling_method"] == "bilinear"
    assert calls["reproject_method"] == "bilinear"
    assert calls["colorize_called"] is True
    assert calls["color_map_id"] == "snowfall_total"
    assert calls["meta_var_key"] == "snowfall_total"

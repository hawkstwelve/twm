import os
import sys
from pathlib import Path

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# Required by backend.app.auth.twf_oauth import side effects used by app.main.
os.environ.setdefault("TWF_BASE", "https://example.com")
os.environ.setdefault("TWF_CLIENT_ID", "test-client")
os.environ.setdefault("TWF_CLIENT_SECRET", "test-secret")
os.environ.setdefault("TWF_REDIRECT_URI", "https://example.com/callback")
os.environ.setdefault("TWF_SCOPES", "profile forums_posts")
os.environ.setdefault("FRONTEND_RETURN", "https://example.com/models-v3")
os.environ.setdefault("TOKEN_DB_PATH", "/tmp/twf_oauth_test.sqlite3")
os.environ.setdefault("TOKEN_ENC_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")

from app import main as main_module


class _FakeDataset:
    def __init__(self, *, width: int, height: int, read_fn):
        self.width = width
        self.height = height
        self._read_fn = read_fn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self, indexes, out_shape=None, resampling=None):
        return self._read_fn(indexes=indexes, out_shape=out_shape, resampling=resampling)


def test_maybe_unsharp_rgba_disable_is_noop():
    rgba = np.zeros((12, 16, 4), dtype=np.uint8)
    rgba[..., 0] = 40
    rgba[..., 1] = 90
    rgba[..., 2] = 140
    rgba[..., 3] = 200

    out = main_module._maybe_unsharp_rgba(rgba, enable=False)
    assert np.array_equal(out, rgba)


def test_maybe_unsharp_rgba_preserves_alpha_shape_and_dtype():
    yy, xx = np.indices((18, 24))
    rgba = np.zeros((18, 24, 4), dtype=np.uint8)
    rgba[..., 0] = np.where(xx < 12, 10, 240).astype(np.uint8)
    rgba[..., 1] = np.where(yy < 9, 20, 220).astype(np.uint8)
    rgba[..., 2] = ((xx * 7 + yy * 9) % 256).astype(np.uint8)
    rgba[..., 3] = ((xx * 13 + yy * 5) % 256).astype(np.uint8)

    out = main_module._maybe_unsharp_rgba(
        rgba,
        enable=True,
        radius=1.2,
        percent=35,
        threshold=3,
    )
    assert out.shape == rgba.shape
    assert out.dtype == np.uint8
    assert np.array_equal(out[..., 3], rgba[..., 3])


def test_render_loop_webp_bytes_uses_value_render_for_gfs_continuous(tmp_path, monkeypatch):
    cog_path = tmp_path / "fh000.rgba.cog.tif"
    val_path = tmp_path / "fh000.val.cog.tif"
    cog_path.write_bytes(b"rgba")
    val_path.write_bytes(b"val")

    calls: dict[str, object] = {"rgba_read": False}

    monkeypatch.setattr(main_module, "use_value_render_for_variable", lambda **kwargs: True)
    monkeypatch.setattr(main_module, "variable_color_map_id", lambda model_id, var_key: "tmp2m")
    monkeypatch.setattr(main_module, "rasterio_resampling_for_loop", lambda **kwargs: "bilinear")

    def _fake_open(path, *args, **kwargs):
        path = Path(path)
        if path == cog_path:
            return _FakeDataset(
                width=8,
                height=4,
                read_fn=lambda indexes, out_shape, resampling: (
                    calls.__setitem__("rgba_read", True)
                    or np.zeros(out_shape or (4, 4, 8), dtype=np.uint8)
                ),
            )
        if path == val_path:
            return _FakeDataset(
                width=2,
                height=1,
                read_fn=lambda indexes, out_shape, resampling: np.full(out_shape, 12.5, dtype=np.float32),
            )
        raise AssertionError(f"Unexpected open path: {path}")

    monkeypatch.setattr(main_module.rasterio, "open", _fake_open)

    def _fake_float_to_rgba(data, color_map_id, *, meta_var_key=None, spec_override=None):
        calls["colorize_called"] = True
        calls["color_map_id"] = color_map_id
        calls["meta_var_key"] = meta_var_key
        h, w = data.shape
        rgba = np.zeros((4, h, w), dtype=np.uint8)
        rgba[1, :, :] = 200
        rgba[3, :, :] = 255
        return rgba, {}

    monkeypatch.setattr(main_module, "float_to_rgba", _fake_float_to_rgba)

    content = main_module._render_loop_webp_bytes(
        cog_path,
        model_id="gfs",
        var_key="tmp2m",
        tier=0,
        value_cog_path=val_path,
    )

    assert content is not None
    assert content.startswith(b"RIFF")
    assert calls["colorize_called"] is True
    assert calls["color_map_id"] == "tmp2m"
    assert calls["meta_var_key"] == "tmp2m"
    assert calls["rgba_read"] is False


def test_render_loop_webp_bytes_uses_rgba_when_gate_false(tmp_path, monkeypatch):
    cog_path = tmp_path / "fh000.rgba.cog.tif"
    val_path = tmp_path / "fh000.val.cog.tif"
    cog_path.write_bytes(b"rgba")
    val_path.write_bytes(b"val")

    calls: dict[str, object] = {"rgba_read": False}

    monkeypatch.setattr(main_module, "use_value_render_for_variable", lambda **kwargs: False)
    monkeypatch.setattr(main_module, "rasterio_resampling_for_loop", lambda **kwargs: "bilinear")
    monkeypatch.setattr(
        main_module,
        "float_to_rgba",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("float_to_rgba should not be called")),
    )

    def _fake_open(path, *args, **kwargs):
        path = Path(path)
        if path == cog_path:
            return _FakeDataset(
                width=8,
                height=4,
                read_fn=lambda indexes, out_shape, resampling: (
                    calls.__setitem__("rgba_read", True)
                    or np.full(out_shape or (4, 4, 8), 255, dtype=np.uint8)
                ),
            )
        raise AssertionError(f"Unexpected open path: {path}")

    monkeypatch.setattr(main_module.rasterio, "open", _fake_open)

    content = main_module._render_loop_webp_bytes(
        cog_path,
        model_id="hrrr",
        var_key="tmp2m",
        tier=0,
        value_cog_path=val_path,
    )

    assert content is not None
    assert content.startswith(b"RIFF")
    assert calls["rgba_read"] is True

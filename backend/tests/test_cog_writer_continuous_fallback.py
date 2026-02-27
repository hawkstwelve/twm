from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.builder import cog_writer


def test_continuous_rgba_falls_back_when_vrt_copy_loses_overviews(
    monkeypatch,
    tmp_path: Path,
) -> None:
    bbox, grid_m = cog_writer.get_grid_params("gfs", "pnw")
    _, height, width = cog_writer.compute_transform_and_shape(bbox, grid_m)
    rgba = np.zeros((4, height, width), dtype=np.uint8)
    out_path = tmp_path / "fh000.rgba.cog.tif"

    called = {
        "continuous": 0,
        "write_base": 0,
        "gdaladdo": 0,
        "translate": 0,
    }

    def fake_continuous_build(
        rgba_in: np.ndarray,
        tmp_dir: Path,
        output_path: Path,
        transform,
        levels: list[int],
    ) -> Path:
        del rgba_in, tmp_dir, transform, levels
        called["continuous"] += 1
        output_path.touch()
        return output_path

    def fake_write_base(*args, **kwargs) -> None:
        del args, kwargs
        called["write_base"] += 1

    def fake_run_gdal(cmd: list[str]) -> None:
        if len(cmd) > 0 and "gdaladdo" in str(cmd[0]):
            called["gdaladdo"] += 1

    def fake_translate(src: Path, dst: Path) -> None:
        del src
        called["translate"] += 1
        dst.touch()

    monkeypatch.setattr(cog_writer, "_build_continuous_rgba_cog", fake_continuous_build)
    monkeypatch.setattr(cog_writer, "_cog_has_overviews", lambda path: False)
    monkeypatch.setattr(cog_writer, "_write_base_gtiff", fake_write_base)
    monkeypatch.setattr(cog_writer, "_run_gdal", fake_run_gdal)
    monkeypatch.setattr(cog_writer, "_gtiff_to_cog", fake_translate)
    monkeypatch.setattr(cog_writer, "_gdal", lambda name: name)

    cog_writer.write_rgba_cog(rgba, out_path, model="gfs", region="pnw", kind="continuous")

    assert called["continuous"] == 1
    assert called["write_base"] == 1
    assert called["gdaladdo"] == 1
    assert called["translate"] == 1

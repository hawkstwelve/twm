from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.builder import cog_writer


def test_continuous_rgba_uses_two_pass_same_source_policy(
    monkeypatch,
    tmp_path: Path,
) -> None:
    bbox, grid_m = cog_writer.get_grid_params("gfs", "pnw")
    _, height, width = cog_writer.compute_transform_and_shape(bbox, grid_m)
    rgba = np.zeros((4, height, width), dtype=np.uint8)
    out_path = tmp_path / "fh000.rgba.cog.tif"

    called = {"write_base": 0, "translate": 0}
    gdal_commands: list[list[str]] = []

    def fake_write_base(*args, **kwargs) -> None:
        del args, kwargs
        called["write_base"] += 1

    def fake_run_gdal(cmd: list[str]) -> None:
        gdal_commands.append(list(cmd))

    def fake_translate(src: Path, dst: Path) -> None:
        del src
        called["translate"] += 1
        dst.touch()

    monkeypatch.setattr(cog_writer, "_write_base_gtiff", fake_write_base)
    monkeypatch.setattr(cog_writer, "_run_gdal", fake_run_gdal)
    monkeypatch.setattr(cog_writer, "_gtiff_to_cog", fake_translate)
    monkeypatch.setattr(cog_writer, "_gdal", lambda name: name)

    cog_writer.write_rgba_cog(rgba, out_path, model="gfs", region="pnw", kind="continuous")

    assert called["write_base"] == 1
    assert called["translate"] == 1

    gdaladdo_cmds = [cmd for cmd in gdal_commands if len(cmd) > 0 and cmd[0] == "gdaladdo"]
    assert len(gdaladdo_cmds) == 2

    def _source_path_arg(cmd: list[str]) -> str:
        for token in cmd:
            if token.endswith(".tif"):
                return token
        return ""
    first = gdaladdo_cmds[0]
    second = gdaladdo_cmds[1]

    assert _source_path_arg(first).endswith("rgba_base.tif")
    assert _source_path_arg(second).endswith("rgba_base.tif")
    assert first[first.index("-r") + 1] == "nearest"
    assert second[second.index("-r") + 1] == "average"

    first_bands = [first[i + 1] for i, token in enumerate(first[:-1]) if token == "-b"]
    second_bands = [second[i + 1] for i, token in enumerate(second[:-1]) if token == "-b"]
    assert first_bands == ["4"]
    assert second_bands == ["1", "2", "3"]

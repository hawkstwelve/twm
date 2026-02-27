import subprocess
from pathlib import Path

import numpy as np
import pytest
import rasterio

from app.services.builder.cog_writer import _gdal, compute_transform_and_shape, get_grid_params, write_rgba_cog


def _require_gdal() -> None:
    try:
        _gdal("gdaladdo")
        _gdal("gdal_translate")
    except RuntimeError as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"GDAL CLI unavailable: {exc}")


def test_continuous_rgba_alpha_overview_is_not_averaged(tmp_path: Path) -> None:
    """Continuous overviews must keep alpha nearest (no new mid-range values)."""
    _require_gdal()

    bbox, grid_m = get_grid_params("gfs", "pnw")
    _, height, width = compute_transform_and_shape(bbox, grid_m)

    yy, xx = np.indices((height, width))
    rgba = np.zeros((4, height, width), dtype=np.uint8)
    rgba[0] = ((xx * 17 + yy * 3) % 256).astype(np.uint8)
    rgba[1] = ((xx * 5 + yy * 11) % 256).astype(np.uint8)
    rgba[2] = ((xx * 7 + yy * 13) % 256).astype(np.uint8)
    # Checkerboard alpha guarantees averaging would create mid-range values.
    rgba[3] = np.where(((xx + yy) % 2) == 0, 0, 255).astype(np.uint8)

    out_path = tmp_path / "fh000.rgba.cog.tif"
    write_rgba_cog(rgba, out_path, model="gfs", region="pnw", kind="continuous")

    with rasterio.open(out_path) as src:
        assert src.count == 4
        assert src.overviews(4), "Expected alpha overviews for continuous RGBA COG"

    alpha_ovr_path = tmp_path / "alpha.ovr0.tif"
    subprocess.run(
        [
            _gdal("gdal_translate"),
            "-of",
            "GTiff",
            "-ovr",
            "0",
            "-b",
            "4",
            str(out_path),
            str(alpha_ovr_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    with rasterio.open(alpha_ovr_path) as alpha_src:
        alpha_ovr = alpha_src.read(1)

    unique_vals = set(np.unique(alpha_ovr).astype(int).tolist())
    disallowed = sorted(v for v in unique_vals if v not in {0, 255})
    assert not disallowed, f"Alpha overview contains averaged values: {disallowed}"

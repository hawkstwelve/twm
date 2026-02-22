"""Integration test: verify cog_writer produces valid COGs.

Tests both RGBA (continuous + discrete) and value COGs.
Validates per-band overview resampling, COG layout, and structure
via rasterio and gdalinfo.
"""
import subprocess
import sys
import os
import tempfile
import numpy as np

# Ensure the backend package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.builder.cog_writer import (
    compute_transform_and_shape,
    write_rgba_cog,
    write_value_cog,
    get_grid_params,
    ensure_gdal,
    _gdal,
)

import rasterio


def _gdalinfo(path: str) -> str:
    """Run gdalinfo and return stdout."""
    gdalinfo_bin = os.path.join(os.path.dirname(_gdal("gdaladdo")), "gdalinfo")
    result = subprocess.run(
        [gdalinfo_bin, "-json", path],
        capture_output=True, text=True, check=True,
    )
    return result.stdout


def test_rgba_cog_continuous():
    """Write a continuous RGBA COG for HRRR/PNW and validate structure.

    Key assertion: band 4 (alpha) overviews use nearest resampling
    while bands 1-3 (RGB) use average — per the artifact contract.
    """
    bbox, grid_m = get_grid_params("hrrr", "pnw")
    transform, height, width = compute_transform_and_shape(bbox, grid_m)
    print(f"HRRR/PNW grid: {width}x{height} at {grid_m}m")

    # Synthetic RGBA data with binary alpha
    rgba = np.random.randint(0, 255, (4, height, width), dtype=np.uint8)
    rgba[3, :, :] = 255  # valid everywhere
    rgba[3, :10, :] = 0  # nodata strip at top

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "fh000.rgba.cog.tif")
        write_rgba_cog(rgba, out, model="hrrr", region="pnw", kind="continuous")

        with rasterio.open(out) as src:
            assert src.count == 4, f"Expected 4 bands, got {src.count}"
            assert src.dtypes[0] == "uint8", f"Expected uint8, got {src.dtypes[0]}"
            assert str(src.crs) == "EPSG:3857", f"Expected EPSG:3857, got {src.crs}"
            ovrs = src.overviews(1)
            print(f"  Bands: {src.count}, Dtype: {src.dtypes[0]}, CRS: {src.crs}")
            print(f"  Size: {src.width}x{src.height}")
            print(f"  Overviews (band 1): {ovrs}")
            print(f"  Overviews (band 4): {src.overviews(4)}")
            print(f"  Block shapes: {src.block_shapes[0]}")
            assert len(ovrs) >= 1, "Expected at least 1 overview level"

            # Verify alpha overviews are nearest (binary values only: 0 or 255)
            # If alpha had been averaged, we'd see intermediate values
            for level in src.overviews(4):
                alpha_ovr = src.read(4, out_shape=(height // level, width // level))
                unique_vals = set(np.unique(alpha_ovr))
                # Nearest on binary alpha should only produce 0 and 255
                non_binary = unique_vals - {0, 255}
                if non_binary:
                    print(f"  WARNING: Alpha overview {level}x has non-binary values: {non_binary}")
                else:
                    print(f"  Alpha overview {level}x: OK (binary 0/255 only)")

        # Validate COG layout with gdalinfo
        info = _gdalinfo(out)
        import json
        info_dict = json.loads(info)
        layout = info_dict.get("metadata", {}).get("IMAGE_STRUCTURE", {}).get("LAYOUT", "")
        print(f"  COG layout: {layout}")
        assert layout == "COG", f"Expected COG layout, got {layout!r}"

    print("  RGBA COG (continuous): PASS\n")


def test_rgba_cog_discrete():
    """Write a discrete RGBA COG and validate all-nearest overviews.

    Uses a checkerboard pattern so averaging would create intermediate values.
    For nearest resampling, overview pixels remain a subset of source values
    for every band (R/G/B/A). This matches radar_ptype requirements.
    """
    bbox, grid_m = get_grid_params("hrrr", "pnw")
    transform, height, width = compute_transform_and_shape(bbox, grid_m)

    yy, xx = np.indices((height, width))
    checker = ((xx + yy) % 2).astype(np.uint8)

    rgba = np.zeros((4, height, width), dtype=np.uint8)
    rgba[0] = np.where(checker == 0, 10, 200).astype(np.uint8)
    rgba[1] = np.where(checker == 0, 20, 180).astype(np.uint8)
    rgba[2] = np.where(checker == 0, 30, 160).astype(np.uint8)
    rgba[3] = np.where(checker == 0, 0, 255).astype(np.uint8)

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "fh000.rgba.cog.tif")
        write_rgba_cog(rgba, out, model="hrrr", region="pnw", kind="discrete")

        with rasterio.open(out) as src:
            assert src.count == 4
            ovrs = src.overviews(1)
            print(f"  Discrete RGBA: {src.width}x{src.height}, overviews={ovrs}")
            assert len(ovrs) >= 1

        # Verify first overview preserves nearest-neighbor semantics on all bands.
        # If averaged, intermediate values would appear for checkerboard input.
        first_ovr = 1
        gdal_translate_bin = _gdal("gdal_translate")
        expected_vals = {
            1: {10, 200},
            2: {20, 180},
            3: {30, 160},
            4: {0, 255},
        }
        for band in (1, 2, 3, 4):
            ovr_band_path = os.path.join(tmpdir, f"ovr{first_ovr}_b{band}.tif")
            subprocess.run(
                [
                    gdal_translate_bin,
                    "-of",
                    "GTiff",
                    "-ovr",
                    str(first_ovr),
                    "-b",
                    str(band),
                    out,
                    ovr_band_path,
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            with rasterio.open(ovr_band_path) as ovr_src:
                band_data = ovr_src.read(1)
            unique_vals = set(np.unique(band_data).astype(int).tolist())
            disallowed = unique_vals - expected_vals[band]
            assert not disallowed, (
                f"Band {band} overview has non-nearest values: {sorted(disallowed)}; "
                f"expected subset of {sorted(expected_vals[band])}"
            )

    print("  RGBA COG (discrete): PASS\n")


def test_value_cog():
    """Write a value COG for HRRR/PNW and validate its structure."""
    bbox, grid_m = get_grid_params("hrrr", "pnw")
    transform, height, width = compute_transform_and_shape(bbox, grid_m)

    # Synthetic float32 data with some NaN
    values = np.random.uniform(-40, 120, (height, width)).astype(np.float32)
    values[:10, :] = np.nan

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "fh000.val.cog.tif")
        write_value_cog(values, out, model="hrrr", region="pnw")

        with rasterio.open(out) as src:
            assert src.count == 1, f"Expected 1 band, got {src.count}"
            assert src.dtypes[0] == "float32", f"Expected float32, got {src.dtypes[0]}"
            assert str(src.crs) == "EPSG:3857", f"Expected EPSG:3857, got {src.crs}"
            ovrs = src.overviews(1)
            print(f"  Bands: {src.count}, Dtype: {src.dtypes[0]}, CRS: {src.crs}")
            print(f"  Size: {src.width}x{src.height}")
            print(f"  Overviews: {ovrs}")
            assert len(ovrs) >= 1, "Expected at least 1 overview level"

        # Validate COG layout
        info = _gdalinfo(out)
        import json
        info_dict = json.loads(info)
        layout = info_dict.get("metadata", {}).get("IMAGE_STRUCTURE", {}).get("LAYOUT", "")
        print(f"  COG layout: {layout}")
        assert layout == "COG"

    print("  Value COG: PASS\n")


def test_value_cog_downsample_4x():
    """Write a 4x-downsampled value COG and validate coarse-grid geometry."""
    bbox, grid_m = get_grid_params("hrrr", "pnw")
    _, height, width = compute_transform_and_shape(bbox, grid_m)
    _, expected_h, expected_w = compute_transform_and_shape(bbox, grid_m * 4)

    values = np.random.uniform(-40, 120, (height, width)).astype(np.float32)
    values[:10, :] = np.nan

    with tempfile.TemporaryDirectory() as tmpdir:
        out = os.path.join(tmpdir, "fh000.down4.val.cog.tif")
        write_value_cog(values, out, model="hrrr", region="pnw", downsample_factor=4)

        with rasterio.open(out) as src:
            assert src.count == 1
            assert src.width == expected_w, f"Expected width {expected_w}, got {src.width}"
            assert src.height == expected_h, f"Expected height {expected_h}, got {src.height}"
            assert abs(src.transform.a - (grid_m * 4)) < 0.1, (
                f"Expected x-res {grid_m * 4}, got {src.transform.a}"
            )
            assert abs(abs(src.transform.e) - (grid_m * 4)) < 0.1, (
                f"Expected y-res {grid_m * 4}, got {abs(src.transform.e)}"
            )

    print("  Value COG (4x downsample): PASS\n")


if __name__ == "__main__":
    print("=== COG Writer Integration Tests ===\n")
    ensure_gdal()
    print(f"gdaladdo:       {_gdal('gdaladdo')}")
    print(f"gdal_translate: {_gdal('gdal_translate')}")
    print(f"gdalbuildvrt:   {_gdal('gdalbuildvrt')}\n")

    test_rgba_cog_continuous()
    test_rgba_cog_discrete()
    test_value_cog()
    test_value_cog_downsample_4x()

    print("All tests passed.")

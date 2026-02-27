"""COG writer: numpy arrays → Cloud Optimized GeoTIFF files.

Produces two artifact types per the V3 artifact contract:
  - RGBA COG: 4-band uint8, EPSG:3857, 512×512 tiles, internal overviews
  - Value COG: 1-band float32, EPSG:3857, 512×512 tiles, internal overviews

All output files share a pixel-aligned grid for a given model/region,
guaranteed by the use of fixed bounding boxes and target-aligned pixels.

Overview strategy:
  - continuous RGBA: average for RGB, nearest for alpha
  - discrete/indexed RGBA: nearest for all bands
  - value: nearest

Overviews are built with gdaladdo (subprocess). Final COG is produced with
gdal_translate -of COG.

Grid constants are defined in this module —
the rest of the builder imports them from here.
"""

from __future__ import annotations

import logging
import math
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import rasterio
import rasterio.crs
import rasterio.transform
from rasterio.enums import Resampling
from rasterio.transform import from_origin
from rasterio.warp import reproject

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Region bounding boxes (EPSG:3857) — authoritative, from ROADMAP_V3
# Format: (west, south, east, north) = (xmin, ymin, xmax, ymax)
# ---------------------------------------------------------------------------

REGION_BBOX_3857: dict[str, tuple[float, float, float, float]] = {
    "conus": (-13914936.35, 2764607.34, -7403013.94, 6446275.84),
    "pnw": (-14026255.80, 5096324.37, -12913060.93, 6378137.00),
}

# WGS84 bounding boxes (for reference / coordinate transforms)
REGION_BBOX_4326: dict[str, tuple[float, float, float, float]] = {
    "conus": (-125.0, 24.0, -66.5, 50.0),
    "pnw": (-126.0, 41.5, -116.0, 49.5),
}

# ---------------------------------------------------------------------------
# Target grid resolution (meters) per model/region
# All variables for a given model/region share an identical pixel grid.
# ---------------------------------------------------------------------------

# Legacy fallback only. Authoritative grid ownership is model capabilities
# (`ModelCapabilities.grid_meters_by_region`).
TARGET_GRID_METERS: dict[str, dict[str, float]] = {
    "hrrr": {
        "conus": 3_000.0,
        "pnw": 3_000.0,
    },
    "gfs": {
        "conus": 25_000.0,
        "pnw": 25_000.0,
    },
    "ecmwf": {
        "conus": 9_000.0,
    },
}

# Internal tile size for all COGs
COG_BLOCKSIZE = 512

# Compression for all COGs
COG_COMPRESS = "deflate"


# ---------------------------------------------------------------------------
# GDAL CLI discovery
# ---------------------------------------------------------------------------

def _find_gdal_tool(name: str) -> str:
    """Locate a GDAL CLI tool, returning its absolute path.

    Checks PATH first, then common Homebrew / system locations.
    Raises RuntimeError if not found.
    """
    path = shutil.which(name)
    if path:
        return path
    # Fallback: common install locations
    for prefix in ("/opt/homebrew/bin", "/usr/local/bin", "/usr/bin"):
        candidate = f"{prefix}/{name}"
        if Path(candidate).is_file():
            return candidate
    raise RuntimeError(
        f"GDAL tool '{name}' not found. Install GDAL CLI tools "
        f"(e.g. `brew install gdal` on macOS, `apt install gdal-bin` on Linux)."
    )


# ---------------------------------------------------------------------------
# Lazy GDAL tool resolution — avoids crashing on import in environments
# that don't have GDAL CLI installed (CI, unit tests, minimal containers).
# Resolved on first use by the write functions.
# ---------------------------------------------------------------------------

_gdal_tools: dict[str, str] = {}


def _gdal(name: str) -> str:
    """Return the absolute path for a GDAL CLI tool, resolving lazily."""
    if name not in _gdal_tools:
        _gdal_tools[name] = _find_gdal_tool(name)
        logger.info("Resolved GDAL tool: %s → %s", name, _gdal_tools[name])
    return _gdal_tools[name]


def ensure_gdal() -> None:
    """Eagerly resolve all required GDAL CLI tools.

    Call this at startup if you want fast-fail instead of lazy discovery.
    Optional — the write functions call _gdal() on first use regardless.
    """
    for tool in ("gdaladdo", "gdal_translate"):
        _gdal(tool)


def get_grid_params(
    model: str,
    region: str,
) -> tuple[tuple[float, float, float, float], float]:
    """Return (bbox_3857, grid_meters) for a model/region pair.

    Raises KeyError if the combination is not defined.
    """
    bbox = REGION_BBOX_3857.get(region)
    if bbox is None:
        raise KeyError(f"Unknown region: {region!r}")
    grid_m = _grid_meters_from_capabilities(model, region)
    if grid_m is None:
        model_grids = TARGET_GRID_METERS.get(model)
        if model_grids is None:
            raise KeyError(f"Unknown model: {model!r}")
        grid_m = model_grids.get(region)
    if grid_m is None:
        raise KeyError(f"No grid resolution defined for {model!r}/{region!r}")
    return bbox, grid_m


def _grid_meters_from_capabilities(model: str, region: str) -> float | None:
    try:
        from app.models.registry import MODEL_REGISTRY
    except Exception:
        return None
    plugin = MODEL_REGISTRY.get(model)
    if plugin is None:
        return None
    capabilities = getattr(plugin, "capabilities", None)
    if capabilities is None:
        return None
    grid_map = getattr(capabilities, "grid_meters_by_region", None)
    if not isinstance(grid_map, dict):
        return None
    value = grid_map.get(region)
    if value is None:
        return None
    return float(value)


def compute_transform_and_shape(
    bbox_3857: tuple[float, float, float, float],
    grid_meters: float,
) -> tuple[rasterio.transform.Affine, int, int]:
    """Compute the affine transform and pixel dimensions for a target grid.

    Uses target-aligned pixels (equivalent to gdalwarp -tap): the grid origin
    is snapped to a multiple of grid_meters, guaranteeing that all COGs for
    the same model/region are pixel-aligned.

    Returns (transform, height, width).
    """
    xmin, ymin, xmax, ymax = bbox_3857
    res = grid_meters

    # Snap to target-aligned pixels (equivalent to -tap)
    aligned_xmin = math.floor(xmin / res) * res
    aligned_ymax = math.ceil(ymax / res) * res
    aligned_xmax = math.ceil(xmax / res) * res
    aligned_ymin = math.floor(ymin / res) * res

    width = round((aligned_xmax - aligned_xmin) / res)
    height = round((aligned_ymax - aligned_ymin) / res)

    # from_origin expects (west, north, xres, yres)
    transform = from_origin(aligned_xmin, aligned_ymax, res, res)

    return transform, height, width


def _overview_levels(height: int, width: int) -> list[int]:
    """Compute overview levels (powers of 2), including small grids.

    Contract requires internal overviews. For compact domains (e.g. GFS PNW),
    still emit at least a 2x overview when possible.
    """
    max_dim = max(height, width)
    if max_dim < 2:
        return []

    levels = []
    factor = 2
    while max_dim // factor >= 128:
        levels.append(factor)
        factor *= 2

    # Always have at least one overview level for any grid that can be downsampled.
    if not levels:
        levels.append(2)
    return levels


# ---------------------------------------------------------------------------
# RGBA COG writer
# ---------------------------------------------------------------------------


def write_rgba_cog(
    rgba: np.ndarray,
    output_path: Path | str,
    *,
    model: str,
    region: str,
    kind: str = "continuous",
) -> Path:
    """Write a 4-band RGBA uint8 array as a Cloud Optimized GeoTIFF.

    Parameters
    ----------
    rgba : np.ndarray
        Shape (4, H, W), dtype uint8. Band order: R, G, B, A.
    output_path : Path or str
        Destination file path. Parent directories are created if needed.
    model : str
        Model id (e.g. "hrrr") — used to look up grid parameters.
    region : str
        Region id (e.g. "pnw") — used to look up grid parameters.
        kind : str
        "continuous" or "discrete" / "indexed" — controls overview resampling.
        Current overview strategy:
          continuous → average RGB + nearest alpha
          discrete/indexed → nearest (all bands)

    Returns
    -------
    Path to the written COG file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if rgba.ndim != 3 or rgba.shape[0] != 4:
        raise ValueError(f"rgba must be shape (4, H, W), got {rgba.shape}")
    if rgba.dtype != np.uint8:
        raise ValueError(f"rgba must be uint8, got {rgba.dtype}")

    bbox, grid_m = get_grid_params(model, region)
    transform, expected_h, expected_w = compute_transform_and_shape(bbox, grid_m)

    _, data_h, data_w = rgba.shape
    if data_h != expected_h or data_w != expected_w:
        raise ValueError(
            f"RGBA array shape ({data_h}, {data_w}) does not match expected "
            f"grid ({expected_h}, {expected_w}) for {model}/{region} at {grid_m}m"
        )

    levels = _overview_levels(data_h, data_w)

    with tempfile.TemporaryDirectory(dir=output_path.parent) as tmp_dir:
        tmp_dir_path = Path(tmp_dir)

        if kind == "continuous" and levels:
            _build_continuous_rgba_cog(
                rgba, tmp_dir_path, output_path, transform, levels,
            )
        else:
            # Discrete/indexed or no overviews: simple single-file path
            tmp_gtiff = tmp_dir_path / "base.tif"
            _write_base_gtiff(
                data=rgba, path=tmp_gtiff, transform=transform,
                count=4, dtype="uint8", nodata=None,
            )
            if levels:
                _run_gdal([
                    _gdal("gdaladdo"), "-r", "nearest",
                    "--config", "GDAL_TIFF_OVR_BLOCKSIZE", str(COG_BLOCKSIZE),
                    str(tmp_gtiff), *[str(l) for l in levels],
                ])
            _gtiff_to_cog(tmp_gtiff, output_path)

    logger.info(
        "Wrote RGBA COG: %s (%dx%d, %d overviews, kind=%s)",
        output_path, data_w, data_h, len(levels), kind,
    )
    return output_path


# ---------------------------------------------------------------------------
# Value COG writer
# ---------------------------------------------------------------------------


def write_value_cog(
    values: np.ndarray,
    output_path: Path | str,
    *,
    model: str,
    region: str,
    nodata: float = float("nan"),
    downsample_factor: int = 1,
) -> Path:
    """Write a single-band float32 array as a Cloud Optimized GeoTIFF.

    Parameters
    ----------
    values : np.ndarray
        Shape (H, W), dtype float32. NaN = nodata.
    output_path : Path or str
        Destination file path.
    model, region : str
        Used to look up grid parameters (bbox + resolution).
    nodata : float
        Nodata value. Defaults to NaN.
    downsample_factor : int
        Optional integer factor for coarser value-grid output (e.g. 4 means
        4x larger pixel size and ~1/16 pixel count). Intended for lightweight
        hover sampling payloads while keeping the same model/region extent.

    Returns
    -------
    Path to the written COG file.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if values.ndim != 2:
        raise ValueError(f"values must be shape (H, W), got {values.shape}")
    if downsample_factor < 1:
        raise ValueError(f"downsample_factor must be >= 1, got {downsample_factor}")

    values_f32 = values.astype(np.float32, copy=False)
    data_h, data_w = values_f32.shape

    bbox, grid_m = get_grid_params(model, region)
    base_transform, expected_h, expected_w = compute_transform_and_shape(bbox, grid_m)

    if data_h != expected_h or data_w != expected_w:
        raise ValueError(
            f"Value array shape ({data_h}, {data_w}) does not match expected "
            f"grid ({expected_h}, {expected_w}) for {model}/{region} at {grid_m}m"
        )

    transform = base_transform
    output_grid_m = grid_m
    if downsample_factor > 1:
        output_grid_m = grid_m * downsample_factor
        transform, out_h, out_w = compute_transform_and_shape(bbox, output_grid_m)
        downsampled = np.full((out_h, out_w), float("nan"), dtype=np.float32)
        reproject(
            source=values_f32,
            destination=downsampled,
            src_transform=base_transform,
            src_crs="EPSG:3857",
            dst_transform=transform,
            dst_crs="EPSG:3857",
            resampling=Resampling.nearest,
            src_nodata=float("nan"),
            dst_nodata=float("nan"),
        )
        values_f32 = downsampled
        data_h, data_w = values_f32.shape

    # Expand to (1, H, W) for rasterio
    data_3d = values_f32[np.newaxis, :, :]
    levels = _overview_levels(data_h, data_w)

    with tempfile.TemporaryDirectory(dir=output_path.parent) as tmp_dir:
        tmp_gtiff = Path(tmp_dir) / "base.tif"

        # Step 1: Write base GTiff (tiled, no overviews)
        _write_base_gtiff(
            data=data_3d,
            path=tmp_gtiff,
            transform=transform,
            count=1,
            dtype="float32",
            nodata=nodata,
        )

        # Step 2: Build overviews (nearest for value grids)
        if levels:
            level_strs = [str(l) for l in levels]
            _run_gdal([
                _gdal("gdaladdo"), "-r", "nearest",
                "--config", "GDAL_TIFF_OVR_BLOCKSIZE", str(COG_BLOCKSIZE),
                str(tmp_gtiff), *level_strs,
            ])

        # Step 3: Convert to COG
        _gtiff_to_cog(tmp_gtiff, output_path)

    logger.info(
        "Wrote value COG: %s (%dx%d, %d overviews, grid=%.1fm, downsample=%dx)",
        output_path, data_w, data_h, len(levels), output_grid_m, downsample_factor,
    )
    return output_path


# ---------------------------------------------------------------------------
# Warp: reproject source raster data to the target model/region grid
# ---------------------------------------------------------------------------


def warp_to_target_grid(
    data: np.ndarray,
    src_crs: Any,
    src_transform: rasterio.transform.Affine,
    *,
    model: str,
    region: str,
    resampling: str = "bilinear",
    src_nodata: float | None = None,
    dst_nodata: float = float("nan"),
) -> tuple[np.ndarray, rasterio.transform.Affine]:
    """Reproject a 2-D array to the target EPSG:3857 grid for a model/region.

    Equivalent to:
        gdalwarp -t_srs EPSG:3857 -te ... -tr ... -tap -r {resampling}

    Parameters
    ----------
    data : np.ndarray
        2-D float array in the source CRS.
    src_crs : rasterio CRS or string
        CRS of the input data.
    src_transform : Affine
        Affine transform of the input data.
    model, region : str
        Target model/region for grid parameters.
    resampling : str
        Resampling method name (e.g. "bilinear", "nearest").
    src_nodata, dst_nodata : float or None
        Nodata values for source and destination.

    Returns
    -------
    (warped_data, dst_transform) where warped_data has the target grid shape.
    """
    bbox, grid_m = get_grid_params(model, region)
    dst_transform, dst_h, dst_w = compute_transform_and_shape(bbox, grid_m)
    dst_crs = rasterio.crs.CRS.from_epsg(3857)

    resamp = Resampling[resampling]

    # Expand to 3-D for reproject
    src_3d = data[np.newaxis, :, :] if data.ndim == 2 else data
    band_count = src_3d.shape[0]
    dst_3d = np.full((band_count, dst_h, dst_w), dst_nodata, dtype=np.float64)

    reproject(
        source=src_3d.astype(np.float64),
        destination=dst_3d,
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=dst_transform,
        dst_crs=dst_crs,
        resampling=resamp,
        src_nodata=src_nodata,
        dst_nodata=dst_nodata,
    )

    # Squeeze back to 2-D if input was 2-D
    if data.ndim == 2:
        dst_3d = dst_3d[0]

    return dst_3d.astype(np.float32), dst_transform


# ---------------------------------------------------------------------------
# Internal helpers: GDAL subprocess calls
# ---------------------------------------------------------------------------


def _run_gdal(cmd: list[str]) -> None:
    """Run a GDAL CLI command, raising on failure."""
    logger.debug("GDAL: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        env=os.environ.copy(),
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"GDAL command failed (exit {result.returncode}):\n"
            f"  cmd: {' '.join(cmd)}\n"
            f"  stdout: {result.stdout.strip()}\n"
            f"  stderr: {result.stderr.strip()}"
        )


def _write_base_gtiff(
    data: np.ndarray,
    path: Path,
    *,
    transform: rasterio.transform.Affine,
    count: int,
    dtype: str,
    nodata: float | None,
) -> None:
    """Write a tiled GTiff with no overviews (base image only).

    This is step 1 of the COG pipeline. Overviews are added
    separately via gdaladdo for per-band resampling control.
    """
    _, height, width = data.shape
    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": count,
        "dtype": dtype,
        "crs": "EPSG:3857",
        "transform": transform,
        "tiled": True,
        "blockxsize": COG_BLOCKSIZE,
        "blockysize": COG_BLOCKSIZE,
        "compress": COG_COMPRESS,
    }
    if nodata is not None:
        profile["nodata"] = nodata

    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data)


def _build_continuous_rgba_cog(
    rgba: np.ndarray,
    tmp_dir: Path,
    output_path: Path,
    transform: rasterio.transform.Affine,
    levels: list[int],
) -> Path:
    """Build a continuous RGBA COG with RGB=average, alpha=nearest overviews."""
    level_strs = [str(l) for l in levels]
    ovr_blocksize = str(COG_BLOCKSIZE)
    rgb_path = tmp_dir / "rgb_base.tif"
    alpha_path = tmp_dir / "alpha_base.tif"
    vrt_path = tmp_dir / "rgba_stacked.vrt"
    height = int(rgba.shape[1])
    width = int(rgba.shape[2])
    gt = (
        f"{transform.c:.14f}, {transform.a:.14f}, {transform.b:.14f}, "
        f"{transform.f:.14f}, {transform.d:.14f}, {transform.e:.14f}"
    )

    _write_base_gtiff(
        data=rgba[:3, :, :],
        path=rgb_path,
        transform=transform,
        count=3,
        dtype="uint8",
        nodata=None,
    )
    _write_base_gtiff(
        data=rgba[3:4, :, :],
        path=alpha_path,
        transform=transform,
        count=1,
        dtype="uint8",
        nodata=None,
    )
    # Pass 1: RGB overviews use average resampling for smoother continuous fields.
    _run_gdal([
        _gdal("gdaladdo"), "-r", "average",
        "--config", "GDAL_TIFF_OVR_BLOCKSIZE", ovr_blocksize,
        str(rgb_path), *level_strs,
    ])
    # Pass 2: Alpha overview stays nearest to preserve sharp validity boundaries.
    _run_gdal([
        _gdal("gdaladdo"), "-r", "nearest",
        "--config", "GDAL_TIFF_OVR_BLOCKSIZE", ovr_blocksize,
        str(alpha_path), *level_strs,
    ])
    # Ensure CRS/geotransform are explicit in the VRT metadata.
    vrt_path.write_text(
        (
            f'<VRTDataset rasterXSize="{width}" rasterYSize="{height}">\n'
            f"  <SRS>EPSG:3857</SRS>\n"
            f"  <GeoTransform>{gt}</GeoTransform>\n"
            f"  <VRTRasterBand dataType=\"Byte\" band=\"1\">\n"
            f"    <SimpleSource>\n"
            f"      <SourceFilename relativeToVRT=\"1\">{rgb_path.name}</SourceFilename>\n"
            f"      <SourceBand>1</SourceBand>\n"
            f"    </SimpleSource>\n"
            f"  </VRTRasterBand>\n"
            f"  <VRTRasterBand dataType=\"Byte\" band=\"2\">\n"
            f"    <SimpleSource>\n"
            f"      <SourceFilename relativeToVRT=\"1\">{rgb_path.name}</SourceFilename>\n"
            f"      <SourceBand>2</SourceBand>\n"
            f"    </SimpleSource>\n"
            f"  </VRTRasterBand>\n"
            f"  <VRTRasterBand dataType=\"Byte\" band=\"3\">\n"
            f"    <SimpleSource>\n"
            f"      <SourceFilename relativeToVRT=\"1\">{rgb_path.name}</SourceFilename>\n"
            f"      <SourceBand>3</SourceBand>\n"
            f"    </SimpleSource>\n"
            f"  </VRTRasterBand>\n"
            f"  <VRTRasterBand dataType=\"Byte\" band=\"4\">\n"
            f"    <SimpleSource>\n"
            f"      <SourceFilename relativeToVRT=\"1\">{alpha_path.name}</SourceFilename>\n"
            f"      <SourceBand>1</SourceBand>\n"
            f"    </SimpleSource>\n"
            f"  </VRTRasterBand>\n"
            f"</VRTDataset>\n"
        ),
        encoding="utf-8",
    )
    _gtiff_to_cog(vrt_path, output_path)
    logger.debug(
        "Built continuous RGBA COG with overviews (RGB=average, A=nearest), levels=%s",
        levels,
    )
    return output_path


def _gtiff_to_cog(src_path: Path, dst_path: Path) -> None:
    """Convert a GTiff or VRT (with overviews already built) to a COG.

    Uses `gdal_translate -of COG` which reorders IFDs for
    cloud-optimized layout (overview IFDs before main image).

    Overviews are expected to already exist in the source (or
    in source files referenced by a VRT). The COG driver copies
    them via COPY_SRC_OVERVIEWS.
    """
    _run_gdal([
        _gdal("gdal_translate"),
        "-of", "COG",
        "-co", f"BLOCKSIZE={COG_BLOCKSIZE}",
        "-co", f"COMPRESS={COG_COMPRESS.upper()}",
        "-co", "COPY_SRC_OVERVIEWS=YES",
        str(src_path),
        str(dst_path),
    ])

"""Build pipeline: orchestrates fetch → warp → colorize → write → validate.

This is the single entry-point for producing V3 artifacts.  For a given
model/region/var/fh it produces three files in the staging directory:

    fh{NNN}.rgba.cog.tif   — 4-band uint8 RGBA Cloud Optimized GeoTIFF
    fh{NNN}.val.cog.tif    — 1-band float32 value COG
    fh{NNN}.json           — sidecar metadata (per artifact contract)

All outputs pass two validation gates before being accepted:
    Gate 1 — gdalinfo structural validation (band count, CRS, tiling, overviews)
    Gate 2 — pixel statistics sanity check (alpha coverage, value range, nodata)

Phase 1 scope: "simple" derivation path only (tmp2m, refc — single GRIB fetch).
Phase 2 adds wspd (vector magnitude) and radar_ptype (categorical combo).

CLI usage:
    python -m backend.app.services.builder.pipeline \\
        --model hrrr --region pnw --var tmp2m --fh 0 \\
        --data-root ./data/v3
"""

from __future__ import annotations

import json
import logging
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import rasterio

from app.services.builder.cog_writer import (
    _gdal,
    compute_transform_and_shape,
    get_grid_params,
    write_rgba_cog,
    write_value_cog,
    warp_to_target_grid,
)
from app.services.builder.colorize import float_to_rgba
from app.services.builder.derive import derive_variable
from app.services.builder.fetch import (
    HerbieTransientUnavailableError,
    convert_units,
    fetch_variable,
)
from app.services.colormaps import get_color_map_spec

logger = logging.getLogger(__name__)

CONTRACT_VERSION = "3.0"
VALUE_HOVER_DOWNSAMPLE_FACTOR = 4
CANONICAL_COVERAGE = "conus"


def _gaussian_kernel_1d(sigma: float) -> np.ndarray:
    radius = max(1, int(np.ceil(3.0 * sigma)))
    x = np.arange(-radius, radius + 1, dtype=np.float32)
    kernel = np.exp(-(x * x) / (2.0 * sigma * sigma), dtype=np.float32)
    kernel_sum = float(kernel.sum())
    if kernel_sum <= 0:
        return np.array([1.0], dtype=np.float32)
    return (kernel / kernel_sum).astype(np.float32)


def _convolve_axis_edge(arr: np.ndarray, kernel: np.ndarray, axis: int) -> np.ndarray:
    if kernel.size == 1:
        return arr.astype(np.float32, copy=True)
    pad = kernel.size // 2
    pad_width = [(0, 0)] * arr.ndim
    pad_width[axis] = (pad, pad)
    padded = np.pad(arr, pad_width, mode="edge")
    return np.apply_along_axis(
        lambda values: np.convolve(values, kernel, mode="valid"),
        axis,
        padded,
    ).astype(np.float32, copy=False)


def _smooth_display_data(data: np.ndarray, sigma: float) -> np.ndarray:
    if sigma <= 0.0:
        return data

    finite_mask = np.isfinite(data)
    if not finite_mask.any():
        return data

    kernel = _gaussian_kernel_1d(sigma)
    data_filled = np.where(finite_mask, data, 0.0).astype(np.float32, copy=False)
    weight = np.where(finite_mask, 1.0, 0.0).astype(np.float32, copy=False)

    num = _convolve_axis_edge(data_filled, kernel, axis=1)
    num = _convolve_axis_edge(num, kernel, axis=0)
    den = _convolve_axis_edge(weight, kernel, axis=1)
    den = _convolve_axis_edge(den, kernel, axis=0)

    smoothed = np.where(den > 1e-6, num / den, np.nan).astype(np.float32, copy=False)
    smoothed[~finite_mask] = np.nan
    return smoothed


def _warp_resampling_for_kind(kind: str | None) -> str:
    """Return warp resampling method from variable kind.

    Categorical/indexed/discrete fields must use nearest to avoid
    interpolation across class boundaries. Continuous fields can use
    a smoother kernel.
    """
    normalized = str(kind or "").strip().lower()
    if normalized in {"discrete", "indexed", "categorical"}:
        return "nearest"
    return "bilinear"


def _prepare_display_data_for_colorize(
    warped_data: np.ndarray,
    var_spec: dict[str, Any],
) -> np.ndarray:
    kind = str(var_spec.get("type") or "").strip().lower()
    if kind in {"discrete", "indexed", "categorical"}:
        return warped_data

    sigma_raw = var_spec.get("display_smoothing_sigma")
    if sigma_raw is None:
        return warped_data
    try:
        sigma = float(sigma_raw)
    except (TypeError, ValueError):
        return warped_data
    if sigma <= 0.0:
        return warped_data
    return _smooth_display_data(warped_data, sigma)


# ---------------------------------------------------------------------------
# Gate 1: gdalinfo structural validation
# ---------------------------------------------------------------------------


def validate_cog(
    path: Path,
    *,
    expected_bands: int,
    expected_dtype: str,
    region: str,
    grid_meters: float,
) -> bool:
    """Validate a COG's structure via gdalinfo -json.

    Checks band count, band type, CRS, internal tiling, overview presence,
    and pixel size.  Returns True if all checks pass, False otherwise.
    Logs specific failures.
    """
    try:
        gdalinfo_bin = _find_gdalinfo()
        result = subprocess.run(
            [gdalinfo_bin, "-json", str(path)],
            capture_output=True, text=True, check=True,
            timeout=30,
        )
        info = json.loads(result.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError, FileNotFoundError) as exc:
        logger.error("gdalinfo failed for %s: %s", path, exc)
        return False

    ok = True

    # Band count
    bands = info.get("bands", [])
    if len(bands) != expected_bands:
        logger.error("Band count: expected %d, got %d (%s)", expected_bands, len(bands), path)
        ok = False

    # Band dtype
    if bands:
        actual_dtype = bands[0].get("type", "")
        if actual_dtype != expected_dtype:
            logger.error("Band type: expected %s, got %s (%s)", expected_dtype, actual_dtype, path)
            ok = False

    # CRS — must be EPSG:3857
    crs_info = info.get("coordinateSystem", {}).get("wkt", "")
    if "3857" not in crs_info:
        logger.error("CRS does not contain EPSG:3857 (%s)", path)
        ok = False

    # Internal tiling (512×512)
    if bands:
        block = bands[0].get("block", [])
        if block != [512, 512]:
            logger.error("Block size: expected [512, 512], got %s (%s)", block, path)
            ok = False

    # Overviews present
    if bands:
        overviews = bands[0].get("overviews", [])
        if not overviews:
            logger.error("No overviews found (%s)", path)
            ok = False

    # Pixel size matches grid_meters (±0.1m tolerance)
    geo_transform = info.get("geoTransform", [])
    if len(geo_transform) >= 6:
        pixel_x = abs(geo_transform[1])
        pixel_y = abs(geo_transform[5])
        if abs(pixel_x - grid_meters) > 0.1 or abs(pixel_y - grid_meters) > 0.1:
            logger.error(
                "Pixel size: expected %.1fm, got (%.1f, %.1f) (%s)",
                grid_meters, pixel_x, pixel_y, path,
            )
            ok = False

    # COG layout metadata
    layout = info.get("metadata", {}).get("IMAGE_STRUCTURE", {}).get("LAYOUT", "")
    if layout != "COG":
        logger.error("Layout: expected 'COG', got %r (%s)", layout, path)
        ok = False

    if ok:
        logger.info("Gate 1 PASS: %s", path.name)
    return ok


def _find_gdalinfo() -> str:
    """Locate gdalinfo via the same lazy resolver used for other GDAL tools."""
    return _gdal("gdalinfo")


# ---------------------------------------------------------------------------
# Gate 2: pixel statistics sanity check
# ---------------------------------------------------------------------------


def check_pixel_sanity(
    rgba_path: Path,
    val_path: Path,
    var_spec: dict[str, Any],
    var_spec_model: Any | None = None,
) -> bool:
    """Sanity-check pixel statistics of the produced artifacts.

    Catches catastrophic failures: all-transparent, solid-color,
    flat value fields, grid misalignment.  Thresholds are intentionally
    loose per the roadmap — the goal is to catch obviously broken artifacts.

    Returns True if all checks pass.
    """
    ok = True
    spec_type = str(var_spec.get("type", "")).lower()
    model_kind = str(getattr(var_spec_model, "kind", "") or "").lower()
    model_units = getattr(var_spec_model, "units", None) if var_spec_model is not None else None
    is_non_physical_kind = spec_type in {"indexed", "categorical", "discrete"} or model_kind in {
        "indexed",
        "categorical",
        "discrete",
    }
    is_non_physical_units = model_units is None or var_spec.get("units") is None
    is_non_physical_flag = var_spec.get("physical") is False
    allow_dry_frame = bool(var_spec.get("allow_dry_frame", False))
    skip_physical_range_checks = is_non_physical_kind or is_non_physical_units or is_non_physical_flag
    is_categorical_ptype = spec_type in {"discrete", "indexed"} and bool(var_spec.get("ptype_breaks"))

    # Default catastrophic-failure thresholds.
    min_alpha_coverage = 0.05
    max_nodata_ratio = 0.95

    # Categorical ptype products can legitimately be very sparse (near-dry scenes).
    # Keep guardrails, but relax thresholds enough to avoid rejecting valid frames.
    if is_categorical_ptype:
        min_alpha_coverage = 0.002  # 0.2%
        max_nodata_ratio = 0.998    # 99.8%
    elif allow_dry_frame:
        min_alpha_coverage = 0.0

    min_discrete_level = None
    levels = var_spec.get("levels")
    if isinstance(levels, list) and levels:
        try:
            min_discrete_level = float(levels[0])
        except (TypeError, ValueError):
            min_discrete_level = None

    # --- RGBA checks ---
    with rasterio.open(rgba_path) as src:
        alpha = src.read(4)
        total_pixels = alpha.size

        # Alpha coverage sanity threshold
        valid_count = int(np.count_nonzero(alpha == 255))
        coverage = valid_count / total_pixels
        if coverage < min_alpha_coverage:
            if is_categorical_ptype and valid_count == 0:
                logger.warning(
                    "Dry categorical ptype frame allowed: alpha coverage %.1f%% (%s)",
                    coverage * 100,
                    rgba_path,
                )
            elif allow_dry_frame and valid_count == 0:
                logger.warning(
                    "Dry frame allowed: alpha coverage %.1f%% (%s)",
                    coverage * 100,
                    rgba_path,
                )
            else:
                logger.error(
                    "Alpha coverage too low: %.1f%% (<%.1f%%) — likely all-transparent (%s)",
                    coverage * 100,
                    min_alpha_coverage * 100,
                    rgba_path,
                )
                ok = False

        # RGB not constant (at least 2 distinct values per band)
        for band_idx in range(1, 4):
            band_data = src.read(band_idx)
            # Only check where alpha is valid
            valid_pixels = band_data[alpha == 255]
            if valid_pixels.size > 0:
                unique_count = len(np.unique(valid_pixels))
                if unique_count < 2:
                    if allow_dry_frame:
                        logger.warning(
                            "Dry frame allowed: band %d is constant (value=%d) (%s)",
                            band_idx,
                            valid_pixels[0],
                            rgba_path,
                        )
                    else:
                        logger.error(
                            "Band %d is constant (value=%d) — likely colormap bug (%s)",
                            band_idx, valid_pixels[0], rgba_path,
                        )
                        ok = False

    # --- Value COG checks ---
    with rasterio.open(val_path) as src:
        values = src.read(1)
        finite_mask = np.isfinite(values)
        finite_count = int(np.count_nonzero(finite_mask))
        total_pixels = values.size

        # Nodata ratio sanity threshold
        nodata_ratio = 1.0 - (finite_count / total_pixels)
        if nodata_ratio > max_nodata_ratio:
            if is_categorical_ptype and finite_count == 0:
                logger.warning(
                    "Dry categorical ptype frame allowed: nodata ratio %.1f%% (%s)",
                    nodata_ratio * 100,
                    val_path,
                )
            else:
                logger.error(
                    "Value COG nodata ratio too high: %.1f%% (>%.1f%%) — "
                    "likely grid misalignment or empty fetch (%s)",
                    nodata_ratio * 100,
                    max_nodata_ratio * 100,
                    val_path,
                )
                ok = False

        # Value range: min ≠ max
        if finite_count > 0:
            vmin = float(np.nanmin(values[finite_mask]))
            vmax = float(np.nanmax(values[finite_mask]))
            if vmin == vmax:
                if allow_dry_frame and (min_discrete_level is None or vmin <= min_discrete_level):
                    logger.warning(
                        "Dry frame allowed: flat value field at %.2f (%s)",
                        vmin,
                        val_path,
                    )
                else:
                    logger.error(
                        "Value COG is flat (min==max==%.2f) — "
                        "likely constant input or unit conversion error (%s)",
                        vmin, val_path,
                    )
                    ok = False

            # Value range within VarSpec.range ± 20% (for physical continuous vars)
            spec_range = var_spec.get("range")
            if not skip_physical_range_checks and spec_range and len(spec_range) == 2:
                spec_min, spec_max = float(spec_range[0]), float(spec_range[1])
                span = spec_max - spec_min
                margin = span * 0.2
                if vmin < spec_min - margin or vmax > spec_max + margin:
                    logger.warning(
                        "Value range [%.1f, %.1f] outside spec range "
                        "[%.1f, %.1f] ± 20%% — may indicate unit error (%s)",
                        vmin, vmax, spec_min, spec_max, val_path,
                    )
                    # Warning only, not a hard fail

    if ok:
        logger.info("Gate 2 PASS: %s", rgba_path.name)
    return ok


# ---------------------------------------------------------------------------
# Sidecar JSON metadata
# ---------------------------------------------------------------------------


def build_sidecar_json(
    *,
    model: str,
    region: str | None = None,
    run_id: str,
    var_id: str,
    fh: int,
    run_date: datetime,
    colorize_meta: dict[str, Any],
    var_spec: dict[str, Any],
    var_spec_model: Any | None = None,
    contours: dict[str, Any] | None = None,
    value_downsample_factor: int = 1,
) -> dict[str, Any]:
    """Build the sidecar metadata dict per the artifact contract.

    The sidecar JSON is written alongside each frame's COGs and provides
    the frontend with all information needed to render legends and tooltips.
    """
    # Compute valid time = run_date + fh hours
    valid_time = run_date + timedelta(hours=fh)

    model_kind = getattr(var_spec_model, "kind", None) if var_spec_model is not None else None
    model_units = getattr(var_spec_model, "units", None) if var_spec_model is not None else None

    kind = colorize_meta.get("kind") or model_kind or var_spec.get("type", "continuous")
    units = model_units or colorize_meta.get("units") or var_spec.get("units", "")

    # Build legend
    legend = _build_legend(kind, var_spec, colorize_meta)

    sidecar: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "model": model,
        "run": run_id,
        "var": var_id,
        "fh": fh,
        "valid_time": valid_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "units": _format_units(units),
        "kind": kind,
        "min": colorize_meta.get("min"),
        "max": colorize_meta.get("max"),
        "legend": legend,
    }

    if region:
        sidecar["region"] = region

    if value_downsample_factor > 1:
        sidecar["hover_value_downsample_factor"] = int(value_downsample_factor)

    # Preserve optional legend-grouping metadata for categorical ptype variables.
    for key in ("ptype_order", "ptype_breaks", "ptype_levels", "bins_per_ptype"):
        value = colorize_meta.get(key)
        if value is None:
            value = var_spec.get(key)
        if value is not None:
            sidecar[key] = value

    if contours:
        sidecar["contours"] = contours

    return sidecar


def build_iso_contour_geojson(
    *,
    value_data: np.ndarray,
    value_transform: Any,
    out_geojson_path: Path,
    level: float,
    srs: str = "EPSG:4326",
) -> None:
    """Generate iso-contour GeoJSON from a full-resolution value grid.

    Writes a temporary in-memory-source GTiff (EPSG:3857) from the provided
    array/transform, then warps/contours via GDAL CLI. This avoids depending
    on the on-disk hover value COG resolution.
    """
    out_geojson_path.parent.mkdir(parents=True, exist_ok=True)

    gdalwarp_bin = _gdal("gdalwarp")
    gdal_contour_bin = _gdal("gdal_contour")

    tmp_path: Path | None = None
    src_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as src_file:
            src_path = Path(src_file.name)
        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)

        value_f32 = value_data.astype(np.float32, copy=False)
        with rasterio.open(
            src_path,
            "w",
            driver="GTiff",
            height=value_f32.shape[0],
            width=value_f32.shape[1],
            count=1,
            dtype="float32",
            crs="EPSG:3857",
            transform=value_transform,
            nodata=float("nan"),
        ) as src_ds:
            src_ds.write(value_f32, 1)

        subprocess.run(
            [
                gdalwarp_bin,
                "-t_srs",
                srs,
                "-r",
                "bilinear",
                "-of",
                "GTiff",
                str(src_path),
                str(tmp_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        subprocess.run(
            [
                gdal_contour_bin,
                "-fl",
                str(float(level)),
                "-a",
                "value",
                "-f",
                "GeoJSON",
                str(tmp_path),
                str(out_geojson_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    finally:
        if src_path is not None:
            try:
                if src_path.exists():
                    src_path.unlink()
            except Exception:
                pass
        if tmp_path is not None:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass


def _build_legend(
    kind: str,
    var_spec: dict[str, Any],
    colorize_meta: dict[str, Any],
) -> dict[str, Any]:
    """Build the legend block for the sidecar JSON.

    For continuous vars: gradient with evenly-spaced or explicit stops.
    For discrete/indexed: discrete with level/color stops.
    """
    if kind == "continuous":
        # Check for explicit legend_stops first
        legend_stops = var_spec.get("legend_stops") or colorize_meta.get("legend_stops")
        if legend_stops:
            # legend_stops is a list of (value, hex_color) tuples
            stops = [[float(v), c] for v, c in legend_stops]
        else:
            anchors = (
                var_spec.get("color_anchors")
                or var_spec.get("anchors")
                or colorize_meta.get("color_anchors")
                or colorize_meta.get("anchors")
            )
            if anchors:
                # Anchors are already value→color stops.
                stops = [[float(v), c] for v, c in anchors]
            else:
                # Generate stops from range + colors
                spec_range = var_spec.get("range", colorize_meta.get("range", [0, 1]))
                colors = var_spec.get("colors", colorize_meta.get("colors", []))
                if not colors:
                    raise ValueError(
                        f"Continuous var spec requires 'colors' but got none "
                        f"(var_spec keys: {sorted(var_spec.keys())})"
                    )
                rmin, rmax = float(spec_range[0]), float(spec_range[1])
                n = len(colors)
                stops = []
                for i, color in enumerate(colors):
                    val = rmin + (rmax - rmin) * i / max(n - 1, 1)
                    stops.append([round(val, 1), color])

        return {"type": "gradient", "stops": stops}

    else:  # discrete / indexed
        levels = var_spec.get("levels", colorize_meta.get("levels", []))
        colors = var_spec.get("colors", colorize_meta.get("colors", []))

        ptype_order = colorize_meta.get("ptype_order") or var_spec.get("ptype_order")
        ptype_breaks = colorize_meta.get("ptype_breaks") or var_spec.get("ptype_breaks")
        ptype_levels = colorize_meta.get("ptype_levels") or var_spec.get("ptype_levels")

        if (
            isinstance(ptype_order, list)
            and isinstance(ptype_breaks, dict)
            and isinstance(ptype_levels, dict)
        ):
            stops: list[list[Any]] = []
            for ptype in ptype_order:
                boundary = ptype_breaks.get(ptype)
                type_levels = ptype_levels.get(ptype)
                if not isinstance(boundary, dict) or not isinstance(type_levels, list):
                    continue
                offset = int(boundary.get("offset", -1))
                count = int(boundary.get("count", 0))
                if offset < 0 or count <= 0:
                    continue
                max_items = min(count, len(type_levels), len(colors) - offset)
                if max_items <= 0:
                    continue
                for idx in range(max_items):
                    stops.append([float(type_levels[idx]), colors[offset + idx]])
            if stops:
                return {"type": "discrete", "stops": stops}

        # Pair levels with colors (take min length)
        n = min(len(levels), len(colors))
        stops = [[float(levels[i]), colors[i]] for i in range(n)]

        return {"type": "discrete", "stops": stops}


def _format_units(units: str) -> str:
    """Normalize unit strings for display (e.g. 'F' → '°F')."""
    mapping = {
        "F": "°F",
        "C": "°C",
        "K": "K",
        "mph": "mph",
        "m/s": "m/s",
        "dBZ": "dBZ",
        "mm/hr": "mm/hr",
        "in": "in",
    }
    return mapping.get(units, units)


# ---------------------------------------------------------------------------
# GRIB search pattern lookup
# ---------------------------------------------------------------------------


def _get_search_pattern(var_spec_model: Any) -> str:
    """Extract the Herbie search pattern from a model VarSpec.

    The VarSpec.selectors.search list contains GRIB index patterns.
    We use the first one.
    """
    selectors = getattr(var_spec_model, "selectors", None)
    if selectors is None:
        raise ValueError("VarSpec has no selectors")
    search_list = getattr(selectors, "search", [])
    if not search_list:
        raise ValueError(
            f"VarSpec for {getattr(var_spec_model, 'id', '?')!r} has no "
            f"search patterns — cannot determine GRIB message to fetch"
        )
    return search_list[0]


# ---------------------------------------------------------------------------
# Frame builder — the main orchestration function
# ---------------------------------------------------------------------------


def build_frame(
    *,
    model: str,
    region: str,
    var_id: str,
    fh: int,
    run_date: datetime,
    data_root: Path,
    product: str = "sfc",
    model_plugin: Any = None,
) -> Path | None:
    """Build one frame's artifacts: RGBA COG + value COG + sidecar JSON.

    This is the core orchestration function implementing the pipeline:
        fetch → unit convert → warp → colorize → write COGs → validate → sidecar

    Parameters
    ----------
    model : str
        Model identifier (e.g. "hrrr").
    region : str
        Region identifier (e.g. "pnw", "conus").
    var_id : str
        Variable identifier (e.g. "tmp2m").
    fh : int
        Forecast hour.
    run_date : datetime
        Model run initialization time (UTC).
    data_root : Path
        Root of the data directory (e.g. ./data/v3).
    product : str
        Herbie product string (default "sfc").
    model_plugin : ModelPlugin, optional
        Model plugin instance for VarSpec lookup.
        If None, uses the model registry.

    Returns
    -------
    Path to the staging directory with the three artifacts,
    or None if validation failed and the frame was rejected.
    """
    run_id = _run_id_from_date(run_date)
    fh_str = f"fh{fh:03d}"

    if region != CANONICAL_COVERAGE:
        logger.error("Rejected non-canonical coverage for build_frame: %s (expected %s)", region, CANONICAL_COVERAGE)
        return None

    logger.info("Building frame: %s/%s/%s/%s (coverage=%s)", model, run_id, var_id, fh_str, region)

    # --- Resolve specs ---
    resolved_plugin = model_plugin or _resolve_model_plugin(model)
    var_key = resolved_plugin.normalize_var_id(var_id)
    var_spec_model = _resolve_model_var_spec(model, var_key, resolved_plugin)
    var_capability = _resolve_model_var_capability(model, var_key, resolved_plugin)
    color_map_id = getattr(var_capability, "color_map_id", None)
    if not isinstance(color_map_id, str) or not color_map_id.strip():
        logger.error(
            "Missing color_map_id in model capability for model=%s var_key=%s; build aborted",
            model,
            var_key,
        )
        return None
    color_map_id = color_map_id.strip()
    try:
        var_spec_colormap = get_color_map_spec(color_map_id)
    except KeyError:
        logger.error("No colormap spec for model=%s var_key=%s color_map_id=%s", model, var_key, color_map_id)
        return None

    kind = (
        getattr(var_capability, "kind", None)
        or getattr(var_spec_model, "kind", None)
        or var_spec_colormap.get("type", "continuous")
    )
    kind_normalized = str(kind).strip().lower() or "continuous"
    warp_resampling = _warp_resampling_for_kind(kind_normalized)
    search_pattern = None if getattr(var_spec_model, "derived", False) else _get_search_pattern(var_spec_model)

    # --- Staging directory ---
    staging_dir = data_root / "staging" / model / run_id / var_key
    staging_dir.mkdir(parents=True, exist_ok=True)

    rgba_path = staging_dir / f"{fh_str}.rgba.cog.tif"
    val_path = staging_dir / f"{fh_str}.val.cog.tif"
    sidecar_path = staging_dir / f"{fh_str}.json"
    contour_geojson_path: Path | None = None
    contour_sidecar: dict[str, Any] | None = None

    try:
        if getattr(var_spec_model, "derived", False):
            # --- Step 1/2: Derive from component GRIB fields ---
            logger.info("Step 1/6: Deriving variable components")
            converted_data, src_crs, src_transform = derive_variable(
                model_id=model,
                var_key=var_key,
                product=product,
                run_date=run_date,
                fh=fh,
                var_spec_model=var_spec_model,
                var_capability=var_capability,
                model_plugin=resolved_plugin,
            )
        else:
            # --- Step 1: Fetch GRIB data ---
            logger.info("Step 1/6: Fetching GRIB data")
            if search_pattern is None:
                raise ValueError(
                    f"No search pattern resolved for non-derived var {var_id!r}"
                )
            raw_data, src_crs, src_transform = fetch_variable(
                    model_id=model,
                    product=product,
                    search_pattern=search_pattern,
                    run_date=run_date,
                    fh=fh,
                )

            # --- Step 2: Unit conversion ---
            logger.info("Step 2/6: Unit conversion")
            converted_data = convert_units(
                raw_data,
                var_key=var_key,
                model_id=model,
                var_capability=var_capability,
            )

        # --- Step 3: Warp to target grid ---
        logger.info("Step 3/6: Warping to target grid (resampling=%s)", warp_resampling)
        warped_data, dst_transform = warp_to_target_grid(
            converted_data,
            src_crs,
            src_transform,
            model=model,
            region=region,
            resampling=warp_resampling,
            src_nodata=None,
            dst_nodata=float("nan"),
        )

        # --- Step 4: Colorize ---
        logger.info("Step 4/6: Colorizing")
        display_data = _prepare_display_data_for_colorize(warped_data, var_spec_colormap)
        rgba, colorize_meta = float_to_rgba(
            display_data,
            color_map_id,
            meta_var_key=var_key,
        )

        # --- Step 5: Write COGs ---
        logger.info("Step 5/6: Writing COGs")
        write_rgba_cog(
            rgba, rgba_path,
            model=model, region=region, kind=kind_normalized,
        )
        write_value_cog(
            warped_data, val_path,
            model=model, region=region,
            downsample_factor=VALUE_HOVER_DOWNSAMPLE_FACTOR,
        )

        # --- Step 5b: Optional contour extraction (tmp2m only) ---
        if var_key == "tmp2m":
            contour_rel_path = f"contours/{fh_str}.iso32.geojson"
            contour_geojson_path = staging_dir / contour_rel_path
            try:
                build_iso_contour_geojson(
                    value_data=warped_data,
                    value_transform=dst_transform,
                    out_geojson_path=contour_geojson_path,
                    level=32.0,
                    srs="EPSG:4326",
                )
                contour_sidecar = {
                    "iso32f": {
                        "format": "geojson",
                        "path": contour_rel_path,
                        "srs": "EPSG:4326",
                        "level": 32.0,
                    }
                }
                logger.info(
                    "Contour generated: %s/%s/%s/%s/%s -> %s",
                    model,
                    region,
                    run_id,
                    var_key,
                    fh_str,
                    contour_geojson_path,
                )
            except Exception as exc:
                stdout = getattr(exc, "stdout", None)
                stderr = getattr(exc, "stderr", None)
                logger.warning(
                    "Contour generation failed (continuing): %s/%s/%s/%s/%s -> %s | %s | stdout=%r stderr=%r",
                    model,
                    region,
                    run_id,
                    var_key,
                    fh_str,
                    contour_geojson_path,
                    exc,
                    stdout,
                    stderr,
                )
                contour_geojson_path = None
                contour_sidecar = None

        # --- Step 6: Validate (Gates 1 & 2) ---
        logger.info("Step 6/6: Validating artifacts")
        _, grid_m = get_grid_params(model, region)

        # Gate 1: structural validation
        if not validate_cog(
            rgba_path,
            expected_bands=4,
            expected_dtype="Byte",
            region=region,
            grid_meters=grid_m,
        ):
            logger.error("Gate 1 FAILED for RGBA COG — rejecting frame")
            _cleanup_artifacts(rgba_path, val_path, sidecar_path, contour_geojson_path)
            return None

        if not validate_cog(
            val_path,
            expected_bands=1,
            expected_dtype="Float32",
            region=region,
            grid_meters=grid_m * VALUE_HOVER_DOWNSAMPLE_FACTOR,
        ):
            logger.error("Gate 1 FAILED for value COG — rejecting frame")
            _cleanup_artifacts(rgba_path, val_path, sidecar_path, contour_geojson_path)
            return None

        # Gate 2: pixel sanity
        if not check_pixel_sanity(
            rgba_path,
            val_path,
            var_spec_colormap,
            var_spec_model=var_spec_model,
        ):
            logger.error("Gate 2 FAILED — rejecting frame")
            _cleanup_artifacts(rgba_path, val_path, sidecar_path, contour_geojson_path)
            return None

        # --- Write sidecar JSON ---
        sidecar = build_sidecar_json(
            model=model,
            run_id=run_id,
            var_id=var_key,
            fh=fh,
            run_date=run_date,
            colorize_meta=colorize_meta,
            var_spec=var_spec_colormap,
            var_spec_model=var_spec_model,
            contours=contour_sidecar,
            value_downsample_factor=VALUE_HOVER_DOWNSAMPLE_FACTOR,
        )
        _write_json_atomic(sidecar_path, sidecar)

        logger.info(
            "Frame complete: %s/%s/%s/%s/%s "
            "(RGBA: %s, Val: %s, JSON: %s)",
            model, region, run_id, var_key, fh_str,
            _file_size_str(rgba_path),
            _file_size_str(val_path),
            _file_size_str(sidecar_path),
        )
        return staging_dir

    except HerbieTransientUnavailableError as exc:
        logger.warning(
            "Build transiently unavailable for %s/%s/%s/%s/%s: %s",
            model,
            region,
            run_id,
            var_key,
            fh_str,
            exc,
        )
        _cleanup_artifacts(rgba_path, val_path, sidecar_path, contour_geojson_path)
        return None

    except Exception:
        logger.exception(
            "Build failed for %s/%s/%s/%s/%s",
            model, region, run_id, var_key, fh_str,
        )
        _cleanup_artifacts(rgba_path, val_path, sidecar_path, contour_geojson_path)
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_model_var_spec(
    model: str,
    var_key: str,
    model_plugin: Any = None,
) -> Any:
    """Resolve the VarSpec from model plugin or registry."""
    plugin = model_plugin or _resolve_model_plugin(model)
    normalized = plugin.normalize_var_id(var_key)
    spec = plugin.get_var(normalized)
    if spec is None:
        raise ValueError(
            f"Variable {normalized!r} not found in {model!r} model plugin"
        )
    return spec


def _resolve_model_var_capability(
    model: str,
    var_key: str,
    model_plugin: Any = None,
) -> Any:
    plugin = model_plugin or _resolve_model_plugin(model)
    normalized = plugin.normalize_var_id(var_key)
    capability = plugin.get_var_capability(normalized)
    if capability is not None:
        return capability
    raise ValueError(
        f"Variable capability missing for {model!r}/{normalized!r}; "
        "plugin capabilities are required for all buildable variables"
    )


def _resolve_model_plugin(model: str) -> Any:
    """Resolve a model plugin by id."""
    from app.models.registry import MODEL_REGISTRY

    plugin = MODEL_REGISTRY.get(model)
    if plugin is None:
        raise ValueError(f"Unknown model: {model!r}")
    return plugin


def _run_id_from_date(run_date: datetime) -> str:
    """Format a run date as the canonical run_id string.

    Example: datetime(2026, 2, 17, 6) → "20260217_06z"
    """
    return run_date.strftime("%Y%m%d_%Hz")


def _write_json_atomic(path: Path, data: dict) -> None:
    """Write JSON to a file atomically via tmp → rename."""
    tmp_path = path.with_suffix(".json.tmp")
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2, default=str)
        f.write("\n")
    tmp_path.rename(path)
    logger.debug("Wrote sidecar JSON: %s", path)


def _cleanup_artifacts(*paths: Path | None) -> None:
    """Remove artifact files that failed validation."""
    for p in paths:
        if p is not None and p.exists():
            p.unlink()
            logger.debug("Cleaned up: %s", p)


def _file_size_str(path: Path) -> str:
    """Human-readable file size."""
    if not path.exists():
        return "??"
    size = path.stat().st_size
    if size < 1024:
        return f"{size}B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f}KB"
    return f"{size / (1024 * 1024):.1f}MB"


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for building a single frame."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Build V3 artifacts for a single frame",
        prog="python -m backend.app.services.builder.pipeline",
    )
    parser.add_argument("--model", required=True, help="Model id (e.g. hrrr)")
    parser.add_argument("--region", required=True, help="Region id (e.g. pnw, conus)")
    parser.add_argument("--var", required=True, dest="var_id", help="Variable id (e.g. tmp2m)")
    parser.add_argument("--fh", required=True, type=int, help="Forecast hour")
    parser.add_argument("--data-root", required=True, type=Path, help="Data root directory")
    parser.add_argument(
        "--run",
        default=None,
        help="Run id (e.g. 20260217_06z). Defaults to latest available.",
    )
    parser.add_argument("--product", default="sfc", help="Herbie product (default: sfc)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    # Parse or determine run date
    if args.run:
        run_date = _parse_run_id(args.run)
    else:
        run_date = _latest_run_date(args.model)
        logger.info("Using latest run: %s", _run_id_from_date(run_date))

    result = build_frame(
        model=args.model,
        region=args.region,
        var_id=args.var_id,
        fh=args.fh,
        run_date=run_date,
        data_root=args.data_root,
        product=args.product,
    )

    if result is None:
        logger.error("Build FAILED — frame rejected")
        raise SystemExit(1)

    logger.info("Build SUCCESS — artifacts in %s", result)


def _parse_run_id(run_id: str) -> datetime:
    """Parse a run_id string like '20260217_06z' into a datetime."""
    # Strip trailing 'z' if present
    clean = run_id.rstrip("zZ")
    try:
        return datetime.strptime(clean, "%Y%m%d_%H").replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    try:
        return datetime.strptime(clean, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    except ValueError:
        raise ValueError(
            f"Cannot parse run_id {run_id!r}. "
            f"Expected format: YYYYMMDD_HHz (e.g. 20260217_06z)"
        )


def _latest_run_date(model: str) -> datetime:
    """Determine the latest available run date for a model.

    Uses a simple heuristic: round the current UTC time down to the
    nearest synoptic cycle, then step back one cycle to ensure data
    availability (GRIB data typically has ~2h latency).

    HRRR: hourly cycles (round back 2 hours)
    GFS:  6-hourly cycles (round back to last 00/06/12/18, minus 4 hours)
    """
    now = datetime.now(timezone.utc)
    plugin = _resolve_model_plugin(model)
    run_discovery = plugin.run_discovery_config() if hasattr(plugin, "run_discovery_config") else {}
    fallback_lag_hours = 3
    cadence_hours = 1
    try:
        fallback_lag_hours = max(0, int(run_discovery.get("fallback_lag_hours", fallback_lag_hours)))
    except (TypeError, ValueError):
        fallback_lag_hours = 3
    try:
        cadence_hours = max(1, int(run_discovery.get("cycle_cadence_hours", cadence_hours)))
    except (TypeError, ValueError):
        cadence_hours = 1

    target = now - timedelta(hours=fallback_lag_hours)
    aligned_hour = (target.hour // cadence_hours) * cadence_hours
    return target.replace(hour=aligned_hour, minute=0, second=0, microsecond=0)


if __name__ == "__main__":
    main()

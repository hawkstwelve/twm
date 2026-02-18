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
from app.services.builder.fetch import convert_units, fetch_variable
from app.services.colormaps import VAR_SPECS

logger = logging.getLogger(__name__)

CONTRACT_VERSION = "3.0"


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
) -> bool:
    """Sanity-check pixel statistics of the produced artifacts.

    Catches catastrophic failures: all-transparent, solid-color,
    flat value fields, grid misalignment.  Thresholds are intentionally
    loose per the roadmap — the goal is to catch obviously broken artifacts.

    Returns True if all checks pass.
    """
    ok = True

    # --- RGBA checks ---
    with rasterio.open(rgba_path) as src:
        alpha = src.read(4)
        total_pixels = alpha.size

        # Alpha coverage: >5% valid
        valid_count = int(np.count_nonzero(alpha == 255))
        coverage = valid_count / total_pixels
        if coverage < 0.05:
            logger.error(
                "Alpha coverage too low: %.1f%% (<5%%) — likely all-transparent (%s)",
                coverage * 100, rgba_path,
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

        # Nodata ratio: <95% nodata
        nodata_ratio = 1.0 - (finite_count / total_pixels)
        if nodata_ratio > 0.95:
            logger.error(
                "Value COG nodata ratio too high: %.1f%% (>95%%) — "
                "likely grid misalignment or empty fetch (%s)",
                nodata_ratio * 100, val_path,
            )
            ok = False

        # Value range: min ≠ max
        if finite_count > 0:
            vmin = float(np.nanmin(values[finite_mask]))
            vmax = float(np.nanmax(values[finite_mask]))
            if vmin == vmax:
                logger.error(
                    "Value COG is flat (min==max==%.2f) — "
                    "likely constant input or unit conversion error (%s)",
                    vmin, val_path,
                )
                ok = False

            # Value range within VarSpec.range ± 20% (for continuous vars)
            spec_range = var_spec.get("range")
            if spec_range and len(spec_range) == 2:
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
    region: str,
    run_id: str,
    var_id: str,
    fh: int,
    run_date: datetime,
    colorize_meta: dict[str, Any],
    var_spec: dict[str, Any],
    contours: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the sidecar metadata dict per the artifact contract.

    The sidecar JSON is written alongside each frame's COGs and provides
    the frontend with all information needed to render legends and tooltips.
    """
    # Compute valid time = run_date + fh hours
    valid_time = run_date + timedelta(hours=fh)

    kind = colorize_meta.get("kind", var_spec.get("type", "continuous"))
    units = colorize_meta.get("units") or var_spec.get("units", "")

    # Build legend
    legend = _build_legend(kind, var_spec, colorize_meta)

    sidecar: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "model": model,
        "region": region,
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

    if contours:
        sidecar["contours"] = contours

    return sidecar


def build_iso_contour_geojson(
    *,
    val_cog_path: Path,
    out_geojson_path: Path,
    level: float,
    srs: str = "EPSG:3857",
) -> None:
    """Generate iso-contour GeoJSON from a value COG using GDAL CLI tools."""
    out_geojson_path.parent.mkdir(parents=True, exist_ok=True)

    gdalwarp_bin = _gdal("gdalwarp")
    gdal_contour_bin = _gdal("gdal_contour")

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)

        subprocess.run(
            [
                gdalwarp_bin,
                "-t_srs",
                srs,
                "-r",
                "bilinear",
                "-of",
                "GTiff",
                str(val_cog_path),
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

    logger.info(
        "Building frame: %s/%s/%s/%s/%s",
        model, region, run_id, var_id, fh_str,
    )

    # --- Resolve specs ---
    var_spec_colormap = VAR_SPECS.get(var_id)
    if var_spec_colormap is None:
        logger.error("No colormap spec (VAR_SPECS) for var_id=%r", var_id)
        return None

    kind = var_spec_colormap.get("type", "continuous")

    # Get GRIB search pattern from model plugin
    var_spec_model = _resolve_model_var_spec(model, var_id, model_plugin)
    search_pattern = _get_search_pattern(var_spec_model)

    # --- Staging directory ---
    staging_dir = data_root / "staging" / model / region / run_id / var_id
    staging_dir.mkdir(parents=True, exist_ok=True)

    rgba_path = staging_dir / f"{fh_str}.rgba.cog.tif"
    val_path = staging_dir / f"{fh_str}.val.cog.tif"
    sidecar_path = staging_dir / f"{fh_str}.json"
    contour_geojson_path: Path | None = None
    contour_sidecar: dict[str, Any] | None = None

    try:
        # --- Step 1: Fetch GRIB data ---
        logger.info("Step 1/6: Fetching GRIB data")
        raw_data, src_crs, src_transform = fetch_variable(
            model_id=model,
            product=product,
            search_pattern=search_pattern,
            run_date=run_date,
            fh=fh,
        )

        # --- Step 2: Unit conversion ---
        logger.info("Step 2/6: Unit conversion")
        converted_data = convert_units(raw_data, var_id)

        # --- Step 3: Warp to target grid ---
        logger.info("Step 3/6: Warping to target grid")
        warped_data, dst_transform = warp_to_target_grid(
            converted_data,
            src_crs,
            src_transform,
            model=model,
            region=region,
            resampling="bilinear",
            src_nodata=None,
            dst_nodata=float("nan"),
        )

        # --- Step 4: Colorize ---
        logger.info("Step 4/6: Colorizing")
        rgba, colorize_meta = float_to_rgba(warped_data, var_id)

        # --- Step 5: Write COGs ---
        logger.info("Step 5/6: Writing COGs")
        write_rgba_cog(
            rgba, rgba_path,
            model=model, region=region, kind=kind,
        )
        write_value_cog(
            warped_data, val_path,
            model=model, region=region,
        )

        # --- Step 5b: Optional contour extraction (tmp2m only) ---
        if var_id == "tmp2m":
            contour_rel_path = f"contours/{fh_str}.iso32.geojson"
            contour_geojson_path = staging_dir / contour_rel_path
            try:
                build_iso_contour_geojson(
                    val_cog_path=val_path,
                    out_geojson_path=contour_geojson_path,
                    level=32.0,
                    srs="EPSG:3857",
                )
                contour_sidecar = {
                    "iso32f": {
                        "format": "geojson",
                        "path": contour_rel_path,
                        "srs": "EPSG:3857",
                        "level": 32.0,
                    }
                }
                logger.info(
                    "Contour generated: %s/%s/%s/%s/%s -> %s",
                    model,
                    region,
                    run_id,
                    var_id,
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
                    var_id,
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
            grid_meters=grid_m,
        ):
            logger.error("Gate 1 FAILED for value COG — rejecting frame")
            _cleanup_artifacts(rgba_path, val_path, sidecar_path, contour_geojson_path)
            return None

        # Gate 2: pixel sanity
        if not check_pixel_sanity(rgba_path, val_path, var_spec_colormap):
            logger.error("Gate 2 FAILED — rejecting frame")
            _cleanup_artifacts(rgba_path, val_path, sidecar_path, contour_geojson_path)
            return None

        # --- Write sidecar JSON ---
        sidecar = build_sidecar_json(
            model=model,
            region=region,
            run_id=run_id,
            var_id=var_id,
            fh=fh,
            run_date=run_date,
            colorize_meta=colorize_meta,
            var_spec=var_spec_colormap,
            contours=contour_sidecar,
        )
        _write_json_atomic(sidecar_path, sidecar)

        logger.info(
            "Frame complete: %s/%s/%s/%s/%s "
            "(RGBA: %s, Val: %s, JSON: %s)",
            model, region, run_id, var_id, fh_str,
            _file_size_str(rgba_path),
            _file_size_str(val_path),
            _file_size_str(sidecar_path),
        )
        return staging_dir

    except Exception:
        logger.exception(
            "Build failed for %s/%s/%s/%s/%s",
            model, region, run_id, var_id, fh_str,
        )
        _cleanup_artifacts(rgba_path, val_path, sidecar_path, contour_geojson_path)
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_model_var_spec(
    model: str,
    var_id: str,
    model_plugin: Any = None,
) -> Any:
    """Resolve the VarSpec from model plugin or registry."""
    if model_plugin is not None:
        spec = model_plugin.get_var(var_id)
        if spec is not None:
            return spec

    # Try direct plugin import first (avoids registry pulling in all models)
    if model == "hrrr":
        from app.models.hrrr import HRRR_MODEL
        plugin = HRRR_MODEL
    else:
        # Fallback: import from model registry
        from app.models.registry import MODEL_REGISTRY
        plugin = MODEL_REGISTRY.get(model)

    if plugin is None:
        raise ValueError(f"Unknown model: {model!r}")
    spec = plugin.get_var(var_id)
    if spec is None:
        raise ValueError(
            f"Variable {var_id!r} not found in {model!r} model plugin"
        )
    return spec


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

    if model == "hrrr":
        # HRRR runs hourly; use 2 hours ago for safety
        target = now - timedelta(hours=2)
        return target.replace(minute=0, second=0, microsecond=0)
    elif model == "gfs":
        # GFS runs at 00/06/12/18z; use 5 hours ago, round to 6h
        target = now - timedelta(hours=5)
        cycle_hour = (target.hour // 6) * 6
        return target.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)
    else:
        # Default: 3 hours ago, on the hour
        target = now - timedelta(hours=3)
        return target.replace(minute=0, second=0, microsecond=0)


if __name__ == "__main__":
    main()

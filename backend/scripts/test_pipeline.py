#!/usr/bin/env python3
"""Integration test for pipeline.py validation gates and sidecar JSON.

Exercises the full pipeline (minus GRIB fetch) by synthesizing data arrays,
running them through colorize → write COGs → validate_cog → check_pixel_sanity
→ build_sidecar_json.  This validates all the wiring without network access.

Run from repo root:
    PYTHONPATH=backend .venv/bin/python backend/scripts/test_pipeline.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

# Ensure backend/ is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.builder.cog_writer import (
    compute_transform_and_shape,
    get_grid_params,
    write_rgba_cog,
    write_value_cog,
)
from app.services.builder.colorize import float_to_rgba
from app.services.builder.fetch import convert_units
from app.services.builder.pipeline import (
    CONTRACT_VERSION,
    _format_units,
    _parse_run_id,
    _run_id_from_date,
    build_sidecar_json,
    check_pixel_sanity,
    validate_cog,
)
from app.services.colormaps import VAR_SPECS


MODEL = "hrrr"
REGION = "pnw"


def _make_synthetic_tmp2m() -> np.ndarray:
    """Create synthetic 2m temperature data in Celsius (GDAL GRIB output).

    GDAL's GRIB driver normalizes temps to °C (GRIB_NORMALIZE_UNITS=YES).
    Fills the HRRR/PNW grid with a gradient from -23.15°C to 36.85°C
    (roughly -10°F to 98°F), with a NaN strip at the top for nodata.
    """
    bbox, grid_m = get_grid_params(MODEL, REGION)
    _, height, width = compute_transform_and_shape(bbox, grid_m)

    # Gradient from -23.15°C to 36.85°C across the height
    data = np.linspace(-23.15, 36.85, height)[:, np.newaxis]
    data = np.broadcast_to(data, (height, width)).copy().astype(np.float32)

    # NaN strip at top (simulates area outside GRIB domain)
    data[:10, :] = np.nan
    return data


def test_unit_conversion():
    """Verify Celsius → Fahrenheit conversion (GDAL delivers °C for GRIB TMP)."""
    c_data = np.array([0.0, 100.0, -40.0], dtype=np.float32)
    f_data = convert_units(c_data, "tmp2m")
    expected = np.array([32.0, 212.0, -40.0], dtype=np.float32)
    np.testing.assert_allclose(f_data, expected, atol=0.01)
    print("  Unit conversion (°C→°F): PASS")


def test_colorize_roundtrip():
    """Verify float_to_rgba produces valid RGBA from tmp2m data."""
    data_k = _make_synthetic_tmp2m()
    data_f = convert_units(data_k, "tmp2m")

    rgba, meta = float_to_rgba(data_f, "tmp2m")

    assert rgba.shape[0] == 4, f"Expected 4 bands, got {rgba.shape[0]}"
    assert rgba.dtype == np.uint8, f"Expected uint8, got {rgba.dtype}"

    # Alpha should be 0 for NaN rows, 255 elsewhere
    alpha = rgba[3]
    assert np.all(alpha[:10, :] == 0), "NaN rows should have alpha=0"
    assert np.all(alpha[10:, :] == 255), "Valid rows should have alpha=255"

    # Meta should have expected fields
    assert meta["kind"] == "continuous"
    assert meta["units"] == "F"
    assert meta["min"] is not None
    assert meta["max"] is not None
    print(f"  Colorize roundtrip: PASS (range {meta['min']:.1f}–{meta['max']:.1f}°F)")


def test_validate_cog_gates():
    """Write COGs and run both validation gates."""
    bbox, grid_m = get_grid_params(MODEL, REGION)
    transform, height, width = compute_transform_and_shape(bbox, grid_m)

    # Synthesize data
    data_k = _make_synthetic_tmp2m()
    data_f = convert_units(data_k, "tmp2m")
    rgba, meta = float_to_rgba(data_f, "tmp2m")

    with tempfile.TemporaryDirectory() as tmpdir:
        rgba_path = Path(tmpdir) / "fh000.rgba.cog.tif"
        val_path = Path(tmpdir) / "fh000.val.cog.tif"

        # Write COGs
        write_rgba_cog(rgba, rgba_path, model=MODEL, region=REGION, kind="continuous")
        write_value_cog(data_f, val_path, model=MODEL, region=REGION)

        rgba_size = rgba_path.stat().st_size / 1024
        val_size = val_path.stat().st_size / 1024
        print(f"  COG sizes: RGBA={rgba_size:.0f}KB, Val={val_size:.0f}KB")

        # Gate 1: structural validation
        assert validate_cog(
            rgba_path, expected_bands=4, expected_dtype="Byte",
            region=REGION, grid_meters=grid_m,
        ), "Gate 1 failed for RGBA COG"
        print("  Gate 1 (RGBA structural): PASS")

        assert validate_cog(
            val_path, expected_bands=1, expected_dtype="Float32",
            region=REGION, grid_meters=grid_m,
        ), "Gate 1 failed for value COG"
        print("  Gate 1 (Value structural): PASS")

        # Gate 2: pixel sanity
        var_spec = VAR_SPECS["tmp2m"]
        assert check_pixel_sanity(
            rgba_path, val_path, var_spec,
        ), "Gate 2 failed"
        print("  Gate 2 (Pixel sanity): PASS")


def test_sidecar_json():
    """Verify sidecar JSON matches the artifact contract schema."""
    run_date = datetime(2026, 2, 17, 6, tzinfo=timezone.utc)
    data_k = _make_synthetic_tmp2m()
    data_f = convert_units(data_k, "tmp2m")
    _, meta = float_to_rgba(data_f, "tmp2m")

    var_spec = VAR_SPECS["tmp2m"]
    sidecar = build_sidecar_json(
        model=MODEL,
        region=REGION,
        run_id="20260217_06z",
        var_id="tmp2m",
        fh=3,
        run_date=run_date,
        colorize_meta=meta,
        var_spec=var_spec,
    )

    # Contract-required fields
    assert sidecar["contract_version"] == CONTRACT_VERSION
    assert sidecar["model"] == "hrrr"
    assert sidecar["region"] == "pnw"
    assert sidecar["run"] == "20260217_06z"
    assert sidecar["var"] == "tmp2m"
    assert sidecar["fh"] == 3
    assert sidecar["valid_time"] == "2026-02-17T09:00:00Z"  # 06z + 3h
    assert sidecar["units"] == "°F"
    assert sidecar["kind"] == "continuous"
    assert isinstance(sidecar["min"], (int, float))
    assert isinstance(sidecar["max"], (int, float))

    # Legend
    legend = sidecar["legend"]
    assert legend["type"] == "gradient"
    assert isinstance(legend["stops"], list)
    assert len(legend["stops"]) > 0
    # Each stop should be [value, hex_color]
    for stop in legend["stops"]:
        assert len(stop) == 2, f"Legend stop should be [value, color], got {stop}"
        assert isinstance(stop[0], (int, float))
        assert isinstance(stop[1], str)

    # Pretty-print for visual inspection
    print(f"  Sidecar JSON:")
    print(f"    contract_version: {sidecar['contract_version']}")
    print(f"    valid_time:       {sidecar['valid_time']}")
    print(f"    units:            {sidecar['units']}")
    print(f"    kind:             {sidecar['kind']}")
    print(f"    min/max:          {sidecar['min']:.1f} / {sidecar['max']:.1f}")
    print(f"    legend.type:      {legend['type']}")
    print(f"    legend.stops:     {len(legend['stops'])} stops")
    print("  Sidecar JSON: PASS")


def test_run_id_helpers():
    """Test run_id formatting and parsing."""
    dt = datetime(2026, 2, 17, 6, tzinfo=timezone.utc)
    run_id = _run_id_from_date(dt)
    assert run_id == "20260217_06z", f"Expected '20260217_06z', got {run_id!r}"

    parsed = _parse_run_id("20260217_06z")
    assert parsed.year == 2026 and parsed.month == 2 and parsed.day == 17
    assert parsed.hour == 6

    assert _format_units("F") == "°F"
    assert _format_units("mph") == "mph"
    assert _format_units("dBZ") == "dBZ"
    print("  Run ID helpers: PASS")


def test_validate_cog_rejects_bad_band_count():
    """Gate 1 should reject a COG with wrong band count."""
    bbox, grid_m = get_grid_params(MODEL, REGION)
    transform, height, width = compute_transform_and_shape(bbox, grid_m)

    # Write a valid value COG, then validate it pretending it should be RGBA
    data = np.random.uniform(-40, 120, (height, width)).astype(np.float32)

    with tempfile.TemporaryDirectory() as tmpdir:
        val_path = Path(tmpdir) / "test.val.cog.tif"
        write_value_cog(data, val_path, model=MODEL, region=REGION)

        # Should fail: 1-band COG asked to validate as 4-band
        result = validate_cog(
            val_path, expected_bands=4, expected_dtype="Byte",
            region=REGION, grid_meters=grid_m,
        )
        assert not result, "Gate 1 should reject wrong band count"
    print("  Gate 1 rejection (wrong band count): PASS")


if __name__ == "__main__":
    print("=== Pipeline Integration Tests ===\n")

    test_run_id_helpers()
    test_unit_conversion()
    test_colorize_roundtrip()
    test_validate_cog_gates()
    test_sidecar_json()
    test_validate_cog_rejects_bad_band_count()

    print("\nAll pipeline tests passed.")

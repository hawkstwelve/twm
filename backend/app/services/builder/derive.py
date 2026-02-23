"""Phase 2 derivation helpers for multi-component variables.

Builds derived fields directly from model component VarSpecs:
  - wspd10m: hypot(10u, 10v) converted to mph
  - radar_ptype_combo: indexed palette field from refc + categorical masks
  - precip_ptype_blend: indexed palette field from prate + categorical masks
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import numpy as np
import rasterio
import rasterio.transform

from app.services.builder.fetch import convert_units, fetch_variable
from app.services.colormaps import (
    PRECIP_PTYPE_BINS_PER_TYPE,
    PRECIP_PTYPE_BREAKS,
    PRECIP_PTYPE_ORDER,
    PRECIP_PTYPE_RANGE,
    RADAR_PTYPE_BREAKS,
    RADAR_PTYPE_ORDER,
)


def derive_variable(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    model_plugin: Any,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    """Compute a derived variable field and return source grid metadata."""
    derive_kind = getattr(var_spec_model, "derive", None)
    if derive_kind == "wspd10m":
        return _derive_wspd10m(
            model_id=model_id,
            product=product,
            run_date=run_date,
            fh=fh,
            var_spec_model=var_spec_model,
            model_plugin=model_plugin,
        )
    if derive_kind == "radar_ptype_combo":
        return _derive_radar_ptype_combo(
            model_id=model_id,
            product=product,
            run_date=run_date,
            fh=fh,
            var_spec_model=var_spec_model,
            model_plugin=model_plugin,
        )
    if derive_kind == "precip_ptype_blend":
        return _derive_precip_ptype_blend(
            model_id=model_id,
            product=product,
            run_date=run_date,
            fh=fh,
            var_spec_model=var_spec_model,
            model_plugin=model_plugin,
        )

    raise ValueError(f"Unsupported derive strategy: {derive_kind!r}")


def _resolve_component_var(model_plugin: Any, var_id: str) -> Any:
    spec = model_plugin.get_var(var_id)
    if spec is None:
        raise ValueError(f"Component var {var_id!r} not found in plugin {getattr(model_plugin, 'id', '?')!r}")
    return spec


def _fetch_component(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    model_plugin: Any,
    var_id: str,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    spec = _resolve_component_var(model_plugin, var_id)
    selectors = getattr(spec, "selectors", None)
    if selectors is None or not getattr(selectors, "search", None):
        raise ValueError(f"Component var {var_id!r} has no search patterns")
    search_pattern = selectors.search[0]
    data, crs, transform = fetch_variable(
        model_id=model_id,
        product=product,
        search_pattern=search_pattern,
        run_date=run_date,
        fh=fh,
    )
    return data.astype(np.float32, copy=False), crs, transform


def _derive_wspd10m(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    model_plugin: Any,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    hints = getattr(getattr(var_spec_model, "selectors", None), "hints", {})
    u_component = hints.get("u_component", "10u")
    v_component = hints.get("v_component", "10v")

    u_data, src_crs, src_transform = _fetch_component(
        model_id=model_id,
        product=product,
        run_date=run_date,
        fh=fh,
        model_plugin=model_plugin,
        var_id=u_component,
    )
    v_data, _, _ = _fetch_component(
        model_id=model_id,
        product=product,
        run_date=run_date,
        fh=fh,
        model_plugin=model_plugin,
        var_id=v_component,
    )

    wspd_ms = np.hypot(u_data, v_data, dtype=np.float32)
    wspd = convert_units(wspd_ms, "wspd10m")
    return wspd.astype(np.float32, copy=False), src_crs, src_transform


def _derive_radar_ptype_combo(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    model_plugin: Any,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    min_visible_dbz = 10.0
    hints = getattr(getattr(var_spec_model, "selectors", None), "hints", {})
    refl_id = hints.get("refl_component", "refc")
    rain_id = hints.get("rain_component", "crain")
    snow_id = hints.get("snow_component", "csnow")
    sleet_id = hints.get("sleet_component", "cicep")
    frzr_id = hints.get("frzr_component", "cfrzr")

    refl, src_crs, src_transform = _fetch_component(
        model_id=model_id,
        product=product,
        run_date=run_date,
        fh=fh,
        model_plugin=model_plugin,
        var_id=refl_id,
    )
    rain, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_id=rain_id)
    snow, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_id=snow_id)
    sleet, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_id=sleet_id)
    frzr, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_id=frzr_id)

    mask_stack = np.stack([rain, snow, sleet, frzr], axis=0).astype(np.float32, copy=False)
    mask_max = np.nanmax(mask_stack, axis=0)
    ptype_idx = np.argmax(mask_stack, axis=0).astype(np.int32)
    ptype_codes = np.array(RADAR_PTYPE_ORDER)
    ptype = ptype_codes[ptype_idx]

    rain_mask = mask_stack[0]
    snow_mask = mask_stack[1]
    frzr_transition = (ptype == "frzr") & ((rain_mask > 0) | (snow_mask > 0))
    if np.any(frzr_transition):
        prefer_rain = rain_mask >= snow_mask
        ptype[frzr_transition & prefer_rain] = "rain"
        ptype[frzr_transition & ~prefer_rain] = "snow"

    refl_safe = np.where(np.isfinite(refl), np.maximum(refl, 0.0), np.nan)
    bins_per_type = {k: int(v["count"]) for k, v in RADAR_PTYPE_BREAKS.items()}
    normalized = np.clip(refl_safe / 70.0, 0.0, 1.0)

    indexed = np.full(refl.shape, np.nan, dtype=np.float32)
    for code in RADAR_PTYPE_ORDER:
        breaks = RADAR_PTYPE_BREAKS[code]
        offset = int(breaks["offset"])
        count = bins_per_type[code]
        local_bin = np.clip(np.rint(normalized * (count - 1)), 0, count - 1).astype(np.int32)
        selector = (ptype == code) & np.isfinite(refl_safe) & (mask_max > 0) & (refl_safe >= min_visible_dbz)
        indexed[selector] = (offset + local_bin[selector]).astype(np.float32)

    return indexed, src_crs, src_transform


def _derive_precip_ptype_blend(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    model_plugin: Any,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    hints = getattr(getattr(var_spec_model, "selectors", None), "hints", {})
    prate_id = hints.get("prate_component", "prate")
    rain_id = hints.get("rain_component", "crain")
    snow_id = hints.get("snow_component", "csnow")
    sleet_id = hints.get("sleet_component", "cicep")
    frzr_id = hints.get("frzr_component", "cfrzr")

    prate, src_crs, src_transform = _fetch_component(
        model_id=model_id,
        product=product,
        run_date=run_date,
        fh=fh,
        model_plugin=model_plugin,
        var_id=prate_id,
    )
    rain, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_id=rain_id)
    snow, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_id=snow_id)
    sleet, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_id=sleet_id)
    frzr, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_id=frzr_id)

    # GFS PRATE is typically kg m^-2 s^-1 (equivalent to mm/s) → mm/hr.
    prate_mmhr = np.where(np.isfinite(prate), np.maximum(prate, 0.0) * 3600.0, np.nan).astype(np.float32)

    # Breaks order is authoritative for flattened palette indexing.
    mask_by_code = {
        "rain": rain,
        "snow": snow,
        "sleet": sleet,
        "frzr": frzr,
    }
    stack_for_pick = np.stack([mask_by_code[c] for c in PRECIP_PTYPE_ORDER], axis=0).astype(np.float32, copy=False)
    mask_max = np.nanmax(stack_for_pick, axis=0)
    ptype_idx = np.argmax(stack_for_pick, axis=0).astype(np.int32)
    ptype_codes = np.array(PRECIP_PTYPE_ORDER)
    ptype = ptype_codes[ptype_idx]

    range_min, range_max = float(PRECIP_PTYPE_RANGE[0]), float(PRECIP_PTYPE_RANGE[1])
    normalized = np.clip((prate_mmhr - range_min) / max(range_max - range_min, 1e-6), 0.0, 1.0)

    indexed = np.full(prate_mmhr.shape, np.nan, dtype=np.float32)
    for code in PRECIP_PTYPE_ORDER:
        breaks = PRECIP_PTYPE_BREAKS[code]
        offset = int(breaks["offset"])
        count = int(breaks["count"])
        # Keep bins anchored to the configured per-type count.
        if count != PRECIP_PTYPE_BINS_PER_TYPE:
            count = PRECIP_PTYPE_BINS_PER_TYPE
        local_bin = np.clip(np.rint(normalized * (count - 1)), 0, count - 1).astype(np.int32)
        selector = (ptype == code) & np.isfinite(prate_mmhr) & (prate_mmhr > 0.0) & (mask_max > 0)
        indexed[selector] = (offset + local_bin[selector]).astype(np.float32)

    return indexed, src_crs, src_transform

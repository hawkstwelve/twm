"""Phase 2 derivation helpers for multi-component variables.

Builds derived fields directly from model component VarSpecs:
  - wspd10m: hypot(10u, 10v) converted to mph
  - radar_ptype_combo: indexed palette field from refc + categorical masks
  - precip_ptype_blend: indexed palette field from prate + categorical masks
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

import numpy as np
import rasterio
import rasterio.transform

from app.services.builder.fetch import convert_units, fetch_variable
from app.services.builder.fetch import HerbieTransientUnavailableError
from app.services.colormaps import (
    PRECIP_PTYPE_BINS_PER_TYPE,
    PRECIP_PTYPE_BREAKS,
    PRECIP_PTYPE_ORDER,
    PRECIP_PTYPE_RANGE,
    RADAR_PTYPE_BREAKS,
    RADAR_PTYPE_ORDER,
)


@dataclass(frozen=True)
class DeriveStrategy:
    id: str
    required_inputs: tuple[str, ...]
    output_var_key: str | None
    execute: Callable[..., tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]]


def derive_variable(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None = None,
    model_plugin: Any,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    """Compute a derived variable field and return source grid metadata."""
    derive_kind = (
        getattr(var_capability, "derive_strategy_id", None)
        or getattr(var_spec_model, "derive", None)
    )
    strategy = DERIVE_STRATEGIES.get(str(derive_kind))
    if strategy is None:
        raise ValueError(f"Unsupported derive strategy: {derive_kind!r}")
    return strategy.execute(
        model_id=model_id,
        var_key=var_key,
        product=product,
        run_date=run_date,
        fh=fh,
        var_spec_model=var_spec_model,
        var_capability=var_capability,
        model_plugin=model_plugin,
    )


def _resolve_component_var(model_plugin: Any, var_key: str) -> tuple[str, Any]:
    normalized_key = model_plugin.normalize_var_id(var_key)
    capability = model_plugin.get_var_capability(normalized_key)
    spec = model_plugin.get_var(normalized_key)
    if capability is None and spec is None:
        raise ValueError(
            f"Component var {normalized_key!r} not found in plugin "
            f"{getattr(model_plugin, 'id', '?')!r}"
        )
    selectors = (
        getattr(capability, "selectors", None)
        if capability is not None
        else getattr(spec, "selectors", None)
    )
    if selectors is None or not getattr(selectors, "search", None):
        raise ValueError(f"Component var {normalized_key!r} has no search patterns")
    return normalized_key, selectors


def _fetch_component(
    *,
    model_id: str,
    product: str,
    run_date: datetime,
    fh: int,
    model_plugin: Any,
    var_key: str,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    normalized_var_key, selectors = _resolve_component_var(model_plugin, var_key)
    last_exc: Exception | None = None
    for search_pattern in selectors.search:
        try:
            data, crs, transform = fetch_variable(
                model_id=model_id,
                product=product,
                search_pattern=search_pattern,
                run_date=run_date,
                fh=fh,
            )
            return data.astype(np.float32, copy=False), crs, transform
        except (HerbieTransientUnavailableError, RuntimeError) as exc:
            last_exc = exc
            continue
    if last_exc is not None:
        raise last_exc
    raise ValueError(f"Component var {normalized_var_key!r} has no usable search patterns")


def _derive_wspd10m(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None,
    model_plugin: Any,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    import logging

    logger = logging.getLogger(__name__)
    hints = getattr(getattr(var_spec_model, "selectors", None), "hints", {})
    u_component = hints.get("u_component", "10u")
    v_component = hints.get("v_component", "10v")
    speed_component = hints.get("speed_component")

    # Prefer a direct wind-speed field when available.
    if speed_component:
        try:
            logger.info(
                "wspd10m derive path (model=%s): trying direct speed component=%s",
                model_id,
                speed_component,
            )
            speed_data, src_crs, src_transform = _fetch_component(
                model_id=model_id,
                product=product,
                run_date=run_date,
                fh=fh,
                model_plugin=model_plugin,
                var_key=str(speed_component),
            )
            wspd = convert_units(
                speed_data.astype(np.float32, copy=False),
                var_key=var_key,
                model_id=model_id,
                var_capability=var_capability,
            )
            return wspd.astype(np.float32, copy=False), src_crs, src_transform
        except (HerbieTransientUnavailableError, RuntimeError, ValueError):
            # Fall back to vector magnitude from 10u/10v.
            pass

    try:
        u_data, src_crs, src_transform = _fetch_component(
            model_id=model_id,
            product=product,
            run_date=run_date,
            fh=fh,
            model_plugin=model_plugin,
            var_key=u_component,
        )
        v_data, _, _ = _fetch_component(
            model_id=model_id,
            product=product,
            run_date=run_date,
            fh=fh,
            model_plugin=model_plugin,
            var_key=v_component,
        )
    except (HerbieTransientUnavailableError, RuntimeError, ValueError):
        if not speed_component:
            raise
        raise

    wspd_ms = np.hypot(u_data, v_data, dtype=np.float32)
    wspd = convert_units(
        wspd_ms,
        var_key=var_key,
        model_id=model_id,
        var_capability=var_capability,
    )
    return wspd.astype(np.float32, copy=False), src_crs, src_transform


def _derive_radar_ptype_combo(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None,
    model_plugin: Any,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    del var_key, var_capability
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
        var_key=refl_id,
    )
    rain, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=rain_id)
    snow, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=snow_id)
    sleet, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=sleet_id)
    frzr, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=frzr_id)

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
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None,
    model_plugin: Any,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    del var_key, var_capability
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
        var_key=prate_id,
    )
    rain, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=rain_id)
    snow, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=snow_id)
    sleet, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=sleet_id)
    frzr, _, _ = _fetch_component(model_id=model_id, product=product, run_date=run_date, fh=fh, model_plugin=model_plugin, var_key=frzr_id)

    # GFS PRATE is typically kg m^-2 s^-1 (equivalent to mm/s) → in/hr.
    prate_inhr = np.where(
        np.isfinite(prate),
        np.maximum(prate, 0.0) * 3600.0 * 0.03937007874015748,
        np.nan,
    ).astype(np.float32)

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
    normalized = np.clip((prate_inhr - range_min) / max(range_max - range_min, 1e-6), 0.0, 1.0)

    indexed = np.full(prate_inhr.shape, np.nan, dtype=np.float32)
    for code in PRECIP_PTYPE_ORDER:
        breaks = PRECIP_PTYPE_BREAKS[code]
        offset = int(breaks["offset"])
        count = int(breaks["count"])
        # Keep bins anchored to the configured per-type count.
        if count != PRECIP_PTYPE_BINS_PER_TYPE:
            count = PRECIP_PTYPE_BINS_PER_TYPE
        local_bin = np.clip(np.rint(normalized * (count - 1)), 0, count - 1).astype(np.int32)
        selector = (ptype == code) & np.isfinite(prate_inhr) & (prate_inhr > 0.0) & (mask_max > 0)
        indexed[selector] = (offset + local_bin[selector]).astype(np.float32)

    return indexed, src_crs, src_transform


def _derive_precip_total_cumulative(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None,
    model_plugin: Any,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    hints = getattr(getattr(var_spec_model, "selectors", None), "hints", {})
    apcp_component = hints.get("apcp_component", "apcp_step")
    step_hours_raw = hints.get("step_hours", "6")
    try:
        step_hours = max(1, int(step_hours_raw))
    except (TypeError, ValueError):
        step_hours = 6

    cumulative_kgm2: np.ndarray | None = None
    valid_mask: np.ndarray | None = None
    src_crs: rasterio.crs.CRS | None = None
    src_transform: rasterio.transform.Affine | None = None

    for step_fh in range(step_hours, fh + 1, step_hours):
        step_data, step_crs, step_transform = _fetch_component(
            model_id=model_id,
            product=product,
            run_date=run_date,
            fh=step_fh,
            model_plugin=model_plugin,
            var_key=apcp_component,
        )
        step_clean = np.where(np.isfinite(step_data), np.maximum(step_data, 0.0), 0.0).astype(np.float32)
        step_valid = np.isfinite(step_data)

        if cumulative_kgm2 is None:
            cumulative_kgm2 = step_clean
            valid_mask = step_valid
            src_crs = step_crs
            src_transform = step_transform
            continue

        if step_clean.shape != cumulative_kgm2.shape:
            raise ValueError(
                f"APCP component shape mismatch for {model_id}/{var_key} at fh{step_fh:03d}: "
                f"{step_clean.shape} != {cumulative_kgm2.shape}"
            )

        cumulative_kgm2 = cumulative_kgm2 + step_clean
        valid_mask = np.logical_or(valid_mask, step_valid)

    if cumulative_kgm2 is None or valid_mask is None or src_crs is None or src_transform is None:
        raise ValueError(
            f"No cumulative APCP source steps resolved for {model_id}/{var_key} fh{fh:03d}"
        )

    cumulative_kgm2 = np.where(valid_mask, cumulative_kgm2, np.nan).astype(np.float32)
    cumulative_inches = convert_units(
        cumulative_kgm2,
        var_key=var_key,
        model_id=model_id,
        var_capability=var_capability,
    )
    return cumulative_inches.astype(np.float32, copy=False), src_crs, src_transform


def _derive_snowfall_total_10to1_cumulative(
    *,
    model_id: str,
    var_key: str,
    product: str,
    run_date: datetime,
    fh: int,
    var_spec_model: Any,
    var_capability: Any | None,
    model_plugin: Any,
) -> tuple[np.ndarray, rasterio.crs.CRS, rasterio.transform.Affine]:
    del var_capability
    hints = getattr(getattr(var_spec_model, "selectors", None), "hints", {})
    apcp_component = hints.get("apcp_component", "apcp_step")
    snow_component = hints.get("snow_component", "csnow")
    step_hours_raw = hints.get("step_hours", "6")
    slr_raw = hints.get("slr", "10")
    snow_mask_threshold_raw = hints.get("snow_mask_threshold", "0.5")
    min_step_lwe_raw = hints.get("min_step_lwe_kgm2", "0.01")

    try:
        step_hours = max(1, int(step_hours_raw))
    except (TypeError, ValueError):
        step_hours = 6

    try:
        slr = float(slr_raw)
    except (TypeError, ValueError):
        slr = 10.0
    if slr <= 0.0:
        slr = 10.0

    try:
        snow_mask_threshold = float(snow_mask_threshold_raw)
    except (TypeError, ValueError):
        snow_mask_threshold = 0.5
    snow_mask_threshold = min(max(snow_mask_threshold, 0.0), 1.0)

    try:
        min_step_lwe = float(min_step_lwe_raw)
    except (TypeError, ValueError):
        min_step_lwe = 0.01
    min_step_lwe = max(min_step_lwe, 0.0)

    cumulative_kgm2: np.ndarray | None = None
    valid_mask: np.ndarray | None = None
    src_crs: rasterio.crs.CRS | None = None
    src_transform: rasterio.transform.Affine | None = None

    for step_fh in range(step_hours, fh + 1, step_hours):
        apcp_step, step_crs, step_transform = _fetch_component(
            model_id=model_id,
            product=product,
            run_date=run_date,
            fh=step_fh,
            model_plugin=model_plugin,
            var_key=apcp_component,
        )
        snow_mask, _, _ = _fetch_component(
            model_id=model_id,
            product=product,
            run_date=run_date,
            fh=step_fh,
            model_plugin=model_plugin,
            var_key=snow_component,
        )

        # Categorical snow mask should be binary; reject out-of-range sentinels.
        apcp_valid = np.isfinite(apcp_step) & (apcp_step >= 0.0)
        snow_valid = np.isfinite(snow_mask) & (snow_mask >= 0.0) & (snow_mask <= 1.0)

        step_apcp_clean = np.where(apcp_valid, apcp_step, 0.0).astype(np.float32, copy=False)
        if min_step_lwe > 0.0:
            step_apcp_clean = np.where(
                step_apcp_clean >= min_step_lwe,
                step_apcp_clean,
                0.0,
            ).astype(np.float32, copy=False)

        step_snow_binary = np.where(
            snow_valid & (snow_mask >= snow_mask_threshold),
            1.0,
            0.0,
        ).astype(np.float32)
        step_snow_kgm2 = step_apcp_clean * step_snow_binary
        step_valid = apcp_valid & snow_valid

        if cumulative_kgm2 is None:
            cumulative_kgm2 = step_snow_kgm2
            valid_mask = step_valid
            src_crs = step_crs
            src_transform = step_transform
            continue

        if step_snow_kgm2.shape != cumulative_kgm2.shape:
            raise ValueError(
                f"Snowfall component shape mismatch for {model_id}/{var_key} at fh{step_fh:03d}: "
                f"{step_snow_kgm2.shape} != {cumulative_kgm2.shape}"
            )

        cumulative_kgm2 = cumulative_kgm2 + step_snow_kgm2
        valid_mask = np.logical_or(valid_mask, step_valid)

    if cumulative_kgm2 is None or valid_mask is None or src_crs is None or src_transform is None:
        raise ValueError(
            f"No cumulative snowfall source steps resolved for {model_id}/{var_key} fh{fh:03d}"
        )

    cumulative_kgm2 = np.where(valid_mask, cumulative_kgm2, np.nan).astype(np.float32)
    # 1 kg/m^2 == 1 mm LWE. Convert to inches liquid then apply fixed 10:1 SLR.
    cumulative_snow_inches = cumulative_kgm2 * 0.03937007874015748 * slr
    return cumulative_snow_inches.astype(np.float32, copy=False), src_crs, src_transform


DERIVE_STRATEGIES: dict[str, DeriveStrategy] = {
    "wspd10m": DeriveStrategy(
        id="wspd10m",
        required_inputs=("10u", "10v"),
        output_var_key="wspd10m",
        execute=_derive_wspd10m,
    ),
    "radar_ptype_combo": DeriveStrategy(
        id="radar_ptype_combo",
        required_inputs=("refc", "crain", "csnow", "cicep", "cfrzr"),
        output_var_key="radar_ptype",
        execute=_derive_radar_ptype_combo,
    ),
    "precip_ptype_blend": DeriveStrategy(
        id="precip_ptype_blend",
        required_inputs=("prate", "crain", "csnow", "cicep", "cfrzr"),
        output_var_key="precip_ptype",
        execute=_derive_precip_ptype_blend,
    ),
    "precip_total_cumulative": DeriveStrategy(
        id="precip_total_cumulative",
        required_inputs=("apcp_step",),
        output_var_key="precip_total",
        execute=_derive_precip_total_cumulative,
    ),
    "snowfall_total_10to1_cumulative": DeriveStrategy(
        id="snowfall_total_10to1_cumulative",
        required_inputs=("apcp_step", "csnow"),
        output_var_key="snowfall_total",
        execute=_derive_snowfall_total_10to1_cumulative,
    ),
}

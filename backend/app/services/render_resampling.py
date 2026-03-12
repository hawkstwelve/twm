"""Shared render-time resampling policy by variable kind.

This module keeps tile extraction and loop WebP downscaling behavior aligned.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping
from functools import lru_cache
from typing import Any

from rasterio.enums import Resampling

from ..models.registry import list_model_capabilities
from .colormaps import get_color_map_spec

logger = logging.getLogger(__name__)

_DISCRETE_KINDS = {"discrete", "indexed", "categorical"}
_VALUE_RENDER_MIN_MODEL_KM = 10.0
_VALUE_RENDER_MODEL_ALLOWLIST = {"gfs"}
_TARGETED_VALUE_RENDER_MODELS = {"hrrr", "nam", "nbm"}
_TARGETED_VALUE_RENDER_VARS = {"snowfall_total", "snowfall_kuchera_total", "precip_total"}
_TARGETED_LOOP_FIXED_WIDTHS: dict[int, int] = {
    0: 2300,
    1: 3400,
}
_TARGETED_LOOP_FIXED_WIDTHS_BY_VAR: dict[tuple[str, int], int] = {
    ("radar_ptype", 0): 3072,
    ("radar_ptype", 1): 3200,
}
_TARGETED_LOOP_MAX_DIMS: dict[tuple[str, int], int] = {
    ("radar_ptype", 0): 2048,
}
_TARGETED_LOOP_QUALITY: dict[tuple[str, int], int] = {
    ("radar_ptype", 0): 92,
    ("radar_ptype", 1): 90,
}
_MODEL_GRID_KM_FALLBACK: dict[str, float] = {
    "gfs": 25.0,
}
_SUPPORTED_DISPLAY_RESAMPLING = {"nearest", "bilinear"}
_warned_unknown_kind: set[tuple[str, str]] = set()
_unknown_kind_hits: dict[tuple[str, str], int] = {}
_fixed_loop_size_log_lock = threading.Lock()
_fixed_loop_size_logged: set[tuple[str, str, str, int]] = set()


def _normalize_kind(kind: Any) -> str:
    return str(kind or "").strip().lower()


@lru_cache(maxsize=64)
def _lookup_kind_from_capabilities(model_id: str, var_key: str) -> str | None:
    entry = _lookup_variable_catalog_entry(model_id, var_key)
    if entry is None:
        return None

    kind = _normalize_kind(getattr(entry, "kind", None))
    return kind or None


@lru_cache(maxsize=64)
def _lookup_variable_catalog_entry(model_id: str, var_key: str) -> Any | None:
    capabilities = list_model_capabilities().get(model_id)
    if capabilities is None:
        return None

    catalog = getattr(capabilities, "variable_catalog", None)
    if not isinstance(catalog, Mapping):
        return None
    return catalog.get(var_key)


@lru_cache(maxsize=32)
def _lookup_model_grid_km(model_id: str) -> float | None:
    capabilities = list_model_capabilities().get(model_id)
    if capabilities is not None:
        grid_map = getattr(capabilities, "grid_meters_by_region", None)
        if isinstance(grid_map, Mapping) and grid_map:
            canonical_region = str(getattr(capabilities, "canonical_region", "") or "")
            if canonical_region and canonical_region in grid_map:
                try:
                    return float(grid_map[canonical_region]) / 1000.0
                except (TypeError, ValueError):
                    pass

            values_km: list[float] = []
            for value in grid_map.values():
                try:
                    parsed = float(value)
                except (TypeError, ValueError):
                    continue
                if parsed > 0:
                    values_km.append(parsed / 1000.0)
            if values_km:
                return min(values_km)

    fallback = _MODEL_GRID_KM_FALLBACK.get(model_id)
    if fallback is None:
        return None
    try:
        return float(fallback)
    except (TypeError, ValueError):
        return None


def variable_kind(model_id: str, var_key: str) -> str | None:
    model_norm = str(model_id or "").strip().lower()
    var_norm = str(var_key or "").strip().lower()
    if not model_norm or not var_norm:
        return None
    return _lookup_kind_from_capabilities(model_norm, var_norm)


@lru_cache(maxsize=64)
def display_resampling_override(model_id: str, var_key: str) -> str | None:
    model_norm = str(model_id or "").strip().lower()
    var_norm = str(var_key or "").strip().lower()
    if not model_norm or not var_norm:
        return None

    color_map_id = variable_color_map_id(model_norm, var_norm)
    if not color_map_id:
        return None

    try:
        spec = get_color_map_spec(color_map_id)
    except KeyError:
        return None

    override = str(spec.get("display_resampling_override") or "").strip().lower()
    if override in _SUPPORTED_DISPLAY_RESAMPLING:
        return override
    return None


def resampling_name_for_kind(
    *,
    model_id: str,
    var_key: str,
    kind: str | None = None,
) -> str:
    """Resolve render-time resampling name with bilinear fallback.

    Continuous/unknown -> bilinear
    Discrete/indexed/categorical -> nearest
    """
    model_norm = str(model_id or "").strip().lower()
    var_norm = str(var_key or "").strip().lower()
    override = display_resampling_override(model_norm, var_norm)
    if override is not None:
        return override

    resolved_kind = _normalize_kind(kind) or _normalize_kind(variable_kind(model_norm, var_norm))

    if resolved_kind in _DISCRETE_KINDS:
        return "nearest"
    if resolved_kind == "continuous":
        return "bilinear"

    key = (model_norm or "<unknown-model>", var_norm or "<unknown-var>")
    _unknown_kind_hits[key] = _unknown_kind_hits.get(key, 0) + 1
    if key not in _warned_unknown_kind:
        _warned_unknown_kind.add(key)
        logger.warning(
            "Unknown or missing variable kind for model=%s var=%s (kind=%r); "
            "defaulting resampling to bilinear (hits=%d)",
            model_norm,
            var_norm,
            resolved_kind or None,
            _unknown_kind_hits[key],
        )
    return "bilinear"


def variable_color_map_id(model_id: str, var_key: str) -> str | None:
    model_norm = str(model_id or "").strip().lower()
    var_norm = str(var_key or "").strip().lower()
    if not model_norm or not var_norm:
        return None

    entry = _lookup_variable_catalog_entry(model_norm, var_norm)
    if entry is None:
        return None
    color_map_id = getattr(entry, "color_map_id", None)
    if not isinstance(color_map_id, str):
        return None
    resolved = color_map_id.strip()
    return resolved or None


def model_grid_km(model_id: str) -> float | None:
    model_norm = str(model_id or "").strip().lower()
    if not model_norm:
        return None
    return _lookup_model_grid_km(model_norm)


def use_value_render_for_variable(
    *,
    model_id: str,
    var_key: str,
    kind: str | None = None,
) -> bool:
    model_norm = str(model_id or "").strip().lower()
    var_norm = str(var_key or "").strip().lower()
    if not model_norm or not var_norm:
        return False

    resolved_kind = _normalize_kind(kind) or _normalize_kind(variable_kind(model_norm, var_norm))
    if resolved_kind != "continuous":
        return False

    if model_norm in _TARGETED_VALUE_RENDER_MODELS:
        return var_norm in _TARGETED_VALUE_RENDER_VARS

    model_km = model_grid_km(model_norm)
    if model_km is None or model_km < _VALUE_RENDER_MIN_MODEL_KM:
        return False

    if model_norm not in _VALUE_RENDER_MODEL_ALLOWLIST:
        return False
    return True


def render_resampling_name(
    *,
    model_id: str,
    var_key: str,
    kind: str | None = None,
) -> str:
    name = resampling_name_for_kind(model_id=model_id, var_key=var_key, kind=kind)
    if name == "nearest" and use_value_render_for_variable(model_id=model_id, var_key=var_key, kind=kind):
        return "bilinear"
    return name


def loop_resampling_name(
    *,
    model_id: str,
    var_key: str,
    kind: str | None = None,
) -> str:
    return render_resampling_name(model_id=model_id, var_key=var_key, kind=kind)


def use_fixed_loop_size_for_variable(
    *,
    model_id: str,
    var_key: str,
    kind: str | None = None,
) -> bool:
    model_norm = str(model_id or "").strip().lower()
    var_norm = str(var_key or "").strip().lower()
    if not model_norm or not var_norm:
        return False

    if (var_norm, 0) in _TARGETED_LOOP_FIXED_WIDTHS_BY_VAR or (var_norm, 1) in _TARGETED_LOOP_FIXED_WIDTHS_BY_VAR:
        return True

    resolved_kind = _normalize_kind(kind) or _normalize_kind(variable_kind(model_norm, var_norm))
    if resolved_kind != "continuous":
        return False

    return True


def compute_loop_output_shape(
    *,
    model_id: str,
    var_key: str,
    src_h: int,
    src_w: int,
    max_dim: int,
    fixed_width: int,
    kind: str | None = None,
) -> tuple[int, int, bool]:
    if src_h <= 0 or src_w <= 0:
        return 0, 0, False

    if use_fixed_loop_size_for_variable(model_id=model_id, var_key=var_key, kind=kind):
        out_w = max(1, int(fixed_width))
        ratio = float(out_w) / float(src_w)
        out_h = max(1, int(round(float(src_h) * ratio)))
        return out_h, out_w, True

    max_side = max(src_h, src_w)
    safe_max_dim = max(1, int(max_dim))
    scale = min(1.0, float(safe_max_dim) / float(max_side))
    out_h = max(1, int(round(src_h * scale)))
    out_w = max(1, int(round(src_w * scale)))
    return out_h, out_w, False


def loop_fixed_width_for_tier(
    *,
    model_id: str,
    var_key: str,
    tier: int,
    default_width: int,
) -> int:
    model_norm = str(model_id or "").strip().lower()
    var_norm = str(var_key or "").strip().lower()
    try:
        tier_int = int(tier)
    except (TypeError, ValueError):
        tier_int = 0

    var_override = _TARGETED_LOOP_FIXED_WIDTHS_BY_VAR.get((var_norm, tier_int))
    if var_override is not None:
        return max(1, int(var_override))

    if model_norm in _TARGETED_VALUE_RENDER_MODELS and var_norm in _TARGETED_VALUE_RENDER_VARS:
        override = _TARGETED_LOOP_FIXED_WIDTHS.get(tier_int)
        if override is not None:
            return max(1, int(override))
    return max(1, int(default_width))


def loop_max_dim_for_tier(
    *,
    model_id: str,
    var_key: str,
    tier: int,
    default_max_dim: int,
) -> int:
    del model_id
    var_norm = str(var_key or "").strip().lower()
    try:
        tier_int = int(tier)
    except (TypeError, ValueError):
        tier_int = 0

    override = _TARGETED_LOOP_MAX_DIMS.get((var_norm, tier_int))
    if override is not None:
        return max(1, int(override))
    return max(1, int(default_max_dim))


def loop_quality_for_tier(
    *,
    model_id: str,
    var_key: str,
    tier: int,
    default_quality: int,
) -> int:
    del model_id
    var_norm = str(var_key or "").strip().lower()
    try:
        tier_int = int(tier)
    except (TypeError, ValueError):
        tier_int = 0

    override = _TARGETED_LOOP_QUALITY.get((var_norm, tier_int))
    if override is not None:
        return max(1, min(100, int(override)))
    return max(1, min(100, int(default_quality)))


def log_fixed_loop_size_once(
    *,
    model_id: str,
    run_id: str | None,
    var_key: str,
    tier: int,
    src_h: int,
    src_w: int,
    out_h: int,
    out_w: int,
) -> None:
    model_norm = str(model_id or "").strip().lower() or "<unknown-model>"
    run_norm = str(run_id or "").strip() or "<unknown-run>"
    var_norm = str(var_key or "").strip().lower() or "<unknown-var>"
    key = (run_norm, model_norm, var_norm, int(tier))
    with _fixed_loop_size_log_lock:
        if key in _fixed_loop_size_logged:
            return
        _fixed_loop_size_logged.add(key)
    logger.info(
        "Loop fixed sizing applied: run=%s model=%s var=%s tier=%d src=%dx%d out=%dx%d",
        run_norm,
        model_norm,
        var_norm,
        int(tier),
        int(src_w),
        int(src_h),
        int(out_w),
        int(out_h),
    )


def high_quality_loop_resampling() -> Resampling:
    lanczos = getattr(Resampling, "lanczos", None)
    if lanczos is not None:
        return lanczos
    cubic = getattr(Resampling, "cubic", None)
    if cubic is not None:
        return cubic
    return Resampling.bilinear


def rio_tiler_resampling_kwargs(
    *,
    model_id: str,
    var_key: str,
    kind: str | None = None,
) -> dict[str, str]:
    name = render_resampling_name(model_id=model_id, var_key=var_key, kind=kind)
    return {
        "resampling_method": name,
        "reproject_method": name,
    }


def rasterio_resampling_for_loop(
    *,
    model_id: str,
    var_key: str,
    kind: str | None = None,
) -> Resampling:
    name = loop_resampling_name(model_id=model_id, var_key=var_key, kind=kind)
    return Resampling.nearest if name == "nearest" else Resampling.bilinear

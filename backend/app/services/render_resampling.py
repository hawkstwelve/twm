"""Shared render-time resampling policy by variable kind.

This module keeps tile extraction and loop WebP downscaling behavior aligned.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from functools import lru_cache
from typing import Any

from rasterio.enums import Resampling

from ..models.registry import list_model_capabilities

logger = logging.getLogger(__name__)

_DISCRETE_KINDS = {"discrete", "indexed", "categorical"}
_warned_unknown_kind: set[tuple[str, str]] = set()
_unknown_kind_hits: dict[tuple[str, str], int] = {}


def _normalize_kind(kind: Any) -> str:
    return str(kind or "").strip().lower()


@lru_cache(maxsize=64)
def _lookup_kind_from_capabilities(model_id: str, var_key: str) -> str | None:
    capabilities = list_model_capabilities().get(model_id)
    if capabilities is None:
        return None

    catalog = getattr(capabilities, "variable_catalog", None)
    if not isinstance(catalog, Mapping):
        return None

    entry = catalog.get(var_key)
    if entry is None:
        return None

    kind = _normalize_kind(getattr(entry, "kind", None))
    return kind or None


def variable_kind(model_id: str, var_key: str) -> str | None:
    model_norm = str(model_id or "").strip().lower()
    var_norm = str(var_key or "").strip().lower()
    if not model_norm or not var_norm:
        return None
    return _lookup_kind_from_capabilities(model_norm, var_norm)


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


def rio_tiler_resampling_kwargs(
    *,
    model_id: str,
    var_key: str,
    kind: str | None = None,
) -> dict[str, str]:
    name = resampling_name_for_kind(model_id=model_id, var_key=var_key, kind=kind)
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
    name = resampling_name_for_kind(model_id=model_id, var_key=var_key, kind=kind)
    return Resampling.nearest if name == "nearest" else Resampling.bilinear

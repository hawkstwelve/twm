from __future__ import annotations

import logging

from fastapi import HTTPException

from .base import ModelCapabilities, ModelPlugin
from .hrrr import HRRR_MODEL

logger = logging.getLogger(__name__)

MODEL_REGISTRY: dict[str, ModelPlugin] = {
    HRRR_MODEL.id: HRRR_MODEL,
}

try:
    from .gfs import GFS_MODEL
    MODEL_REGISTRY[GFS_MODEL.id] = GFS_MODEL
except ImportError as exc:
    logger.warning("GFS plugin unavailable (missing dependency): %s", exc)

try:
    from .nam import NAM_MODEL
    MODEL_REGISTRY[NAM_MODEL.id] = NAM_MODEL
except ImportError as exc:
    logger.warning("NAM plugin unavailable (missing dependency): %s", exc)

try:
    from .nbm import NBM_MODEL
    MODEL_REGISTRY[NBM_MODEL.id] = NBM_MODEL
except ImportError as exc:
    logger.warning("NBM plugin unavailable (missing dependency): %s", exc)


def get_model(model_id: str) -> ModelPlugin:
    model = MODEL_REGISTRY.get(model_id)
    if model is None:
        raise HTTPException(status_code=404, detail=f"Unknown model: {model_id}")
    return model


def get_model_capabilities(model_id: str) -> ModelCapabilities:
    model = get_model(model_id)
    capabilities = getattr(model, "capabilities", None)
    if capabilities is None:
        raise HTTPException(
            status_code=500,
            detail=f"Capabilities unavailable for model: {model_id}",
        )
    return capabilities


def list_model_capabilities() -> dict[str, ModelCapabilities]:
    capabilities_by_model: dict[str, ModelCapabilities] = {}
    for model_id, model in MODEL_REGISTRY.items():
        capabilities = getattr(model, "capabilities", None)
        if capabilities is not None:
            capabilities_by_model[model_id] = capabilities
    return capabilities_by_model

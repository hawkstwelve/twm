from __future__ import annotations

import logging
from typing import Mapping

from fastapi import HTTPException

from .base import ModelPlugin
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


def get_model(model_id: str) -> ModelPlugin:
    model = MODEL_REGISTRY.get(model_id)
    if model is None:
        raise HTTPException(status_code=404, detail=f"Unknown model: {model_id}")
    return model

from __future__ import annotations

from typing import Mapping

from fastapi import HTTPException

from .base import ModelPlugin
from .gfs import GFS_MODEL
from .hrrr import HRRR_MODEL


MODEL_REGISTRY: dict[str, ModelPlugin] = {
    HRRR_MODEL.id: HRRR_MODEL,
    GFS_MODEL.id: GFS_MODEL,
}


def get_model(model_id: str) -> ModelPlugin:
    model = MODEL_REGISTRY.get(model_id)
    if model is None:
        raise HTTPException(status_code=404, detail=f"Unknown model: {model_id}")
    return model

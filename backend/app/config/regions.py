from __future__ import annotations

REGION_PRESETS: dict[str, dict] = {
    "pnw": {
        "label": "Pacific Northwest",
        "bbox": [-126.0, 41.5, -116.0, 49.5],
        "defaultCenter": [-120.8, 45.6],
        "defaultZoom": 6,
        "minZoom": 3,
        "maxZoom": 10,
    },
    "north_central": {
        "label": "North Central",
        "bbox": [-106.0, 40.0, -92.0, 50.0],
        "defaultCenter": [-99.0, 45.0],
        "defaultZoom": 6,
        "minZoom": 3,
        "maxZoom": 10,
    },
    "conus": {
        "label": "CONUS",
        "bbox": [-125.0, 24.0, -66.5, 50.0],
        "defaultCenter": [-98.58, 39.83],
        "defaultZoom": 4,
        "minZoom": 2,
        "maxZoom": 8,
    },
}

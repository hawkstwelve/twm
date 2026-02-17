"""V2 palette constants for COG-encoded tiles.

Band 1 in each COG stores a byte: a palette index for discrete fields, or a fixed-range
byte (0–255) for continuous fields. Band 2 stores alpha as a byte. Runtime tiles are
rendered by mapping LUT[band1] and applying band2 as the output alpha.
"""

from __future__ import annotations

import numpy as np

# Precipitation type configuration with levels and colors
RAIN_LEVELS = [0.01, 0.1, 0.25, 0.5, 1.0, 1.5, 2.5, 4, 6, 10, 16, 24]
SNOW_LEVELS = [0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0, 6.0, 8.0, 10.0, 12.0, 14.0]
WINTER_LEVELS = [0.1, 0.5, 1, 2, 3, 4, 6, 10, 14]

PRECIP_CONFIG = {
    "rain": {
        "levels": RAIN_LEVELS,
        "colors": [
            "#90ee90", "#66dd66", "#33cc33", "#00bb00", "#009900", "#007700",
            "#005500", "#ffff00", "#ffb300", "#ff6600", "#ff0000", "#ff00ff",
        ],
    },
    "frzr": {
        "levels": WINTER_LEVELS,
        "colors": [
            "#ffc0cb", "#ff69b4", "#ff1493", "#c71585", "#931040", "#b03060",
            "#d20000", "#ff2400", "#ff4500",
        ],
    },
    "sleet": {
        "levels": WINTER_LEVELS,
        "colors": [
            "#e0ffff", "#add8e6", "#9370db", "#8a2be2", "#9400d3", "#800080",
            "#4b0082", "#8b008b", "#b22222",
        ],
    },
    "snow": {
        "levels": SNOW_LEVELS,
        "colors": [
            "#c0ffff", "#55ffff", "#4feaff", "#48d3ff", "#42bfff", "#3caaff",
            "#3693ff", "#2a69f1", "#1d42ca", "#1b18dc", "#161fb8", "#130495",
            "#130495", "#550a87", "#550a87", "#af068e", "#ea0081",
        ],
    },
}

PRECIP_PTYPE_ORDER = ("frzr", "sleet", "snow", "rain")
PRECIP_PTYPE_BINS_PER_TYPE = 64
PRECIP_PTYPE_RANGE = (
    0.0,
    float(
        max(
            max(PRECIP_CONFIG["rain"]["levels"]),
            max(PRECIP_CONFIG["snow"]["levels"]),
            max(PRECIP_CONFIG["sleet"]["levels"]),
            max(PRECIP_CONFIG["frzr"]["levels"]),
        )
    ),
)


def _hex_to_rgb(hex_color: str) -> np.ndarray:
    hex_str = hex_color.strip().lstrip("#")
    return np.array(
        [
            int(hex_str[0:2], 16),
            int(hex_str[2:4], 16),
            int(hex_str[4:6], 16),
        ],
        dtype=np.float64,
    )


def _rgb_to_hex(rgb: np.ndarray) -> str:
    r, g, b = np.clip(np.rint(rgb), 0, 255).astype(np.uint8).tolist()
    return f"#{r:02x}{g:02x}{b:02x}"


def _expand_hex_ramp(colors_hex: list[str], n: int) -> list[str]:
    if not colors_hex:
        raise ValueError("colors_hex must not be empty")
    if len(colors_hex) == 1:
        return [colors_hex[0]] * n

    anchors = np.stack([_hex_to_rgb(color) for color in colors_hex], axis=0)
    stop_positions = np.linspace(0.0, 1.0, num=len(colors_hex), dtype=np.float64)
    target_positions = np.linspace(0.0, 1.0, num=n, dtype=np.float64)

    r = np.interp(target_positions, stop_positions, anchors[:, 0])
    g = np.interp(target_positions, stop_positions, anchors[:, 1])
    b = np.interp(target_positions, stop_positions, anchors[:, 2])
    return [_rgb_to_hex(np.array([rr, gg, bb], dtype=np.float64)) for rr, gg, bb in zip(r, g, b)]


def _build_precip_ptype_flat_palette() -> tuple[
    list[float],
    list[str],
    dict[str, dict[str, int]],
    dict[str, list[float]],
]:
    colors: list[str] = []
    breaks: dict[str, dict[str, int]] = {}
    levels_by_type: dict[str, list[float]] = {}
    for idx, key in enumerate(PRECIP_PTYPE_ORDER):
        cfg = PRECIP_CONFIG[key]
        type_colors = _expand_hex_ramp(list(cfg["colors"]), PRECIP_PTYPE_BINS_PER_TYPE)
        offset = idx * PRECIP_PTYPE_BINS_PER_TYPE
        colors.extend(type_colors)
        breaks[key] = {
            "offset": offset,
            "count": PRECIP_PTYPE_BINS_PER_TYPE,
        }
        levels_by_type[key] = np.linspace(
            PRECIP_PTYPE_RANGE[0],
            PRECIP_PTYPE_RANGE[1],
            num=PRECIP_PTYPE_BINS_PER_TYPE,
            dtype=float,
        ).tolist()
    levels = np.linspace(
        PRECIP_PTYPE_RANGE[0],
        PRECIP_PTYPE_RANGE[1],
        num=len(colors),
        dtype=float,
    ).tolist()
    return levels, colors, breaks, levels_by_type


(
    PRECIP_PTYPE_LEVELS,
    PRECIP_PTYPE_COLORS,
    PRECIP_PTYPE_BREAKS,
    PRECIP_PTYPE_LEVELS_BY_TYPE,
) = _build_precip_ptype_flat_palette()

# Radar reflectivity configuration with dBZ levels and colors for each precipitation type
RADAR_CONFIG = {
    "rain": {
        "levels": [0, 10, 15, 20, 23, 25, 28, 30, 33, 35, 38, 40, 43, 45, 48, 50, 53, 55, 58, 60, 70],
        "colors": [
            "#ffffff", "#4efb4c", "#46e444", "#3ecd3d", "#36b536", "#2d9e2e", "#258528",
            "#1d6e1f", "#155719", "#feff50", "#fad248", "#f8a442", "#f6763c", "#f5253a",
            "#de0a35", "#c21230", "#9c0045", "#bc0f9c", "#e300c1", "#f600dc",
        ],
    },
    "frzr": {
        "levels": [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 70],
        "colors": [
            "#ffffff", "#fbcad0", "#f893ba", "#e96c9f", "#dd88a5", "#dc4f8b", "#d03a80",
            "#c62773", "#bd1366", "#b00145", "#c21230", "#da2d0d", "#e33403", "#f53c00",
            "#f53c00", "#f54603",
        ],
    },
    "sleet": {
        "levels": [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 70],
        "colors": [
            "#ffffff", "#b49dff", "#b788ff", "#c56cff", "#c54ef9", "#c54ef9", "#b730e7",
            "#a913d3", "#a913d3", "#9b02b4", "#bc0f9c", "#a50085", "#c52c7b", "#cf346f",
            "#d83c64", "#e24556",
        ],
    },
    "snow": {
        "levels": [0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48, 52, 56, 60, 70],
        "colors": [
            "#ffffff", "#55ffff", "#4feaff", "#48d3ff", "#42bfff", "#3caaff", "#3693ff",
            "#2a6aee", "#1e40d0", "#110ba7", "#2a009a", "#0c276f", "#540093", "#bc0f9c",
            "#d30085", "#f5007f",
        ],
    },
}

RADAR_PTYPE_ORDER = ("rain", "snow", "sleet", "frzr")


def _build_radar_ptype_flat_palette() -> tuple[list[float], list[str], dict[str, dict[str, int]]]:
    levels: list[float] = []
    colors: list[str] = []
    breaks: dict[str, dict[str, int]] = {}
    offset = 0
    for key in RADAR_PTYPE_ORDER:
        cfg = RADAR_CONFIG[key]
        type_levels = list(cfg["levels"])
        type_colors = list(cfg["colors"])
        levels.extend(type_levels)
        colors.extend(type_colors)
        breaks[key] = {
            "offset": offset,
            "count": len(type_colors),
        }
        offset += len(type_colors)
    return levels, colors, breaks


RADAR_PTYPE_LEVELS, RADAR_PTYPE_COLORS, RADAR_PTYPE_BREAKS = _build_radar_ptype_flat_palette()

# 2m temperature (°F) palette
temp_colors = [
    "#e8d0d8", "#d8b0c8", "#c080b0", "#9050a0", "#703090",
    "#a070b0", "#c8a0d0", "#e8e0f0", "#d0e0f0", "#a0c0e0",
    "#7090c0", "#4070b0", "#2050a0", "#103070",
    "#204048", "#406058", "#709078", "#a0c098", "#d0e0b0",
    "#f0f0c0", "#e0d0a0", "#c0b080", "#a08060", "#805040",
    "#602018", "#801010", "#a01010", "#702020",
    "#886666", "#a08888", "#c0a0a0", "#d8c8c8", "#e8e0e0",
    "#b0a0a0", "#807070", "#504040",
]

# Total precipitation (inches)
precip_colors = [
    "#c0c0c0", "#909090", "#606060",
    "#b0f090", "#80e060", "#50c040",
    "#3070f0", "#5090f0", "#80b0f0", "#b0d0f0",
    "#ffff80", "#ffd060", "#ffa040",
    "#ff6030", "#e03020", "#a01010", "#700000",
    "#d0b0e0", "#b080d0", "#9050c0", "#7020a0",
    "#c040c0",
]
precip_levels = [
    0.01, 0.05, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9, 1.2, 1.6,
    2.0, 3.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0, 25.0,
]

# Total snowfall (inches, 10:1 ratio)
snow_colors = [
    "#ffffff", "#dbdbdb", "#959595", "#6e6e6e", "#505050",
    "#96d1fa", "#78b9fb", "#50a5f5", "#3c97f5", "#3083f1",
    "#2b6eeb", "#2664d3", "#215ac3",
    "#3e0091", "#4c008f", "#5a008d", "#67008a", "#860087",
    "#a10285", "#c90181", "#f3027c",
    "#f41484", "#f53b9b", "#f65faf", "#f76eb7", "#f885c3",
    "#f58dc7", "#ea95ca", "#e79dcd", "#d9acd5", "#cfb2d6",
    "#c1c7dd", "#b6d8ec", "#a9e3ef", "#a1eff3", "#94f8f6",
    "#8dedeb", "#7edbd9", "#73c0c7", "#7cb9ca", "#81b7cd",
    "#88b0ce", "#8db0d0", "#90b0d2", "#93abd7", "#93abd7",
    "#99a7db", "#9da5dd", "#a5a0df", "#a5a0df", "#af9be7",
    "#af9be7", "#ad95e2", "#b795eb", "#b291e5", "#bf91f1",
    "#c68df5", "#c488f0", "#d187f9", "#cb84f3",
]
snow_levels = [
    0.1, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5,
    5.0, 5.5, 6.0, 6.5, 7.0, 7.5, 8.0, 8.5, 9.0, 9.5,
    10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
    20.0, 21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0, 29.0,
    30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 36.0, 37.0, 38.0, 39.0,
    40.0, 41.0, 42.0, 43.0, 44.0, 45.0, 46.0, 47.0, 48.0,
]

# 10m wind speed (mph) - continuous palette with NWS-style legend breakpoints
WSPD10M_COLORS = [
    "#FFFFFF",
    "#E6F2FF",
    "#CCE5FF",
    "#99CCFF",
    "#66B2FF",
    "#3399FF",
    "#66FF66",
    "#33FF33",
    "#00FF00",
    "#CCFF33",
    "#FFFF00",
    "#FFCC00",
    "#FF9900",
    "#FF6600",
    "#FF3300",
    "#FF0000",
    "#CC0000",
    "#990000",
    "#800000",
    "#660033",
    "#660066",
    "#800080",
    "#990099",
    "#B300B3",
    "#CC00CC",
    "#E600E6",
    "#680868",
]

# Legend breakpoints for wind speed (stepped display only, not used for tile LUT)
WSPD10M_LEGEND_STOPS = [
    (0, "#FFFFFF"),
    (4, "#E6F2FF"),
    (6, "#CCE5FF"),
    (8, "#99CCFF"),
    (9, "#66B2FF"),
    (10, "#3399FF"),
    (12, "#66FF66"),
    (14, "#33FF33"),
    (16, "#00FF00"),
    (20, "#CCFF33"),
    (22, "#FFFF00"),
    (24, "#FFCC00"),
    (26, "#FF9900"),
    (30, "#FF6600"),
    (34, "#FF3300"),
    (36, "#FF0000"),
    (40, "#CC0000"),
    (44, "#990000"),
    (48, "#800000"),
    (52, "#660033"),
    (58, "#660066"),
    (64, "#800080"),
    (70, "#990099"),
    (75, "#B300B3"),
    (85, "#CC00CC"),
    (95, "#E600E6"),
    (100, "#680868"),
]
QPF6H_LEGEND_STOPS = list(zip(precip_levels, precip_colors))

# 850mb temperature (°C) continuous palette anchors and range
TEMP850_C_COLOR_ANCHORS = [
    (-40.0, "#E8D0D8"),
    (-34.4, "#D0A0C0"),
    (-28.9, "#A070B0"),
    (-23.3, "#704090"),
    (-17.8, "#8050A0"),
    (-12.2, "#C0D0F0"),
    (-6.7, "#80A0D0"),
    (-1.1, "#4060B0"),
    (0.0, "#204080"),
    (1.7, "#406050"),
    (7.2, "#709070"),
    (12.8, "#D0D090"),
    (18.3, "#B09060"),
    (23.9, "#804030"),
    (29.4, "#901010"),
    (35.0, "#A08080"),
    (40.6, "#D0C0C0"),
    (46.1, "#504040"),
]
TEMP850_C_RANGE = (-40.0, 46.1)

VAR_SPECS = {
    "precip_rain": {
        "type": "discrete",
        "units": "mm/hr",
        "levels": PRECIP_CONFIG["rain"]["levels"],
        "colors": PRECIP_CONFIG["rain"]["colors"],
    },
    "precip_frzr": {
        "type": "discrete",
        "units": "mm/hr",
        "levels": PRECIP_CONFIG["frzr"]["levels"],
        "colors": PRECIP_CONFIG["frzr"]["colors"],
    },
    "precip_sleet": {
        "type": "discrete",
        "units": "mm/hr",
        "levels": PRECIP_CONFIG["sleet"]["levels"],
        "colors": PRECIP_CONFIG["sleet"]["colors"],
    },
    "precip_snow": {
        "type": "discrete",
        "units": "mm/hr",
        "levels": PRECIP_CONFIG["snow"]["levels"],
        "colors": PRECIP_CONFIG["snow"]["colors"],
    },
    "radar_ptype": {
        "type": "discrete",
        "units": "dBZ",
        "levels": RADAR_PTYPE_LEVELS,
        "colors": RADAR_PTYPE_COLORS,
        "display_name": "Composite Reflectivity + P-Type",
        "legend_title": "Composite Reflectivity + P-Type (dBZ)",
        "ptype_order": list(RADAR_PTYPE_ORDER),
        "ptype_breaks": RADAR_PTYPE_BREAKS,
    },
    "precip_total": {
        "type": "discrete",
        "units": "in",
        "levels": precip_levels,
        "colors": precip_colors,
    },
    "qpf6h": {
        "type": "continuous",
        "units": "in",
        "range": (0.0, 6.0),
        "colors": precip_colors,
        "display_name": "6-hr Precip",
        "legend_title": "6-hr Precip (in)",
        "legend_stops": QPF6H_LEGEND_STOPS,
    },
    "precip_ptype": {
        "type": "discrete",
        "units": "mm/hr",
        "levels": PRECIP_PTYPE_LEVELS,
        "colors": PRECIP_PTYPE_COLORS,
        "range": PRECIP_PTYPE_RANGE,
        "bins_per_ptype": PRECIP_PTYPE_BINS_PER_TYPE,
        "display_name": "Precipitation Intensity",
        "legend_title": "Precipitation Rate (mm/hr)",
        "ptype_order": list(PRECIP_PTYPE_ORDER),
        "ptype_breaks": PRECIP_PTYPE_BREAKS,
        "ptype_levels": PRECIP_PTYPE_LEVELS_BY_TYPE,
    },
    "snowfall_total": {
        "type": "discrete",
        "units": "in",
        "levels": snow_levels,
        "colors": snow_colors,
    },
    "tmp2m": {
        "type": "continuous",
        "units": "F",
        "range": (-40.0, 122.5),
        "colors": temp_colors,
        "display_name": "2m Temperature",
        "legend_title": "Temperature (°F)",
    },
    "wspd10m": {
        "type": "continuous",
        "units": "mph",
        "range": (0.0, 100.0),
        "colors": WSPD10M_COLORS,
        "display_name": "10m Wind Speed",
        "legend_title": "Wind Speed (mph)",
        "legend_stops": WSPD10M_LEGEND_STOPS,
    },
    "refc": {
        "type": "discrete",
        "units": "dBZ",
        # Hide "no precip" / near-noise returns by making <10 dBZ transparent.
        # Keep visible echoes starting at the first non-white radar color.
        "levels": RADAR_CONFIG["rain"]["levels"][1:],
        "colors": RADAR_CONFIG["rain"]["colors"][1:],
        "display_name": "Composite Reflectivity",
        "legend_title": "Reflectivity (dBZ)",
    },
}

_LUT_CACHE: dict[str, np.ndarray] = {}


def hex_to_rgba_u8(hex_color: str, alpha: int = 255) -> tuple[int, int, int, int]:
    hex_str = hex_color.strip().lstrip("#")
    if len(hex_str) != 6:
        raise ValueError(f"Invalid hex color: {hex_color}")
    r = int(hex_str[0:2], 16)
    g = int(hex_str[2:4], 16)
    b = int(hex_str[4:6], 16)
    a = int(alpha)
    return r, g, b, a


def build_discrete_lut(colors_hex: list[str]) -> np.ndarray:
    if not colors_hex:
        raise ValueError("colors_hex must contain at least one color")
    lut = np.zeros((256, 4), dtype=np.uint8)
    max_idx = min(len(colors_hex), 256)
    for idx in range(max_idx):
        lut[idx] = hex_to_rgba_u8(colors_hex[idx], 255)
    if max_idx < 256:
        lut[max_idx:] = lut[max_idx - 1]
    return lut


def build_continuous_lut(colors_hex: list[str], n: int = 256) -> np.ndarray:
    if len(colors_hex) < 2:
        raise ValueError("colors_hex must contain at least two colors")
    stops = np.array([hex_to_rgba_u8(color, 255)[:3] for color in colors_hex], dtype=float)
    stop_positions = np.linspace(0.0, 1.0, num=len(colors_hex))
    target_positions = np.linspace(0.0, 1.0, num=n)
    r = np.interp(target_positions, stop_positions, stops[:, 0])
    g = np.interp(target_positions, stop_positions, stops[:, 1])
    b = np.interp(target_positions, stop_positions, stops[:, 2])
    a = np.full(n, 255.0)
    lut = np.stack([r, g, b, a], axis=1).astype(np.uint8)
    return lut


def build_continuous_lut_from_stops(
    stops: list[tuple[float, str]],
    n: int = 256,
    *,
    range_vals: tuple[float, float] | None = None,
) -> np.ndarray:
    if len(stops) < 2:
        raise ValueError("stops must contain at least two entries")

    sorted_stops = sorted(stops, key=lambda item: item[0])
    stop_values = np.array([float(value) for value, _ in sorted_stops], dtype=float)
    stop_colors = np.array([
        hex_to_rgba_u8(color, 255)[:3] for _, color in sorted_stops
    ], dtype=float)

    if range_vals is None:
        range_min, range_max = float(stop_values[0]), float(stop_values[-1])
    else:
        range_min, range_max = float(range_vals[0]), float(range_vals[1])

    if range_max == range_min:
        raise ValueError("stop range must not be zero")

    target_values = np.linspace(range_min, range_max, num=n)
    r = np.interp(target_values, stop_values, stop_colors[:, 0])
    g = np.interp(target_values, stop_values, stop_colors[:, 1])
    b = np.interp(target_values, stop_values, stop_colors[:, 2])
    a = np.full(n, 255.0)
    lut = np.stack([r, g, b, a], axis=1).astype(np.uint8)
    return lut


def get_lut(var_key: str) -> np.ndarray:
    """Build runtime LUT for tile rendering.
    
    For discrete vars: maps byte index to color.
    For continuous vars: always interpolates colors array into 256 steps.
    Never uses legend_stops for LUT generation (stops are legend-only).
    """
    if var_key in _LUT_CACHE:
        return _LUT_CACHE[var_key]
    spec = VAR_SPECS.get(var_key)
    if not spec:
        raise KeyError(f"Unknown var_key: {var_key}")
    colors = spec["colors"]
    if spec["type"] == "discrete":
        lut = build_discrete_lut(colors)
    else:
        # Continuous: always build from colors array, never from stops
        lut = build_continuous_lut(colors, n=256)
    _LUT_CACHE[var_key] = lut
    return lut


def encode_to_byte_and_alpha(
    values: np.ndarray,
    var_key: str,
) -> tuple[np.ndarray, np.ndarray, dict]:
    spec = VAR_SPECS.get(var_key)
    if not spec:
        raise KeyError(f"Unknown var_key: {var_key}")
    kind = spec.get("type")
    if kind not in {"discrete", "continuous"}:
        raise ValueError(f"Unsupported var spec type for {var_key}: {kind}")

    finite_mask = np.isfinite(values)

    if kind == "discrete":
        levels = spec.get("levels")
        colors = spec.get("colors")
        if not levels or not colors:
            raise ValueError(f"Discrete spec for {var_key} must include levels and colors")
        if len(colors) not in {len(levels), len(levels) - 1}:
            raise ValueError(
                f"Discrete spec for {var_key} must have colors length == levels or levels-1 "
                f"(got colors={len(colors)} levels={len(levels)})"
            )

        bins = np.digitize(np.where(finite_mask, values, levels[0]), levels, right=False) - 1
        bins = np.clip(bins, 0, len(colors) - 1).astype(np.uint8)
        alpha = np.where(finite_mask & (values >= levels[0]), 255, 0).astype(np.uint8)
        byte_band = np.where(alpha == 255, bins, 0).astype(np.uint8)

        meta = {
            "var_key": var_key,
            "kind": "discrete",
            "units": spec.get("units"),
            "levels": list(levels),
            "colors": list(colors),
        }
        # Add optional display metadata if present
        if "display_name" in spec:
            meta["display_name"] = spec["display_name"]
        if "legend_title" in spec:
            meta["legend_title"] = spec["legend_title"]
        if "ptype_order" in spec:
            meta["ptype_order"] = list(spec["ptype_order"])
        if "ptype_breaks" in spec:
            meta["ptype_breaks"] = dict(spec["ptype_breaks"])
        if "ptype_levels" in spec:
            meta["ptype_levels"] = {
                str(key): list(values) for key, values in dict(spec["ptype_levels"]).items()
            }
        if "range" in spec:
            range_vals = spec.get("range")
            if isinstance(range_vals, (list, tuple)) and len(range_vals) == 2:
                meta["range"] = [float(range_vals[0]), float(range_vals[1])]
        if "bins_per_ptype" in spec:
            meta["bins_per_ptype"] = int(spec["bins_per_ptype"])
        return byte_band, alpha, meta

    range_vals = spec.get("range")
    if not range_vals or len(range_vals) != 2:
        raise ValueError(f"Continuous spec for {var_key} must include range (min,max)")
    range_min, range_max = range_vals
    if range_max == range_min:
        raise ValueError(f"Continuous spec for {var_key} has invalid range: {range_vals}")

    scale = (values - range_min) / (range_max - range_min)
    scaled = np.clip(np.rint(scale * 255.0), 0, 255).astype(np.uint8)
    alpha = np.where(finite_mask, 255, 0).astype(np.uint8)
    byte_band = np.where(finite_mask, scaled, 0).astype(np.uint8)

    meta = {
        "var_key": var_key,
        "kind": "continuous",
        "units": spec.get("units"),
        "range": [float(range_min), float(range_max)],
        "colors": list(spec.get("colors", [])),
    }
    # Add optional display metadata if present
    if "display_name" in spec:
        meta["display_name"] = spec["display_name"]
    if "legend_title" in spec:
        meta["legend_title"] = spec["legend_title"]
    if "legend_stops" in spec:
        meta["legend_stops"] = [list(item) for item in spec["legend_stops"]]
    return byte_band, alpha, meta

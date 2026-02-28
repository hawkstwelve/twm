"""GFS model plugin — V3 clean implementation.

Provides VarSpec definitions, region specs, and forecast-hour schedule
for the Global Forecast System (GFS) model.

V3 design: this module is import-safe with zero external service dependencies.
All runtime logic (xarray selection, cfgrib parsing, cycle management) that
lived here in V2 has been removed — the V3 builder never calls those paths.
Selection happens via Herbie search patterns (VarSpec.selectors.search).
Derivation dispatch happens in builder/derive.py (Phase 2).
"""

from __future__ import annotations

from .base import (
    BaseModelPlugin,
    ModelCapabilities,
    RegionSpec,
    VarSelectors,
    VarSpec,
    VariableCapability,
)


class GFSPlugin(BaseModelPlugin):
    """V3-clean GFS plugin.

    Inherits get_var() / get_region() from BaseModelPlugin.
    Only overrides target_fhs() and normalize_var_id() — both
    are dependency-free.
    """

    def target_fhs(self, cycle_hour: int) -> list[int]:
        """GFS forecast hours to build.

        GFS runs 4×/day at 00/06/12/18z.  Initial V3 rollout builds
        fh000–fh120 in 6-hour steps (same behaviour for all cycles).
        Extend to fh384 in Phase 3 once CONUS is validated.
        """
        del cycle_hour  # all GFS cycles use the same FH set for now
        return list(GFS_INITIAL_FHS)

    def normalize_var_id(self, var_id: str) -> str:
        """Normalise common GFS variable aliases to canonical V3 IDs."""
        normalized = var_id.strip().lower()
        _aliases: dict[str, str] = {
            "t2m": "tmp2m",
            "2t": "tmp2m",
            "tmp2m": "tmp2m",
            "dp2m": "dp2m",
            "d2m": "dp2m",
            "2d": "dp2m",
            "dpt2m": "dp2m",
            "dewpoint2m": "dp2m",
            "dewpoint": "dp2m",
            "tmp850": "tmp850",
            "t850": "tmp850",
            "t850mb": "tmp850",
            "temp850": "tmp850",
            "temp850mb": "tmp850",
            "refc": "refc",
            "cref": "refc",
            "wspd10m": "wspd10m",
            "wgst10m": "wgst10m",
            "gust": "wgst10m",
            "gust10m": "wgst10m",
            "10m_gust": "wgst10m",
            "wind_gust": "wgst10m",
            "10u": "10u",
            "u10": "10u",
            "10v": "10v",
            "v10": "10v",
            "precip_total": "precip_total",
            "total_precip": "precip_total",
            "apcp": "precip_total",
            "qpf": "precip_total",
            "total_qpf": "precip_total",
            "qpf6h": "qpf6h",
            "snowfall_total": "snowfall_total",
            "asnow": "snowfall_total",
            "snow10": "snowfall_total",
            "total_snow": "snowfall_total",
            "totalsnow": "snowfall_total",
            "precip_ptype": "precip_ptype",
            "crain": "crain",
            "csnow": "csnow",
            "cicep": "cicep",
            "cfrzr": "cfrzr",
        }
        return _aliases.get(normalized, normalized)


# ---------------------------------------------------------------------------
# Region definitions
# ---------------------------------------------------------------------------

GFS_REGIONS: dict[str, RegionSpec] = {
    "pnw": RegionSpec(
        id="pnw",
        name="Pacific Northwest",
        bbox_wgs84=(-125.5, 41.5, -111.0, 49.5),
        clip=True,
    ),
    "conus": RegionSpec(
        id="conus",
        name="CONUS",
        bbox_wgs84=(-125.0, 24.0, -66.5, 50.0),
        clip=True,
    ),
}

# ---------------------------------------------------------------------------
# Forecast-hour schedule
# ---------------------------------------------------------------------------

# Phase 3: extend to range(0, 385, 6) (fh384)
GFS_INITIAL_FHS: tuple[int, ...] = tuple(range(0, 121, 6))  # fh000–fh120

# Initial rollout: PNW only.  CONUS added in Phase 3 after scale validation.
GFS_INITIAL_ROLLOUT_REGIONS: tuple[str, ...] = ("pnw",)

# ---------------------------------------------------------------------------
# Variable definitions
# ---------------------------------------------------------------------------

GFS_VARS: dict[str, VarSpec] = {
    # ── Simple variables (Phase 1+) ─────────────────────────────────────────
    "tmp2m": VarSpec(
        id="tmp2m",
        name="2m Temp",
        selectors=VarSelectors(
            search=[":TMP:2 m above ground:"],
            filter_by_keys={
                "typeOfLevel": "heightAboveGround",
                "level": "2",
            },
            hints={
                "upstream_var": "t2m",
                "cf_var": "t2m",
                "short_name": "2t",
            },
        ),
        primary=True,
        kind="continuous",
        units="F",
    ),
    "dp2m": VarSpec(
        id="dp2m",
        name="Surface Dew Point",
        selectors=VarSelectors(
            search=[":DPT:2 m above ground:"],
            filter_by_keys={
                "typeOfLevel": "heightAboveGround",
                "level": "2",
            },
            hints={
                "upstream_var": "d2m",
                "cf_var": "d2m",
                "short_name": "2d",
            },
        ),
        primary=True,
        kind="continuous",
        units="F",
    ),
    "tmp850": VarSpec(
        id="tmp850",
        name="850mb Temp",
        selectors=VarSelectors(
            search=[":TMP:850 mb:"],
            filter_by_keys={
                "shortName": "t",
                "typeOfLevel": "isobaricInhPa",
                "level": "850",
            },
            hints={
                "upstream_var": "t850",
                "cf_var": "t",
                "short_name": "t",
            },
        ),
        primary=True,
        kind="continuous",
        units="C",
    ),
    # ── Wind components (fetched separately for wspd10m derivation) ─────────
    "10u": VarSpec(
        id="10u",
        name="10m U Wind",
        selectors=VarSelectors(
            search=[":UGRD:10 m above ground:"],
            filter_by_keys={
                "typeOfLevel": "heightAboveGround",
                "level": "10",
            },
            hints={
                "upstream_var": "10u",
                "cf_var": "u10",
                "short_name": "10u",
            },
        ),
    ),
    "10v": VarSpec(
        id="10v",
        name="10m V Wind",
        selectors=VarSelectors(
            search=[":VGRD:10 m above ground:"],
            filter_by_keys={
                "typeOfLevel": "heightAboveGround",
                "level": "10",
            },
            hints={
                "upstream_var": "10v",
                "cf_var": "v10",
                "short_name": "10v",
            },
        ),
    ),
    # ── Derived: wind speed (Phase 2) ────────────────────────────────────────
    "wspd10m": VarSpec(
        id="wspd10m",
        name="10m Wind Speed",
        selectors=VarSelectors(
            hints={
                "u_component": "10u",
                "v_component": "10v",
            }
        ),
        derived=True,
        derive="wspd10m",
        kind="continuous",
        units="mph",
    ),
    "wgst10m": VarSpec(
        id="wgst10m",
        name="10m Wind Gust",
        selectors=VarSelectors(
            search=[":GUST:surface:"],
            filter_by_keys={
                "typeOfLevel": "surface",
                "shortName": "gust",
            },
            hints={
                "upstream_var": "gust",
                "cf_var": "gust",
                "short_name": "gust",
            },
        ),
        primary=True,
        kind="continuous",
        units="mph",
    ),
    "refc": VarSpec(
        id="refc",
        name="Composite Reflectivity",
        selectors=VarSelectors(
            search=[":REFC:"],
            filter_by_keys={
                "shortName": "refc",
            },
            hints={
                "upstream_var": "refc",
                "cf_var": "refc",
                "short_name": "refc",
            },
        ),
    ),
    "prate": VarSpec(
        id="prate",
        name="Precipitation Rate",
        selectors=VarSelectors(
            search=[":PRATE:surface:"],
            filter_by_keys={
                "typeOfLevel": "surface",
                "shortName": "prate",
            },
            hints={
                "upstream_var": "prate",
                "cf_var": "prate",
                "short_name": "prate",
            },
        ),
    ),
    # ── Derived: precip type (Phase 2) ──────────────────────────────────────
    "precip_ptype": VarSpec(
        id="precip_ptype",
        name="Precipitation Intensity + Type",
        selectors=VarSelectors(
            hints={
                "display_kind": "precip_ptype",
                "prate_component": "prate",
                "rain_component": "crain",
                "snow_component": "csnow",
                "sleet_component": "cicep",
                "frzr_component": "cfrzr",
            },
        ),
        primary=True,
        derived=True,
        derive="precip_ptype_blend",
        kind="discrete",
        units="in/hr",
        normalize_units="in/hr",
    ),
    "crain": VarSpec(
        id="crain",
        name="Categorical Rain",
        selectors=VarSelectors(
            search=[":CRAIN:surface:"],
            filter_by_keys={
                "typeOfLevel": "surface",
                "shortName": "crain",
            },
            hints={
                "upstream_var": "crain",
                "short_name": "crain",
            },
        ),
    ),
    "csnow": VarSpec(
        id="csnow",
        name="Categorical Snow",
        selectors=VarSelectors(
            search=[":CSNOW:surface:"],
            filter_by_keys={
                "typeOfLevel": "surface",
                "shortName": "csnow",
            },
            hints={
                "upstream_var": "csnow",
                "short_name": "csnow",
            },
        ),
    ),
    "cicep": VarSpec(
        id="cicep",
        name="Categorical Sleet",
        selectors=VarSelectors(
            search=[":CICEP:surface:"],
            filter_by_keys={
                "typeOfLevel": "surface",
                "shortName": "cicep",
            },
            hints={
                "upstream_var": "cicep",
                "short_name": "cicep",
            },
        ),
    ),
    "cfrzr": VarSpec(
        id="cfrzr",
        name="Categorical Freezing Rain",
        selectors=VarSelectors(
            search=[":CFRZR:surface:"],
            filter_by_keys={
                "typeOfLevel": "surface",
                "shortName": "cfrzr",
            },
            hints={
                "upstream_var": "cfrzr",
                "short_name": "cfrzr",
            },
        ),
    ),
    # ── QPF (Phase 3 candidate) ──────────────────────────────────────────────
    "apcp_step": VarSpec(
        id="apcp_step",
        name="APCP Step",
        selectors=VarSelectors(
            search=[":APCP:surface:"],
            filter_by_keys={
                "shortName": "apcp",
                "typeOfLevel": "surface",
            },
            hints={
                "upstream_var": "apcp",
            },
        ),
    ),
    "precip_total": VarSpec(
        id="precip_total",
        name="Total Precip",
        selectors=VarSelectors(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "6",
            },
        ),
        derived=True,
        derive="precip_total_cumulative",
        kind="continuous",
        units="in",
    ),
    "snowfall_total": VarSpec(
        id="snowfall_total",
        name="Total Snowfall (10:1)",
        selectors=VarSelectors(
            search=[
                ":SNOD:surface:",
                ":ASNOW:surface:",
            ],
            filter_by_keys={
                "shortName": "snod",
                "typeOfLevel": "surface",
            },
            hints={
                "upstream_var": "snod",
                "cf_var": "snod",
                "short_name": "snod",
            },
        ),
        primary=True,
        kind="continuous",
        units="in",
    ),
    "qpf6h": VarSpec(
        id="qpf6h",
        name="6-hr Precip",
        selectors=VarSelectors(
            search=[":APCP:surface:"],
            hints={
                "kind": "apcp_rolling_6h",
                "apcp_window_hours": "6",
            }
        ),
        kind="continuous",
        units="in",
    ),
}

GFS_COLOR_MAP_BY_VAR_KEY: dict[str, str] = {
    "tmp2m": "tmp2m",
    "dp2m": "dp2m",
    "tmp850": "tmp850",
    "wspd10m": "wspd10m",
    "wgst10m": "wgst10m",
    "refc": "refc",
    "precip_ptype": "precip_ptype",
    "precip_total": "precip_total",
    "snowfall_total": "snowfall_total",
    "qpf6h": "qpf6h",
}

GFS_DEFAULT_FH_BY_VAR_KEY: dict[str, int] = {
    "precip_ptype": 6,
    "precip_total": 6,
    "snowfall_total": 6,
    "qpf6h": 6,
}

GFS_ORDER_BY_VAR_KEY: dict[str, int] = {
    "tmp2m": 0,
    "dp2m": 1,
    "tmp850": 2,
    "refc": 3,
    "wspd10m": 4,
    "wgst10m": 5,
    "precip_ptype": 6,
    "precip_total": 7,
    "snowfall_total": 8,
    "qpf6h": 9,
}

GFS_CONVERSION_BY_VAR_KEY: dict[str, str] = {
    "tmp2m": "c_to_f",
    "dp2m": "c_to_f",
    "wspd10m": "ms_to_mph",
    "wgst10m": "ms_to_mph",
    "precip_total": "kgm2_to_in",
    "snowfall_total": "m_to_in",
    "qpf6h": "kgm2_to_in",
}

GFS_CONSTRAINTS_BY_VAR_KEY: dict[str, dict[str, int]] = {
    "precip_total": {
        "min_fh": 6,
    },
    "snowfall_total": {
        "min_fh": 6,
    },
    "qpf6h": {
        "min_fh": 6,
    },
}


def _capability_from_var_spec(var_key: str, var_spec: VarSpec) -> VariableCapability:
    is_buildable = bool(var_spec.primary or var_spec.derived)
    return VariableCapability(
        var_key=var_key,
        name=var_spec.name,
        selectors=var_spec.selectors,
        primary=var_spec.primary,
        derived=var_spec.derived,
        derive_strategy_id=var_spec.derive,
        kind=var_spec.kind,
        units=var_spec.units,
        normalize_units=var_spec.normalize_units,
        scale=var_spec.scale,
        color_map_id=GFS_COLOR_MAP_BY_VAR_KEY.get(var_key),
        default_fh=GFS_DEFAULT_FH_BY_VAR_KEY.get(var_key),
        buildable=is_buildable,
        order=GFS_ORDER_BY_VAR_KEY.get(var_key),
        conversion=GFS_CONVERSION_BY_VAR_KEY.get(var_key),
        constraints=dict(GFS_CONSTRAINTS_BY_VAR_KEY.get(var_key, {})),
    )


GFS_VARIABLE_CATALOG: dict[str, VariableCapability] = {
    var_key: _capability_from_var_spec(var_key, var_spec)
    for var_key, var_spec in GFS_VARS.items()
}

GFS_CAPABILITIES = ModelCapabilities(
    model_id="gfs",
    name="GFS",
    product="pgrb2.0p25",
    canonical_region="conus",
    grid_meters_by_region={
        "conus": 25_000.0,
        "pnw": 25_000.0,
    },
    run_discovery={
        "probe_var_key": "tmp2m",
        "probe_enabled": True,
        "probe_attempts": 4,
        "cycle_cadence_hours": 6,
        "fallback_lag_hours": 5,
    },
    ui_defaults={
        "default_var_key": "tmp2m",
        "default_run": "latest",
    },
    ui_constraints={
        "canonical_region": "conus",
        "zoom_hint_min": 7,
        "overlay_fade_out_zoom_start": 6,
        "overlay_fade_out_zoom_end": 7,
    },
    variable_catalog=GFS_VARIABLE_CATALOG,
)

# ---------------------------------------------------------------------------
# Plugin instance — imported by the model registry
# ---------------------------------------------------------------------------

GFS_MODEL = GFSPlugin(
    id="gfs",
    name="GFS",
    regions=GFS_REGIONS,
    vars=GFS_VARS,
    product="pgrb2.0p25",
    capabilities=GFS_CAPABILITIES,
)

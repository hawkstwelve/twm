"""NAM CONUS Nest model plugin.

Initial rollout scope:
  - tmp2m (2m temperature)
  - dp2m (2m dewpoint)
  - tmp850 (850mb temperature)
  - wspd10m (10m wind speed, derived from 10u/10v)
  - wgst10m (10m wind gust)
  - precip_total (total precipitation)
  - snowfall_total (10:1 total snowfall, cumulative)
  - radar_ptype (composite reflectivity + precipitation type)

Herbie wiring:
  - model = "nam"
  - product = "conusnest.hiresf"
"""

from __future__ import annotations

from pathlib import Path

from .base import (
    BaseModelPlugin,
    ModelCapabilities,
    RegionSpec,
    VarSelectors,
    VarSpec,
    VariableCapability,
)


class NAMPlugin(BaseModelPlugin):
    def target_fhs(self, cycle_hour: int) -> list[int]:
        del cycle_hour  # all NAM cycles share the same FH set for now
        return list(NAM_INITIAL_FHS)

    def normalize_var_id(self, var_id: str) -> str:
        normalized = var_id.strip().lower()
        aliases: dict[str, str] = {
            "tmp2m": "tmp2m",
            "t2m": "tmp2m",
            "2t": "tmp2m",
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
            "wgst10m": "wgst10m",
            "gust10m": "wgst10m",
            "10m_gust": "wgst10m",
            "gust": "wgst10m",
            "wind_gust": "wgst10m",
            "precip_total": "precip_total",
            "total_precip": "precip_total",
            "apcp": "precip_total",
            "qpf": "precip_total",
            "total_qpf": "precip_total",
            "snowfall_total": "snowfall_total",
            "asnow": "snowfall_total",
            "snow10": "snowfall_total",
            "snow_10to1": "snowfall_total",
            "total_snow": "snowfall_total",
            "totalsnow": "snowfall_total",
            "refc": "refc",
            "cref": "refc",
            "radar_ptype": "radar_ptype",
            "radarptype": "radar_ptype",
            "wspd10m": "wspd10m",
            "wind10m": "10si",
            "10si": "10si",
            "10u": "10u",
            "u10": "10u",
            "10v": "10v",
            "v10": "10v",
        }
        return aliases.get(normalized, normalized)

    def select_dataarray(self, ds: object, var_id: str) -> object:
        del ds, var_id
        raise NotImplementedError("select_dataarray is not used in the V3 builder path")

    def ensure_latest_cycles(self, keep_cycles: int, *, cache_dir: Path | None = None) -> dict[str, int]:
        del keep_cycles, cache_dir
        raise NotImplementedError("ensure_latest_cycles is not used in the V3 scheduler/builder path")


NAM_REGIONS: dict[str, RegionSpec] = {
    "conus": RegionSpec(
        id="conus",
        name="CONUS",
        bbox_wgs84=(-125.0, 24.0, -66.5, 50.0),
        clip=True,
    ),
    "pnw": RegionSpec(
        id="pnw",
        name="Pacific Northwest",
        bbox_wgs84=(-125.5, 41.5, -111.0, 49.5),
        clip=True,
    ),
}


# NAM CONUS Nest: hourly forecast hours through fh060.
NAM_INITIAL_FHS: tuple[int, ...] = tuple(range(0, 61))


NAM_VARS: dict[str, VarSpec] = {
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
    "10si": VarSpec(
        id="10si",
        name="10m Wind Speed (direct)",
        selectors=VarSelectors(
            search=[
                ":WIND:10 m above ground:",
                ":WIND:10 m above ground",
            ],
            filter_by_keys={
                "typeOfLevel": "heightAboveGround",
                "level": "10",
            },
            hints={
                "upstream_var": "10si",
                "cf_var": "si10",
                "short_name": "10si",
            },
        ),
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
                "cf_var": "apcp",
                "short_name": "apcp",
            },
        ),
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
    "crain": VarSpec(
        id="crain",
        name="Categorical Rain",
        selectors=VarSelectors(
            search=[":CRAIN:surface:"],
            filter_by_keys={
                "shortName": "crain",
                "typeOfLevel": "surface",
            },
            hints={
                "upstream_var": "crain",
                "cf_var": "crain",
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
                "shortName": "csnow",
                "typeOfLevel": "surface",
            },
            hints={
                "upstream_var": "csnow",
                "cf_var": "csnow",
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
                "shortName": "cicep",
                "typeOfLevel": "surface",
            },
            hints={
                "upstream_var": "cicep",
                "cf_var": "cicep",
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
                "shortName": "cfrzr",
                "typeOfLevel": "surface",
            },
            hints={
                "upstream_var": "cfrzr",
                "cf_var": "cfrzr",
                "short_name": "cfrzr",
            },
        ),
    ),
    "precip_total": VarSpec(
        id="precip_total",
        name="Total Precip",
        selectors=VarSelectors(
            hints={
                "apcp_component": "apcp_step",
                "step_hours": "1",
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
            hints={
                "apcp_component": "apcp_step",
                "snow_component": "csnow",
                "step_hours": "1",
                "slr": "10",
                "snow_mask_threshold": "0.5",
                "min_step_lwe_kgm2": "0.01",
            },
        ),
        derived=True,
        derive="snowfall_total_10to1_cumulative",
        kind="continuous",
        units="in",
    ),
    "wspd10m": VarSpec(
        id="wspd10m",
        name="10m Wind Speed",
        selectors=VarSelectors(
            hints={
                "u_component": "10u",
                "v_component": "10v",
                "speed_component": "10si",
            },
        ),
        derived=True,
        derive="wspd10m",
        kind="continuous",
        units="mph",
    ),
    "radar_ptype": VarSpec(
        id="radar_ptype",
        name="Composite Reflectivity + Ptype",
        selectors=VarSelectors(
            hints={
                "display_kind": "radar_ptype",
                "refl_component": "refc",
                "rain_component": "crain",
                "snow_component": "csnow",
                "sleet_component": "cicep",
                "frzr_component": "cfrzr",
            },
        ),
        derived=True,
        derive="radar_ptype_combo",
        kind="discrete",
        units="dBZ",
    ),
}


NAM_COLOR_MAP_BY_VAR_KEY: dict[str, str] = {
    "tmp2m": "tmp2m",
    "dp2m": "dp2m",
    "tmp850": "tmp850",
    "wspd10m": "wspd10m",
    "wgst10m": "wgst10m",
    "precip_total": "precip_total",
    "snowfall_total": "snowfall_total",
    "radar_ptype": "radar_ptype",
}

NAM_DEFAULT_FH_BY_VAR_KEY: dict[str, int] = {
    "radar_ptype": 1,
    "precip_total": 1,
    "snowfall_total": 1,
}

NAM_ORDER_BY_VAR_KEY: dict[str, int] = {
    "tmp2m": 0,
    "dp2m": 1,
    "tmp850": 2,
    "wspd10m": 3,
    "wgst10m": 4,
    "precip_total": 5,
    "snowfall_total": 6,
    "radar_ptype": 7,
}

NAM_CONVERSION_BY_VAR_KEY: dict[str, str] = {
    "tmp2m": "c_to_f",
    "dp2m": "c_to_f",
    "wspd10m": "ms_to_mph",
    "wgst10m": "ms_to_mph",
    "precip_total": "kgm2_to_in",
}

NAM_CONSTRAINTS_BY_VAR_KEY: dict[str, dict[str, int]] = {
    "precip_total": {
        "min_fh": 1,
    },
    "snowfall_total": {
        "min_fh": 1,
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
        color_map_id=NAM_COLOR_MAP_BY_VAR_KEY.get(var_key),
        default_fh=NAM_DEFAULT_FH_BY_VAR_KEY.get(var_key),
        buildable=is_buildable,
        order=NAM_ORDER_BY_VAR_KEY.get(var_key),
        conversion=NAM_CONVERSION_BY_VAR_KEY.get(var_key),
        constraints=dict(NAM_CONSTRAINTS_BY_VAR_KEY.get(var_key, {})),
    )


NAM_VARIABLE_CATALOG: dict[str, VariableCapability] = {
    var_key: _capability_from_var_spec(var_key, var_spec)
    for var_key, var_spec in NAM_VARS.items()
}


NAM_CAPABILITIES = ModelCapabilities(
    model_id="nam",
    name="NAM",
    product="conusnest.hiresf",
    canonical_region="conus",
    grid_meters_by_region={
        "conus": 5_000.0,
        "pnw": 5_000.0,
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
    },
    variable_catalog=NAM_VARIABLE_CATALOG,
)


NAM_MODEL = NAMPlugin(
    id="nam",
    name="NAM",
    regions=NAM_REGIONS,
    vars=NAM_VARS,
    product="conusnest.hiresf",
    capabilities=NAM_CAPABILITIES,
)

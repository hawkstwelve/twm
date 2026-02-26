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


class HRRRPlugin(BaseModelPlugin):
    def target_fhs(self, cycle_hour: int) -> list[int]:
        if cycle_hour in {0, 6, 12, 18}:
            return list(range(0, 49))
        return list(range(0, 19))

    def normalize_var_id(self, var_id: str) -> str:
        normalized = var_id.strip().lower()
        if normalized in {"t2m", "tmp2m", "2t"}:
            return "tmp2m"
        if normalized in {"dp2m", "d2m", "2d", "dpt2m", "dewpoint2m", "dewpoint"}:
            return "dp2m"
        if normalized in {"precip_total", "total_precip", "apcp", "qpf", "total_qpf"}:
            return "precip_total"
        if normalized in {"snowfall_total", "asnow", "snow10", "snow_10to1", "total_snow", "totalsnow"}:
            return "snowfall_total"
        if normalized in {"tmp850", "t850", "t850mb", "temp850", "temp850mb"}:
            return "tmp850"
        if normalized in {"10u", "u10"}:
            return "10u"
        if normalized in {"10v", "v10"}:
            return "10v"
        if normalized in {"refc", "cref"}:
            return "refc"
        if normalized == "wspd10m":
            return "wspd10m"
        if normalized in {"wgst10m", "gust10m", "10m_gust", "gust", "wind_gust"}:
            return "wgst10m"
        if normalized in {"radar_ptype", "radarptype"}:
            return "radar_ptype"
        return normalized

    def select_dataarray(self, ds: object, var_id: str) -> object:
        del ds, var_id
        raise NotImplementedError("select_dataarray is not used in the V3 builder path")

    def ensure_latest_cycles(self, keep_cycles: int, *, cache_dir: Path | None = None) -> dict[str, int]:
        del keep_cycles, cache_dir
        raise NotImplementedError("ensure_latest_cycles is not used in the V3 scheduler/builder path")


PNW_BBOX_WGS84 = (-125.5, 41.5, -111.0, 49.5)
CONUS_BBOX_WGS84 = (-125.0, 24.0, -66.5, 50.0)

HRRR_REGIONS: dict[str, RegionSpec] = {
    "conus": RegionSpec(
        id="conus",
        name="CONUS",
        bbox_wgs84=CONUS_BBOX_WGS84,
        clip=True,
    ),
    "pnw": RegionSpec(
        id="pnw",
        name="Pacific Northwest",
        bbox_wgs84=PNW_BBOX_WGS84,
        clip=True,
    ),
}

HRRR_VARS: dict[str, VarSpec] = {
    "tmp2m": VarSpec(
        id="tmp2m",
        name="2m Temp",
        selectors=VarSelectors(
            search=[":TMP:2 m above ground:"],
            filter_by_keys={
                "shortName": "2t",
                "typeOfLevel": "heightAboveGround",
                "level": "2",
            },
            hints={
                "upstream_var": "t2m",
            },
        ),
        primary=True,
        kind="continuous",
        units="F",
    ),
    "dp2m": VarSpec(
        id="dp2m",
        name="2m Dew Point",
        selectors=VarSelectors(
            search=[":DPT:2 m above ground:"],
            filter_by_keys={
                "typeOfLevel": "heightAboveGround",
                "level": "2",
            },
            hints={
                "upstream_var": "d2m",
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
            },
        ),
        primary=True,
        kind="continuous",
        units="C",
    ),
    "snowfall_total": VarSpec(
        id="snowfall_total",
        name="Total Snowfall (10:1)",
        selectors=VarSelectors(
            search=[":ASNOW:surface:"],
            filter_by_keys={
                "shortName": "asnow",
                "typeOfLevel": "surface",
            },
            hints={
                "upstream_var": "asnow",
            },
        ),
        primary=True,
        kind="continuous",
        units="in",
    ),
    "precip_total": VarSpec(
        id="precip_total",
        name="Total Precipitation",
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
        primary=True,
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
                "shortName": "gust",
                "typeOfLevel": "surface",
            },
            hints={
                "upstream_var": "gust",
            },
        ),
        primary=True,
        kind="continuous",
        units="mph",
    ),
    "10u": VarSpec(
        id="10u",
        name="10m U Wind",
        selectors=VarSelectors(
            search=[":UGRD:10 m above ground:"],
            filter_by_keys={
                "shortName": "10u",
                "typeOfLevel": "heightAboveGround",
                "level": "10",
            },
            hints={
                "upstream_var": "10u",
            },
        ),
    ),
    "10v": VarSpec(
        id="10v",
        name="10m V Wind",
        selectors=VarSelectors(
            search=[":VGRD:10 m above ground:"],
            filter_by_keys={
                "shortName": "10v",
                "typeOfLevel": "heightAboveGround",
                "level": "10",
            },
            hints={
                "upstream_var": "10v",
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
            hints={"upstream_var": "crain"},
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
            hints={"upstream_var": "csnow"},
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
            hints={"upstream_var": "cicep"},
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
            hints={"upstream_var": "cfrzr"},
        ),
    ),
    "radar_ptype": VarSpec(
        id="radar_ptype",
        name="Composite Reflectivity + P-Type",
        selectors=VarSelectors(
            hints={
                "display_kind": "radar_ptype",
                "refl_component": "refc",
                "rain_component": "crain",
                "snow_component": "csnow",
                "sleet_component": "cicep",
                "frzr_component": "cfrzr",
            }
        ),
        derived=True,
        derive="radar_ptype_combo",
        kind="discrete",
        units="dBZ",
    ),
}

HRRR_COLOR_MAP_BY_VAR_KEY: dict[str, str] = {
    "tmp2m": "tmp2m",
    "dp2m": "dp2m",
    "tmp850": "tmp850",
    "snowfall_total": "snowfall_total",
    "precip_total": "precip_total",
    "wspd10m": "wspd10m",
    "wgst10m": "wgst10m",
    "refc": "refc",
    "radar_ptype": "radar_ptype",
}

HRRR_DEFAULT_FH_BY_VAR_KEY: dict[str, int] = {
    "radar_ptype": 1,
    "precip_total": 1,
    "snowfall_total": 1,
}

HRRR_ORDER_BY_VAR_KEY: dict[str, int] = {
    "radar_ptype": 0,
    "tmp2m": 1,
    "tmp850": 2,
    "dp2m": 3,
    "precip_total": 4,
    "snowfall_total": 5,
    "wspd10m": 6,
    "wgst10m": 7,
}

HRRR_CONVERSION_BY_VAR_KEY: dict[str, str] = {
    "tmp2m": "c_to_f",
    "dp2m": "c_to_f",
    "wspd10m": "ms_to_mph",
    "wgst10m": "ms_to_mph",
    "snowfall_total": "m_to_in",
    "precip_total": "kgm2_to_in",
}

HRRR_CONSTRAINTS_BY_VAR_KEY: dict[str, dict[str, int]] = {}


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
        color_map_id=HRRR_COLOR_MAP_BY_VAR_KEY.get(var_key),
        default_fh=HRRR_DEFAULT_FH_BY_VAR_KEY.get(var_key),
        buildable=is_buildable,
        order=HRRR_ORDER_BY_VAR_KEY.get(var_key),
        conversion=HRRR_CONVERSION_BY_VAR_KEY.get(var_key),
        constraints=dict(HRRR_CONSTRAINTS_BY_VAR_KEY.get(var_key, {})),
    )


HRRR_VARIABLE_CATALOG: dict[str, VariableCapability] = {
    var_key: _capability_from_var_spec(var_key, var_spec)
    for var_key, var_spec in HRRR_VARS.items()
}

HRRR_CAPABILITIES = ModelCapabilities(
    model_id="hrrr",
    name="HRRR",
    product="sfc",
    canonical_region="conus",
    grid_meters_by_region={
        "conus": 3_000.0,
        "pnw": 3_000.0,
    },
    run_discovery={
        "probe_var_key": "tmp2m",
        "probe_enabled": True,
        "cycle_cadence_hours": 1,
        "probe_attempts": 4,
        "fallback_lag_hours": 2,
    },
    ui_defaults={
        "default_var_key": "tmp2m",
        "default_run": "latest",
    },
    ui_constraints={
        "canonical_region": "conus",
    },
    variable_catalog=HRRR_VARIABLE_CATALOG,
)


HRRR_MODEL = HRRRPlugin(
    id="hrrr",
    name="HRRR",
    regions=HRRR_REGIONS,
    vars=HRRR_VARS,
    product="sfc",
    capabilities=HRRR_CAPABILITIES,
)

from __future__ import annotations

from pathlib import Path

from .base import BaseModelPlugin, RegionSpec, VarSelectors, VarSpec


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


HRRR_MODEL = HRRRPlugin(
    id="hrrr",
    name="HRRR",
    regions=HRRR_REGIONS,
    vars=HRRR_VARS,
    product="sfc",
)

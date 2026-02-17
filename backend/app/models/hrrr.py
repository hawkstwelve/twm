from __future__ import annotations

from pathlib import Path

from app.services.hrrr_fetch import ensure_latest_cycles
from app.services.hrrr_runs import HRRRCacheConfig
from app.services.variable_registry import normalize_api_variable, select_dataarray

from .base import BaseModelPlugin, RegionSpec, VarSelectors, VarSpec


class HRRRPlugin(BaseModelPlugin):
    def target_fhs(self, cycle_hour: int) -> list[int]:
        if cycle_hour in {0, 6, 12, 18}:
            return list(range(0, 49))
        return list(range(0, 19))

    def normalize_var_id(self, var_id: str) -> str:
        normalized = normalize_api_variable(var_id)
        if normalized in {"t2m", "tmp2m", "2t"}:
            return "tmp2m"
        if normalized in {"10u", "u10"}:
            return "10u"
        if normalized in {"10v", "v10"}:
            return "10v"
        if normalized in {"refc", "cref"}:
            return "refc"
        if normalized == "wspd10m":
            return "wspd10m"
        if normalized == "radar_ptype":
            return "radar_ptype"
        return normalized

    def select_dataarray(self, ds: object, var_id: str) -> object:
        return select_dataarray(ds, var_id)

    def ensure_latest_cycles(self, keep_cycles: int, *, cache_dir: Path | None = None) -> dict[str, int]:
        if cache_dir is None:
            return ensure_latest_cycles(keep_cycles=keep_cycles)
        cache_cfg = HRRRCacheConfig(base_dir=cache_dir, keep_runs=keep_cycles)
        return ensure_latest_cycles(keep_cycles=keep_cycles, cache_cfg=cache_cfg)


PNW_BBOX_WGS84 = (-125.5, 41.5, -111.0, 49.5)

HRRR_REGIONS: dict[str, RegionSpec] = {
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
                "refl_component": "refc",
                "rain_component": "crain",
                "snow_component": "csnow",
                "sleet_component": "cicep",
                "frzr_component": "cfrzr",
            }
        ),
        derived=True,
        derive="radar_ptype_combo",
    ),
}


HRRR_MODEL = HRRRPlugin(
    id="hrrr",
    name="HRRR",
    regions=HRRR_REGIONS,
    vars=HRRR_VARS,
    product="sfc",
)

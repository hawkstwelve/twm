"""NBM model plugin.

Initial rollout scope:
  - tmp2m (2m temperature)
  - wspd10m (10m wind speed)

Herbie wiring:
  - model = "nbm"
  - product = "co"
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


class NBMPlugin(BaseModelPlugin):
    def target_fhs(self, cycle_hour: int) -> list[int]:
        del cycle_hour  # all NBM cycles share the same initial FH set for now
        return list(NBM_INITIAL_FHS)

    def normalize_var_id(self, var_id: str) -> str:
        normalized = var_id.strip().lower()
        aliases: dict[str, str] = {
            "tmp2m": "tmp2m",
            "t2m": "tmp2m",
            "2t": "tmp2m",
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


NBM_REGIONS: dict[str, RegionSpec] = {
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


# Initial NBM rollout: 6-hourly forecast hours through fh120.
NBM_INITIAL_FHS: tuple[int, ...] = tuple(range(0, 121, 6))


NBM_VARS: dict[str, VarSpec] = {
    "tmp2m": VarSpec(
        id="tmp2m",
        name="Surface Temp",
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
}


NBM_COLOR_MAP_BY_VAR_KEY: dict[str, str] = {
    "tmp2m": "tmp2m",
    "wspd10m": "wspd10m",
}

NBM_ORDER_BY_VAR_KEY: dict[str, int] = {
    "tmp2m": 1,
    "wspd10m": 2,
}

NBM_CONVERSION_BY_VAR_KEY: dict[str, str] = {
    "tmp2m": "c_to_f",
    "wspd10m": "ms_to_mph",
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
        color_map_id=NBM_COLOR_MAP_BY_VAR_KEY.get(var_key),
        buildable=is_buildable,
        order=NBM_ORDER_BY_VAR_KEY.get(var_key),
        conversion=NBM_CONVERSION_BY_VAR_KEY.get(var_key),
        constraints={},
    )


NBM_VARIABLE_CATALOG: dict[str, VariableCapability] = {
    var_key: _capability_from_var_spec(var_key, var_spec)
    for var_key, var_spec in NBM_VARS.items()
}


NBM_CAPABILITIES = ModelCapabilities(
    model_id="nbm",
    name="NBM",
    product="co",
    canonical_region="conus",
    grid_meters_by_region={
        "conus": 13_000.0,
        "pnw": 13_000.0,
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
    variable_catalog=NBM_VARIABLE_CATALOG,
)


NBM_MODEL = NBMPlugin(
    id="nbm",
    name="NBM",
    regions=NBM_REGIONS,
    vars=NBM_VARS,
    product="co",
    capabilities=NBM_CAPABILITIES,
)

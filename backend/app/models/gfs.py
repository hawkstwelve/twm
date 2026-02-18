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

from .base import BaseModelPlugin, RegionSpec, VarSelectors, VarSpec


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
        _aliases: dict[str, str] = {
            "t2m": "tmp2m",
            "2t": "tmp2m",
            "tmp2m": "tmp2m",
            "refc": "refc",
            "cref": "refc",
            "wspd10m": "wspd10m",
            "10u": "10u",
            "u10": "10u",
            "10v": "10v",
            "v10": "10v",
            "qpf6h": "qpf6h",
            "precip_ptype": "precip_ptype",
            "crain": "crain",
            "csnow": "csnow",
            "cicep": "cicep",
            "cfrzr": "cfrzr",
        }
        return _aliases.get(var_id, var_id)


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
        units="mm/hr",
        normalize_units="mm/hr",
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
        primary=True,
    ),
}

# ---------------------------------------------------------------------------
# Plugin instance — imported by the model registry
# ---------------------------------------------------------------------------

GFS_MODEL = GFSPlugin(
    id="gfs",
    name="GFS",
    regions=GFS_REGIONS,
    vars=GFS_VARS,
    product="pgrb2.0p25",
)
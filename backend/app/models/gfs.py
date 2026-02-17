from __future__ import annotations

from pathlib import Path

import numpy as np
import xarray as xr

from app.services.gfs_runs import GFSCacheConfig, enforce_cycle_retention
from app.services.variable_registry import normalize_api_variable

from .base import BaseModelPlugin, RegionSpec, VarSelectors, VarSpec


class GFSPlugin(BaseModelPlugin):
    _PRATE_TO_MM_PER_HOUR = 3600.0
    _STRICT_SELECTION_VARS = {
        "tmp2m",
        "10u",
        "10v",
        "refc",
        "precip_ptype",
        "crain",
        "csnow",
        "cicep",
        "cfrzr",
    }

    def target_fhs(self, cycle_hour: int) -> list[int]:
        del cycle_hour
        return list(GFS_INITIAL_FHS)

    @classmethod
    def _prate_mm_per_s_to_mm_per_hr(cls, values: np.ndarray) -> np.ndarray:
        return values * cls._PRATE_TO_MM_PER_HOUR

    def normalize_var_id(self, var_id: str) -> str:
        normalized = normalize_api_variable(var_id)
        if normalized in {"t2m", "tmp2m", "2t"}:
            return "tmp2m"
        if normalized in {"refc", "cref"}:
            return "refc"
        if normalized == "wspd10m":
            return "wspd10m"
        if normalized == "10u":
            return "10u"
        if normalized == "10v":
            return "10v"
        if normalized == "qpf6h":
            return "qpf6h"
        return normalized

    def _score_candidate(self, da: xr.DataArray, var_spec: VarSpec) -> int:
        selectors = var_spec.selectors
        hints = selectors.hints
        filter_keys = selectors.filter_by_keys

        score = 0

        expected_cf = hints.get("cf_var")
        if expected_cf and da.attrs.get("GRIB_cfVarName") == expected_cf:
            score += 10

        expected_short = hints.get("short_name")
        if expected_short and da.attrs.get("GRIB_shortName") == expected_short:
            score += 8

        expected_tol = filter_keys.get("typeOfLevel")
        if expected_tol and da.attrs.get("GRIB_typeOfLevel") == expected_tol:
            score += 4

        expected_level = filter_keys.get("level")
        if expected_level is not None:
            actual_level = da.attrs.get("GRIB_level")
            try:
                expected_level_i = int(expected_level)
                actual_level_i = int(actual_level) if actual_level is not None else None
            except (TypeError, ValueError):
                expected_level_i = None
                actual_level_i = None
            if expected_level_i is not None and actual_level_i == expected_level_i:
                score += 2

        expected_upstream = hints.get("upstream_var")
        if expected_upstream:
            if da.attrs.get("GRIB_cfVarName") == expected_upstream:
                score += 8
            elif da.name == expected_upstream:
                score += 6

        return score

    def _select_from_spec(self, ds: xr.Dataset, var_id: str) -> xr.DataArray:
        var_spec = self.get_var(var_id)
        if var_spec is None:
            raise ValueError(f"Unknown GFS variable: {var_id}")

        scored: list[tuple[int, str, xr.DataArray]] = []
        for name in sorted(ds.data_vars):
            da = ds[name]
            score = self._score_candidate(da, var_spec)
            if score > 0:
                scored.append((score, name, da))

        scored.sort(key=lambda item: item[0], reverse=True)

        if scored:
            top_score = scored[0][0]
            top = [row for row in scored if row[0] == top_score]
            if len(top) == 1:
                return top[0][2]

        if var_id in ds.data_vars:
            return ds[var_id]

        upstream = var_spec.selectors.hints.get("upstream_var")
        if upstream and upstream in ds.data_vars:
            return ds[upstream]

        if var_id in self._STRICT_SELECTION_VARS:
            available = ", ".join(sorted(ds.data_vars))
            raise ValueError(
                f"GFS strict selection failed for {var_id}; available={available}"
            )

        if len(ds.data_vars) == 1:
            only_name = next(iter(ds.data_vars))
            return ds[only_name]

        available = ", ".join(sorted(ds.data_vars))
        raise ValueError(
            f"GFS variable selection failed for {var_id}; available={available}"
        )

    def select_dataarray(self, ds: object, var_id: str) -> object:
        if not isinstance(ds, xr.Dataset):
            raise TypeError("Expected xarray.Dataset for GFS selection")

        normalized = self.normalize_var_id(var_id)
        if normalized == "precip_ptype":
            prate_da = self._select_from_spec(ds, "precip_ptype")
            source_units = str(
                prate_da.attrs.get("GRIB_units")
                or prate_da.attrs.get("units")
                or ""
            ).strip().lower()
            convert = "mm/hr" not in source_units and "mm h-1" not in source_units

            values = np.asarray(prate_da.values, dtype=np.float32)
            if convert:
                values = self._prate_mm_per_s_to_mm_per_hr(values)

            coords = {
                dim: prate_da.coords[dim]
                for dim in prate_da.dims
                if dim in prate_da.coords
            }
            precip_da = xr.DataArray(
                values.astype(np.float32),
                dims=prate_da.dims,
                coords=coords,
                name="precip_ptype",
            )
            precip_da.attrs = dict(prate_da.attrs)
            precip_da.attrs["GRIB_units"] = "mm/hr"
            precip_da.attrs["units"] = "mm/hr"
            return precip_da
        if normalized == "wspd10m":
            u_da = self._select_from_spec(ds, "10u")
            v_da = self._select_from_spec(ds, "10v")
            if "time" in u_da.dims:
                u_da = u_da.isel(time=0)
            if "time" in v_da.dims:
                v_da = v_da.isel(time=0)
            u_da = u_da.squeeze()
            v_da = v_da.squeeze()
            if u_da.shape != v_da.shape:
                raise ValueError(
                    f"wspd10m component shape mismatch: u_shape={u_da.shape} v_shape={v_da.shape}"
                )

            u_vals = np.asarray(u_da.values, dtype=np.float32)
            v_vals = np.asarray(v_da.values, dtype=np.float32)
            speed_vals = np.hypot(u_vals, v_vals) * 2.23694
            coords = {dim: u_da.coords[dim] for dim in u_da.dims if dim in u_da.coords}
            speed_mph = xr.DataArray(
                speed_vals.astype(np.float32),
                dims=u_da.dims,
                coords=coords,
                name="wspd10m",
            )
            speed_mph.name = "wspd10m"
            speed_mph.attrs = dict(u_da.attrs)
            speed_mph.attrs["GRIB_units"] = "mph"
            return speed_mph
        return self._select_from_spec(ds, normalized)

    def ensure_latest_cycles(self, keep_cycles: int, *, cache_dir: Path | None = None) -> dict[str, int]:
        cfg = GFSCacheConfig(base_dir=cache_dir or GFSCacheConfig().base_dir, keep_runs=keep_cycles)
        return enforce_cycle_retention(cfg)


PNW_BBOX_WGS84 = (-125.5, 41.5, -111.0, 49.5)

# Initial rollout scope (M0): keep CONUS configured, but build/schedule only PNW.
GFS_INITIAL_ROLLOUT_REGIONS: tuple[str, ...] = ("pnw",)
GFS_INITIAL_FHS: tuple[int, ...] = tuple(range(0, 121, 6))

GFS_REGIONS: dict[str, RegionSpec] = {
    "pnw": RegionSpec(
        id="pnw",
        name="Pacific Northwest",
        bbox_wgs84=PNW_BBOX_WGS84,
        clip=True,
    ),
    "conus": RegionSpec(
        id="conus",
        name="CONUS",
        bbox_wgs84=None,
        clip=False,
    ),
}

GFS_VARS: dict[str, VarSpec] = {
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
    "precip_ptype": VarSpec(
        id="precip_ptype",
        name="Precipitation Intensity + Type",
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
                "kind": "precip_ptype",
                "units": "mm/hr",
                "prate_component": "precip_ptype",
                "rain_component": "crain",
                "snow_component": "csnow",
                "sleet_component": "cicep",
                "frzr_component": "cfrzr",
            },
        ),
        primary=True,
        derived=True,
        derive="precip_ptype_blend",
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


GFS_MODEL = GFSPlugin(
    id="gfs",
    name="GFS",
    regions=GFS_REGIONS,
    vars=GFS_VARS,
    product="pgrb2.0p25",
)

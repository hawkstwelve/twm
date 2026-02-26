from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Protocol, Sequence


@dataclass(frozen=True)
class RegionSpec:
    id: str
    name: str
    bbox_wgs84: Optional[tuple[float, float, float, float]] = None
    tile_matrix: Optional[str] = None
    clip: bool = False


@dataclass(frozen=True)
class VarSelectors:
    search: list[str] = field(default_factory=list)
    filter_by_keys: dict[str, str] = field(default_factory=dict)
    hints: dict[str, str] = field(default_factory=dict)


SelectorInput = VarSelectors | Mapping[str, str] | Sequence[str] | None


def normalize_selectors(value: SelectorInput) -> VarSelectors:
    if isinstance(value, VarSelectors):
        return value
    if value is None:
        return VarSelectors()
    if isinstance(value, Mapping):
        return VarSelectors(filter_by_keys=dict(value))
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return VarSelectors(search=list(value))
    raise TypeError(f"Unsupported selector type: {type(value)!r}")


@dataclass(frozen=True)
class VarSpec:
    id: str
    name: str
    selectors: SelectorInput = field(default_factory=VarSelectors)
    primary: bool = False
    derived: bool = False
    derive: Optional[str] = None
    kind: Optional[str] = None
    units: Optional[str] = None
    normalize_units: Optional[str] = None
    scale: Optional[float] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "selectors", normalize_selectors(self.selectors))


@dataclass(frozen=True)
class VariableCapability:
    var_key: str
    name: str
    selectors: SelectorInput = field(default_factory=VarSelectors)
    primary: bool = False
    derived: bool = False
    derive_strategy_id: Optional[str] = None
    kind: Optional[str] = None
    units: Optional[str] = None
    normalize_units: Optional[str] = None
    scale: Optional[float] = None
    color_map_id: Optional[str] = None
    default_fh: Optional[int] = None
    buildable: bool = True
    order: Optional[int] = None
    legend_title: Optional[str] = None
    conversion: Optional[str] = None
    constraints: dict[str, Any] = field(default_factory=dict)
    frontend: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "selectors", normalize_selectors(self.selectors))

    def to_var_spec(self) -> VarSpec:
        return VarSpec(
            id=self.var_key,
            name=self.name,
            selectors=self.selectors,
            primary=self.primary,
            derived=self.derived,
            derive=self.derive_strategy_id,
            kind=self.kind,
            units=self.units,
            normalize_units=self.normalize_units,
            scale=self.scale,
        )


def build_var_specs(variable_catalog: Mapping[str, VariableCapability]) -> dict[str, VarSpec]:
    built: dict[str, VarSpec] = {}
    for key, capability in variable_catalog.items():
        normalized_key = str(key).strip()
        if normalized_key != capability.var_key:
            raise ValueError(
                f"Variable catalog key mismatch: key={normalized_key!r} "
                f"var_key={capability.var_key!r}"
            )
        built[normalized_key] = capability.to_var_spec()
    return built


@dataclass(frozen=True)
class ModelCapabilities:
    model_id: str
    name: str
    product: str = "sfc"
    canonical_region: str = "conus"
    grid_meters_by_region: dict[str, float] = field(default_factory=dict)
    run_discovery: dict[str, Any] = field(default_factory=dict)
    ui_defaults: dict[str, Any] = field(default_factory=dict)
    ui_constraints: dict[str, Any] = field(default_factory=dict)
    variable_catalog: dict[str, VariableCapability] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for key, capability in self.variable_catalog.items():
            normalized_key = str(key).strip()
            if normalized_key != capability.var_key:
                raise ValueError(
                    f"Invalid variable catalog entry for model={self.model_id!r}: "
                    f"key={normalized_key!r}, var_key={capability.var_key!r}"
                )


class ModelPlugin(Protocol):
    id: str
    name: str
    regions: Mapping[str, RegionSpec]
    vars: Mapping[str, VarSpec]
    product: str
    capabilities: ModelCapabilities | None

    def get_region(self, region_id: str) -> RegionSpec | None:
        ...

    def get_var(self, var_id: str) -> VarSpec | None:
        ...

    def get_var_capability(self, var_key: str) -> VariableCapability | None:
        ...

    def run_discovery_config(self) -> dict[str, Any]:
        ...

    def scheduled_fhs_for_var(self, var_key: str, cycle_hour: int) -> list[int]:
        ...

    def resolve_probe_var_key(self, requested_probe_var: str | None) -> str | None:
        ...

    def target_fhs(self, cycle_hour: int) -> list[int]:
        ...

    def normalize_var_id(self, var_id: str) -> str:
        ...

    def select_dataarray(self, ds: object, var_id: str) -> object:
        ...

    def ensure_latest_cycles(self, keep_cycles: int, *, cache_dir: Path | None = None) -> dict[str, int]:
        ...


@dataclass(frozen=True)
class BaseModelPlugin:
    id: str
    name: str
    regions: Mapping[str, RegionSpec] = field(default_factory=dict)
    vars: Mapping[str, VarSpec] = field(default_factory=dict)
    product: str = "sfc"
    capabilities: ModelCapabilities | None = None

    def __post_init__(self) -> None:
        if self.capabilities is None:
            return
        if not self.vars and self.capabilities.variable_catalog:
            object.__setattr__(self, "vars", build_var_specs(self.capabilities.variable_catalog))

    def get_region(self, region_id: str) -> RegionSpec | None:
        return self.regions.get(region_id)

    def get_var(self, var_id: str) -> VarSpec | None:
        return self.vars.get(var_id)

    def get_var_capability(self, var_key: str) -> VariableCapability | None:
        if self.capabilities is None:
            return None
        return self.capabilities.variable_catalog.get(var_key)

    def run_discovery_config(self) -> dict[str, Any]:
        if self.capabilities is None:
            return {}
        return dict(self.capabilities.run_discovery)

    def resolve_probe_var_key(self, requested_probe_var: str | None) -> str | None:
        if isinstance(requested_probe_var, str) and requested_probe_var.strip():
            normalized = self.normalize_var_id(requested_probe_var.strip().lower())
            if self.get_var(normalized) is not None:
                return normalized
        configured = self.run_discovery_config().get("probe_var_key")
        if isinstance(configured, str) and configured.strip():
            normalized = self.normalize_var_id(configured.strip().lower())
            if self.get_var(normalized) is not None:
                return normalized
        return None

    def _var_constraints(self, var_key: str) -> dict[str, Any]:
        capability = self.get_var_capability(var_key)
        if capability is None:
            return {}
        constraints = getattr(capability, "constraints", None)
        if isinstance(constraints, dict):
            return constraints
        return {}

    def scheduled_fhs_for_var(self, var_key: str, cycle_hour: int) -> list[int]:
        fhs = [int(fh) for fh in self.target_fhs(cycle_hour)]
        constraints = self._var_constraints(var_key)

        min_fh = constraints.get("min_fh")
        max_fh = constraints.get("max_fh")
        try:
            min_fh_value = int(min_fh) if min_fh is not None else None
        except (TypeError, ValueError):
            min_fh_value = None
        try:
            max_fh_value = int(max_fh) if max_fh is not None else None
        except (TypeError, ValueError):
            max_fh_value = None

        filtered: list[int] = []
        for fh in fhs:
            if min_fh_value is not None and fh < min_fh_value:
                continue
            if max_fh_value is not None and fh > max_fh_value:
                continue
            filtered.append(fh)
        return filtered

    def target_fhs(self, cycle_hour: int) -> list[int]:
        raise NotImplementedError("target_fhs is not implemented for this model")

    def normalize_var_id(self, var_id: str) -> str:
        return var_id

    def select_dataarray(self, ds: object, var_id: str) -> object:
        raise NotImplementedError("select_dataarray is not implemented for this model")

    def ensure_latest_cycles(self, keep_cycles: int, *, cache_dir: Path | None = None) -> dict[str, int]:
        raise NotImplementedError("ensure_latest_cycles is not implemented for this model")

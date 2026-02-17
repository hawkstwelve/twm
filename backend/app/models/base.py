from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Optional, Protocol, Sequence


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
    normalize_units: Optional[str] = None
    scale: Optional[float] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "selectors", normalize_selectors(self.selectors))


class ModelPlugin(Protocol):
    id: str
    name: str
    regions: Mapping[str, RegionSpec]
    vars: Mapping[str, VarSpec]
    product: str

    def get_region(self, region_id: str) -> RegionSpec | None:
        ...

    def get_var(self, var_id: str) -> VarSpec | None:
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

    def get_region(self, region_id: str) -> RegionSpec | None:
        return self.regions.get(region_id)

    def get_var(self, var_id: str) -> VarSpec | None:
        return self.vars.get(var_id)

    def target_fhs(self, cycle_hour: int) -> list[int]:
        raise NotImplementedError("target_fhs is not implemented for this model")

    def normalize_var_id(self, var_id: str) -> str:
        return var_id

    def select_dataarray(self, ds: object, var_id: str) -> object:
        raise NotImplementedError("select_dataarray is not implemented for this model")

    def ensure_latest_cycles(self, keep_cycles: int, *, cache_dir: Path | None = None) -> dict[str, int]:
        raise NotImplementedError("ensure_latest_cycles is not implemented for this model")

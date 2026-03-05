from __future__ import annotations

import sys
import threading
import time
import types
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.builder import fetch as fetch_module


def _install_fake_herbie(monkeypatch: pytest.MonkeyPatch, herbie_cls: type) -> None:
    fake_core = types.ModuleType("herbie.core")
    fake_core.Herbie = herbie_cls
    fake_pkg = types.ModuleType("herbie")
    fake_pkg.core = fake_core
    monkeypatch.setitem(sys.modules, "herbie", fake_pkg)
    monkeypatch.setitem(sys.modules, "herbie.core", fake_core)


def _install_fake_rasterio_open(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeDataset:
        crs = "EPSG:4326"
        transform = fetch_module.rasterio.transform.Affine.identity()
        nodata = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        def read(self, _band: int, masked: bool = True):
            del masked
            return np.ma.array([[1.0]], mask=[[False]], dtype=np.float32)

        def tags(self, *_args):
            return {}

    monkeypatch.setattr(fetch_module.rasterio, "open", lambda _path: _FakeDataset())


def test_no_idx_negative_cache_skips_repeated_herbie_calls_within_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeHerbie:
        calls = 0

        def __init__(self, *args, **kwargs):
            del args, kwargs
            type(self).calls += 1
            self.idx = None

    _install_fake_herbie(monkeypatch, _FakeHerbie)
    fetch_module.reset_herbie_runtime_caches_for_tests()

    clock = {"now": 1000.0}
    monkeypatch.setattr(fetch_module.time, "monotonic", lambda: float(clock["now"]))
    monkeypatch.setenv("TWF_HERBIE_PRIORITY", "aws")
    monkeypatch.setenv("TWF_HERBIE_SUBSET_RETRIES", "2")
    monkeypatch.setenv("TWF_HERBIE_IDX_NEGATIVE_CACHE_INITIAL_TTL_SECONDS", "60")
    monkeypatch.setenv("TWF_HERBIE_IDX_NEGATIVE_CACHE_MAX_TTL_SECONDS", "300")

    kwargs = dict(
        model_id="hrrr",
        product="sfc",
        search_pattern=":TMP:2 m above ground:",
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=13,
        herbie_kwargs={"priority": "aws"},
    )

    with pytest.raises(fetch_module.HerbieTransientUnavailableError):
        fetch_module.fetch_variable(**kwargs)
    assert _FakeHerbie.calls == 1

    with pytest.raises(fetch_module.HerbieTransientUnavailableError):
        fetch_module.fetch_variable(**kwargs)
    assert _FakeHerbie.calls == 1

    clock["now"] += 61.0
    with pytest.raises(fetch_module.HerbieTransientUnavailableError):
        fetch_module.fetch_variable(**kwargs)
    assert _FakeHerbie.calls == 2


def test_prs_idx_missing_switches_to_nomads(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pattern = ":TMP:850 mb:"

    class _FakeHerbie:
        init_priorities: list[str] = []
        download_priorities: list[str] = []
        idx_df_calls: dict[str, int] = {}

        def __init__(self, date: datetime, **kwargs):
            self.date = date
            self.model = kwargs.get("model")
            self.product = kwargs.get("product")
            self.fxx = kwargs.get("fxx")
            self.priority = kwargs.get("priority")
            type(self).init_priorities.append(str(self.priority))
            self.idx = None if self.priority == "aws" else f"https://{self.priority}.example/file.idx"
            self.grib = f"https://{self.priority}.example/file.grib2"

        @property
        def index_as_dataframe(self):
            current = str(self.priority)
            type(self).idx_df_calls[current] = int(type(self).idx_df_calls.get(current, 0)) + 1
            if self.priority == "aws":
                raise AssertionError("aws idx dataframe should not be requested when idx is missing")
            return pd.DataFrame([{"search_this": pattern, "start_byte": 0, "end_byte": 100}])

        def download(self, search_pattern: str, errors: str = "raise", overwrite: bool = True):
            del errors, overwrite
            assert search_pattern == pattern
            type(self).download_priorities.append(str(self.priority))
            out_path = tmp_path / f"{self.priority}.grib2"
            out_path.write_bytes(b"grib")
            return str(out_path)

    _install_fake_herbie(monkeypatch, _FakeHerbie)
    _install_fake_rasterio_open(monkeypatch)
    fetch_module.reset_herbie_runtime_caches_for_tests()
    monkeypatch.setenv("TWF_HERBIE_PRIORITY", "aws,google,azure")
    monkeypatch.setenv("TWF_HERBIE_SUBSET_RETRIES", "4")
    monkeypatch.setenv("TWF_HERBIE_RETRY_SLEEP_SECONDS", "0")

    fetch_module.fetch_variable(
        model_id="hrrr",
        product="prs",
        search_pattern=pattern,
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=13,
    )

    assert _FakeHerbie.init_priorities == ["aws", "nomads"]
    assert _FakeHerbie.download_priorities == ["nomads"]
    metrics = fetch_module.get_herbie_runtime_metrics_for_tests()
    assert metrics["counters"].get("source_switch_count", 0) == 1
    assert metrics["counters"].get("prs_idx_lag_count", 0) == 1


def test_prs_idx_missing_pattern_switches_to_nomads(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    requested_pattern = ":TMP:850 mb:"

    class _FakeHerbie:
        init_priorities: list[str] = []
        download_priorities: list[str] = []
        idx_df_calls: dict[str, int] = {}

        def __init__(self, date: datetime, **kwargs):
            self.date = date
            self.model = kwargs.get("model")
            self.product = kwargs.get("product")
            self.fxx = kwargs.get("fxx")
            self.priority = kwargs.get("priority")
            type(self).init_priorities.append(str(self.priority))
            self.idx = f"https://{self.priority}.example/file.idx"
            self.grib = f"https://{self.priority}.example/file.grib2"

        @property
        def index_as_dataframe(self):
            current = str(self.priority)
            type(self).idx_df_calls[current] = int(type(self).idx_df_calls.get(current, 0)) + 1
            if self.priority == "aws":
                return pd.DataFrame([{"search_this": ":RH:850 mb:", "start_byte": 0, "end_byte": 100}])
            return pd.DataFrame([{"search_this": requested_pattern, "start_byte": 0, "end_byte": 100}])

        def download(self, search_pattern: str, errors: str = "raise", overwrite: bool = True):
            del errors, overwrite
            assert search_pattern == requested_pattern
            type(self).download_priorities.append(str(self.priority))
            out_path = tmp_path / f"{self.priority}.grib2"
            out_path.write_bytes(b"grib")
            return str(out_path)

    _install_fake_herbie(monkeypatch, _FakeHerbie)
    _install_fake_rasterio_open(monkeypatch)
    fetch_module.reset_herbie_runtime_caches_for_tests()
    monkeypatch.setenv("TWF_HERBIE_PRIORITY", "aws,google,azure")
    monkeypatch.setenv("TWF_HERBIE_SUBSET_RETRIES", "4")
    monkeypatch.setenv("TWF_HERBIE_RETRY_SLEEP_SECONDS", "0")

    fetch_module.fetch_variable(
        model_id="hrrr",
        product="prs",
        search_pattern=requested_pattern,
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=13,
    )

    assert _FakeHerbie.init_priorities == ["aws", "nomads"]
    assert _FakeHerbie.download_priorities == ["nomads"]


def test_prs_empty_idx_switches_to_nomads(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pattern = ":TMP:850 mb:"

    class _FakeHerbie:
        init_priorities: list[str] = []
        download_priorities: list[str] = []

        def __init__(self, date: datetime, **kwargs):
            self.date = date
            self.model = kwargs.get("model")
            self.product = kwargs.get("product")
            self.fxx = kwargs.get("fxx")
            self.priority = kwargs.get("priority")
            type(self).init_priorities.append(str(self.priority))
            self.idx = f"https://{self.priority}.example/file.idx"
            self.grib = f"https://{self.priority}.example/file.grib2"

        @property
        def index_as_dataframe(self):
            if self.priority == "aws":
                return pd.DataFrame(columns=["search_this", "start_byte", "end_byte"])
            return pd.DataFrame([{"search_this": pattern, "start_byte": 0, "end_byte": 100}])

        def download(self, search_pattern: str, errors: str = "raise", overwrite: bool = True):
            del errors, overwrite
            assert search_pattern == pattern
            type(self).download_priorities.append(str(self.priority))
            out_path = tmp_path / f"{self.priority}.grib2"
            out_path.write_bytes(b"grib")
            return str(out_path)

    _install_fake_herbie(monkeypatch, _FakeHerbie)
    _install_fake_rasterio_open(monkeypatch)
    fetch_module.reset_herbie_runtime_caches_for_tests()
    monkeypatch.setenv("TWF_HERBIE_PRIORITY", "aws,google,azure")
    monkeypatch.setenv("TWF_HERBIE_SUBSET_RETRIES", "3")
    monkeypatch.setenv("TWF_HERBIE_RETRY_SLEEP_SECONDS", "0")

    fetch_module.fetch_variable(
        model_id="hrrr",
        product="prs",
        search_pattern=pattern,
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=13,
    )

    assert _FakeHerbie.init_priorities == ["aws", "nomads"]
    assert _FakeHerbie.download_priorities == ["nomads"]


def test_prs_idx_match_uses_prs_without_switch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pattern = ":TMP:850 mb:"

    class _FakeHerbie:
        init_priorities: list[str] = []
        download_priorities: list[str] = []

        def __init__(self, date: datetime, **kwargs):
            self.date = date
            self.model = kwargs.get("model")
            self.product = kwargs.get("product")
            self.fxx = kwargs.get("fxx")
            self.priority = kwargs.get("priority")
            type(self).init_priorities.append(str(self.priority))
            self.idx = f"https://{self.priority}.example/file.idx"
            self.grib = f"https://{self.priority}.example/file.grib2"

        @property
        def index_as_dataframe(self):
            return pd.DataFrame([{"search_this": pattern, "start_byte": 0, "end_byte": 100}])

        def download(self, search_pattern: str, errors: str = "raise", overwrite: bool = True):
            del errors, overwrite
            assert search_pattern == pattern
            type(self).download_priorities.append(str(self.priority))
            out_path = tmp_path / f"{self.priority}.grib2"
            out_path.write_bytes(b"grib")
            return str(out_path)

    _install_fake_herbie(monkeypatch, _FakeHerbie)
    _install_fake_rasterio_open(monkeypatch)
    fetch_module.reset_herbie_runtime_caches_for_tests()
    monkeypatch.setenv("TWF_HERBIE_PRIORITY", "aws,nomads")
    monkeypatch.setenv("TWF_HERBIE_SUBSET_RETRIES", "4")
    monkeypatch.setenv("TWF_HERBIE_RETRY_SLEEP_SECONDS", "0")

    fetch_module.fetch_variable(
        model_id="hrrr",
        product="prs",
        search_pattern=pattern,
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=13,
    )

    assert _FakeHerbie.init_priorities == ["aws"]
    assert _FakeHerbie.download_priorities == ["aws"]
    metrics = fetch_module.get_herbie_runtime_metrics_for_tests()
    assert metrics["counters"].get("source_switch_count", 0) == 0


def test_prs_idx_lag_does_not_retry_or_fan_out(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pattern = ":TMP:850 mb:"

    class _FakeHerbie:
        init_priorities: list[str] = []
        idx_df_calls: dict[str, int] = {}
        download_priorities: list[str] = []

        def __init__(self, date: datetime, **kwargs):
            self.date = date
            self.model = kwargs.get("model")
            self.product = kwargs.get("product")
            self.fxx = kwargs.get("fxx")
            self.priority = kwargs.get("priority")
            type(self).init_priorities.append(str(self.priority))
            self.idx = f"https://{self.priority}.example/file.idx"
            self.grib = f"https://{self.priority}.example/file.grib2"

        @property
        def index_as_dataframe(self):
            current = str(self.priority)
            type(self).idx_df_calls[current] = int(type(self).idx_df_calls.get(current, 0)) + 1
            if self.priority == "aws":
                raise RuntimeError("404 idx not found")
            return pd.DataFrame([{"search_this": pattern, "start_byte": 0, "end_byte": 100}])

        def download(self, search_pattern: str, errors: str = "raise", overwrite: bool = True):
            del errors, overwrite
            assert search_pattern == pattern
            type(self).download_priorities.append(str(self.priority))
            out_path = tmp_path / f"{self.priority}.grib2"
            out_path.write_bytes(b"grib")
            return str(out_path)

    _install_fake_herbie(monkeypatch, _FakeHerbie)
    _install_fake_rasterio_open(monkeypatch)
    fetch_module.reset_herbie_runtime_caches_for_tests()
    monkeypatch.setenv("TWF_HERBIE_PRIORITY", "aws,google,azure,pando")
    monkeypatch.setenv("TWF_HERBIE_SUBSET_RETRIES", "5")
    monkeypatch.setenv("TWF_HERBIE_RETRY_SLEEP_SECONDS", "0")

    fetch_module.fetch_variable(
        model_id="hrrr",
        product="prs",
        search_pattern=pattern,
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=13,
    )

    assert _FakeHerbie.idx_df_calls.get("aws", 0) == 1
    assert _FakeHerbie.init_priorities == ["aws", "nomads"]
    assert _FakeHerbie.download_priorities == ["nomads"]


def test_inventory_cache_reuses_idx_for_multiple_patterns(monkeypatch: pytest.MonkeyPatch) -> None:
    index_df = pd.DataFrame(
        [
            {"search_this": ":TMP:850 mb:", "start_byte": 0, "end_byte": 100},
            {"search_this": ":RH:850 mb:", "start_byte": 101, "end_byte": 200},
        ]
    )

    class _FakeHerbie:
        idx_df_calls = 0

        def __init__(self, *args, **kwargs):
            del args, kwargs
            self.idx = "https://aws.example/hrrr.t17z.wrfprsf13.grib2.idx"

        @property
        def index_as_dataframe(self):
            type(self).idx_df_calls += 1
            return index_df

    _install_fake_herbie(monkeypatch, _FakeHerbie)
    fetch_module.reset_herbie_runtime_caches_for_tests()
    monkeypatch.setenv("TWF_HERBIE_PRIORITY", "aws")
    monkeypatch.setenv("TWF_HERBIE_INVENTORY_CACHE_TTL_SECONDS", "600")

    common_kwargs = dict(
        model_id="hrrr",
        product="prs",
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=13,
        herbie_kwargs={"priority": "aws"},
    )
    tmp_lines = fetch_module.inventory_lines_for_pattern(
        search_pattern=":TMP:850 mb:",
        **common_kwargs,
    )
    rh_lines = fetch_module.inventory_lines_for_pattern(
        search_pattern=":RH:850 mb:",
        **common_kwargs,
    )

    assert tmp_lines == [":TMP:850 mb:"]
    assert rh_lines == [":RH:850 mb:"]
    assert _FakeHerbie.idx_df_calls == 1
    metrics = fetch_module.get_herbie_runtime_metrics_for_tests()
    assert metrics["counters"].get("idx_cache_store", 0) == 1
    assert metrics["counters"].get("idx_cache_hit", 0) >= 1
    assert metrics["timers_ms"].get("idx_fetch_ms", {}).get("count", 0) == 1
    assert metrics["timers_ms"].get("idx_parse_ms", {}).get("count", 0) >= 2


def test_inventory_cache_dedupes_inflight_idx_downloads(monkeypatch: pytest.MonkeyPatch) -> None:
    index_df = pd.DataFrame(
        [
            {
                "search_this": ":TMP:850 mb:",
                "start_byte": 0,
                "end_byte": 100,
            }
        ]
    )

    class _FakeHerbie:
        init_calls = 0
        idx_df_calls = 0
        _lock = threading.Lock()

        def __init__(self, *args, **kwargs):
            del args, kwargs
            type(self).init_calls += 1
            self.idx = "https://nomads.example/hrrr.t17z.wrfprsf13.grib2.idx"

        @property
        def index_as_dataframe(self):
            with type(self)._lock:
                type(self).idx_df_calls += 1
            time.sleep(0.1)
            return index_df

    _install_fake_herbie(monkeypatch, _FakeHerbie)
    fetch_module.reset_herbie_runtime_caches_for_tests()
    monkeypatch.setenv("TWF_HERBIE_PRIORITY", "aws")
    monkeypatch.setenv("TWF_HERBIE_INVENTORY_CACHE_TTL_SECONDS", "600")

    def _fetch_lines() -> list[str]:
        return fetch_module.inventory_lines_for_pattern(
            model_id="hrrr",
            product="prs",
            run_date=datetime(2026, 3, 5, 17, 0),
            fh=13,
            search_pattern=":TMP:850 mb:",
            herbie_kwargs={"priority": "aws"},
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        left_future = pool.submit(_fetch_lines)
        right_future = pool.submit(_fetch_lines)
        left = left_future.result()
        right = right_future.result()

    assert left == [":TMP:850 mb:"]
    assert right == [":TMP:850 mb:"]
    assert _FakeHerbie.init_calls == 2
    assert _FakeHerbie.idx_df_calls == 1


def test_inventory_cache_fetch_error_does_not_poison_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    index_df = pd.DataFrame([{"search_this": ":TMP:850 mb:", "start_byte": 0, "end_byte": 100}])

    class _FakeHerbie:
        idx_df_calls = 0

        def __init__(self, *args, **kwargs):
            del args, kwargs
            self.idx = "https://aws.example/hrrr.t17z.wrfsfcf13.grib2.idx"

        @property
        def index_as_dataframe(self):
            type(self).idx_df_calls += 1
            if type(self).idx_df_calls == 1:
                raise RuntimeError("temporary idx parse failure")
            return index_df

    _install_fake_herbie(monkeypatch, _FakeHerbie)
    fetch_module.reset_herbie_runtime_caches_for_tests()
    monkeypatch.setenv("TWF_HERBIE_PRIORITY", "aws")
    monkeypatch.setenv("TWF_HERBIE_INVENTORY_CACHE_TTL_SECONDS", "600")

    kwargs = dict(
        model_id="hrrr",
        product="sfc",
        run_date=datetime(2026, 3, 5, 17, 0),
        fh=13,
        search_pattern=":TMP:850 mb:",
        herbie_kwargs={"priority": "aws"},
    )
    first = fetch_module.inventory_lines_for_pattern(**kwargs)
    second = fetch_module.inventory_lines_for_pattern(**kwargs)

    assert first == []
    assert second == [":TMP:850 mb:"]
    assert _FakeHerbie.idx_df_calls == 2
    metrics = fetch_module.get_herbie_runtime_metrics_for_tests()
    assert metrics["counters"].get("idx_cache_error", 0) >= 1
    assert metrics["counters"].get("idx_cache_store", 0) == 1

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services import scheduler as scheduler_module


class _FakePlugin:
    id = "hrrr"

    def scheduled_fhs_for_var(self, var_key: str, cycle_hour: int) -> list[int]:
        del var_key, cycle_hour
        return [0, 1, 2, 3, 4]


class _FakeGFSPlugin:
    id = "gfs"

    def scheduled_fhs_for_var(self, var_key: str, cycle_hour: int) -> list[int]:
        del var_key, cycle_hour
        return [0, 3, 6, 9]


def test_resolve_promotion_fhs_uses_model_schedule() -> None:
    assert scheduler_module._resolve_promotion_fhs(_FakeGFSPlugin(), ["tmp2m"], 18) == (0, 3, 6)


def test_process_run_uses_resolved_promotion_fhs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    run_dt = datetime(2026, 2, 27, 18, tzinfo=timezone.utc)
    seen_promotion_fhs: list[tuple[int, ...]] = []

    def fake_frame_artifacts_exist(
        data_root: Path,
        model: str,
        run: str,
        var_id: str,
        fh: int,
    ) -> bool:
        del data_root, model, run, var_id, fh
        return False

    def fake_build_one(
        *,
        model_id: str,
        var_id: str,
        fh: int,
        run_dt: datetime,
        data_root: Path,
        plugin: object,
    ) -> tuple[str, int, bool]:
        del model_id, run_dt, data_root, plugin
        return var_id, fh, False

    def fake_should_promote(
        data_root: Path,
        model: str,
        run_id: str,
        primary_vars: list[str],
        promotion_fhs: tuple[int, ...],
    ) -> bool:
        del data_root, model, run_id, primary_vars
        seen_promotion_fhs.append(tuple(int(fh) for fh in promotion_fhs))
        return False

    monkeypatch.setattr(scheduler_module, "_frame_artifacts_exist", fake_frame_artifacts_exist)
    monkeypatch.setattr(scheduler_module, "_build_one", fake_build_one)
    monkeypatch.setattr(scheduler_module, "_should_promote", fake_should_promote)
    monkeypatch.setattr(scheduler_module, "_enforce_run_retention", lambda *args, **kwargs: None)

    scheduler_module._process_run(
        plugin=_FakeGFSPlugin(),
        model_id="gfs",
        vars_to_build=["tmp2m"],
        primary_vars=["tmp2m"],
        run_dt=run_dt,
        data_root=tmp_path,
        workers=1,
        keep_runs=2,
        loop_pregenerate_enabled=False,
        loop_cache_root=tmp_path / "loop-cache",
        loop_workers=1,
        loop_tier0_quality=82,
        loop_tier0_max_dim=1600,
        loop_tier0_fixed_w=1600,
        loop_tier1_quality=86,
        loop_tier1_max_dim=2400,
        loop_tier1_fixed_w=2400,
    )

    assert seen_promotion_fhs
    assert seen_promotion_fhs[0] == (0, 3, 6)


def test_process_run_catches_up_consecutive_available_hours(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    run_dt = datetime(2026, 2, 27, 12, tzinfo=timezone.utc)
    run_id = scheduler_module._run_id_from_dt(run_dt)
    model_id = "hrrr"

    built: set[tuple[str, int]] = set()
    attempted: list[tuple[str, int]] = []
    available_up_to = {"tmp2m": 3}

    def fake_frame_artifacts_exist(
        data_root: Path,
        model: str,
        run: str,
        var_id: str,
        fh: int,
    ) -> bool:
        del data_root, model, run
        return (var_id, fh) in built

    def fake_build_one(
        *,
        model_id: str,
        var_id: str,
        fh: int,
        run_dt: datetime,
        data_root: Path,
        plugin: object,
    ) -> tuple[str, int, bool]:
        del model_id, run_dt, data_root, plugin
        attempted.append((var_id, fh))
        ok = fh <= available_up_to[var_id]
        if ok:
            built.add((var_id, fh))
        return var_id, fh, ok

    monkeypatch.setattr(scheduler_module, "_frame_artifacts_exist", fake_frame_artifacts_exist)
    monkeypatch.setattr(scheduler_module, "_build_one", fake_build_one)
    monkeypatch.setattr(scheduler_module, "_should_promote", lambda *args, **kwargs: False)
    monkeypatch.setattr(scheduler_module, "_enforce_run_retention", lambda *args, **kwargs: None)

    processed_run_id, available, total = scheduler_module._process_run(
        plugin=_FakePlugin(),
        model_id=model_id,
        vars_to_build=["tmp2m"],
        primary_vars=["tmp2m"],
        run_dt=run_dt,
        data_root=tmp_path,
        workers=1,
        keep_runs=2,
        loop_pregenerate_enabled=False,
        loop_cache_root=tmp_path / "loop-cache",
        loop_workers=1,
        loop_tier0_quality=82,
        loop_tier0_max_dim=1600,
        loop_tier0_fixed_w=1600,
        loop_tier1_quality=86,
        loop_tier1_max_dim=2400,
        loop_tier1_fixed_w=2400,
    )

    assert processed_run_id == run_id
    assert total == 5
    assert available == 4
    assert attempted == [("tmp2m", 0), ("tmp2m", 1), ("tmp2m", 2), ("tmp2m", 3), ("tmp2m", 4)]


def test_process_run_publishes_early_then_refreshes_after_more_progress(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    run_dt = datetime(2026, 2, 27, 12, tzinfo=timezone.utc)
    model_id = "hrrr"

    built: set[tuple[str, int]] = set()
    available_up_to = {"tmp2m": 3}

    def fake_frame_artifacts_exist(
        data_root: Path,
        model: str,
        run: str,
        var_id: str,
        fh: int,
    ) -> bool:
        del data_root, model, run
        return (var_id, fh) in built

    def fake_build_one(
        *,
        model_id: str,
        var_id: str,
        fh: int,
        run_dt: datetime,
        data_root: Path,
        plugin: object,
    ) -> tuple[str, int, bool]:
        del model_id, run_dt, data_root, plugin
        ok = fh <= available_up_to[var_id]
        if ok:
            built.add((var_id, fh))
        return var_id, fh, ok

    publish_promote_snapshots: list[list[int]] = []
    manifest_calls = 0
    pointer_calls = 0

    def fake_should_promote(*args: object, **kwargs: object) -> bool:
        del args, kwargs
        return ("tmp2m", 2) in built

    def fake_promote_run(data_root: Path, model: str, run_id: str) -> None:
        del data_root, model, run_id
        publish_promote_snapshots.append(sorted(fh for var_id, fh in built if var_id == "tmp2m"))

    def fake_write_run_manifest(*args: object, **kwargs: object) -> None:
        del args, kwargs
        nonlocal manifest_calls
        manifest_calls += 1

    def fake_write_latest_pointer(data_root: Path, model: str, run_id: str) -> None:
        del data_root, model, run_id
        nonlocal pointer_calls
        pointer_calls += 1

    monkeypatch.setattr(scheduler_module, "_frame_artifacts_exist", fake_frame_artifacts_exist)
    monkeypatch.setattr(scheduler_module, "_build_one", fake_build_one)
    monkeypatch.setattr(scheduler_module, "_should_promote", fake_should_promote)
    monkeypatch.setattr(scheduler_module, "_promote_run", fake_promote_run)
    monkeypatch.setattr(scheduler_module, "_write_run_manifest", fake_write_run_manifest)
    monkeypatch.setattr(scheduler_module, "_write_latest_pointer", fake_write_latest_pointer)
    monkeypatch.setattr(scheduler_module, "_enforce_run_retention", lambda *args, **kwargs: None)

    scheduler_module._process_run(
        plugin=_FakePlugin(),
        model_id=model_id,
        vars_to_build=["tmp2m"],
        primary_vars=["tmp2m"],
        run_dt=run_dt,
        data_root=tmp_path,
        workers=1,
        keep_runs=2,
        loop_pregenerate_enabled=False,
        loop_cache_root=tmp_path / "loop-cache",
        loop_workers=1,
        loop_tier0_quality=82,
        loop_tier0_max_dim=1600,
        loop_tier0_fixed_w=1600,
        loop_tier1_quality=86,
        loop_tier1_max_dim=2400,
        loop_tier1_fixed_w=2400,
    )

    assert publish_promote_snapshots == [[0, 1, 2], [0, 1, 2, 3]]
    assert manifest_calls == 2
    assert pointer_calls == 2

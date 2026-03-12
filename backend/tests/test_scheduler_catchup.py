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


def test_process_run_republishes_progress_during_long_catchup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    run_dt = datetime(2026, 2, 27, 12, tzinfo=timezone.utc)
    model_id = "hrrr"

    built: set[tuple[str, int]] = set()
    available_up_to = {"tmp2m": 4}
    publish_promote_snapshots: list[list[int]] = []

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

    def fake_should_promote(*args: object, **kwargs: object) -> bool:
        del args, kwargs
        return ("tmp2m", 1) in built

    def fake_promote_run(data_root: Path, model: str, run_id: str) -> None:
        del data_root, model, run_id
        publish_promote_snapshots.append(sorted(fh for var_id, fh in built if var_id == "tmp2m"))

    monkeypatch.setenv("CARTOSKY_PROGRESS_PUBLISH_MIN_NEW_FRAMES", "1")
    monkeypatch.setattr(scheduler_module, "_frame_artifacts_exist", fake_frame_artifacts_exist)
    monkeypatch.setattr(scheduler_module, "_build_one", fake_build_one)
    monkeypatch.setattr(scheduler_module, "_should_promote", fake_should_promote)
    monkeypatch.setattr(scheduler_module, "_promote_run", fake_promote_run)
    monkeypatch.setattr(scheduler_module, "_write_run_manifest", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_module, "_write_latest_pointer", lambda *args, **kwargs: None)
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

    assert publish_promote_snapshots == [
        [0, 1],
        [0, 1, 2],
        [0, 1, 2, 3],
        [0, 1, 2, 3, 4],
    ]


def test_enforce_herbie_cache_retention_keeps_latest_four_runs(tmp_path: Path) -> None:
    herbie_root = tmp_path / "herbie_cache"
    model_root = herbie_root / "hrrr"
    kept = {
        "20260227_18z",
        "20260227_12z",
        "20260227_06z",
        "20260227_00z",
    }
    removed = {
        "20260226_18z",
        "20260226_12z",
    }

    files = {
        "20260227_18z": [
            model_root / "20260227" / "hrrr.t18z.wrfsfcf00.grib2",
            model_root / "20260227" / "subset_deadbeef__hrrr.t18z.wrfsfcf00.grib2",
            model_root / "20260227" / "subset_deadbeef__hrrr.t18z.wrfsfcf00.grib2.lock",
        ],
        "20260227_12z": [model_root / "20260227" / "hrrr.t12z.wrfsfcf00.grib2"],
        "20260227_06z": [model_root / "20260227" / "hrrr.t06z.wrfsfcf00.grib2"],
        "20260227_00z": [model_root / "20260227" / "hrrr.t00z.wrfsfcf00.grib2"],
        "20260226_18z": [model_root / "20260226" / "hrrr.t18z.wrfsfcf00.grib2"],
        "20260226_12z": [model_root / "20260226" / "subset_badcafe__hrrr.t12z.wrfsfcf00.grib2"],
    }
    for paths in files.values():
        for path in paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("x")

    scheduler_module._enforce_herbie_cache_retention(herbie_root, "hrrr", 4)

    for run_id in kept:
        for path in files[run_id]:
            assert path.exists()
    for run_id in removed:
        for path in files[run_id]:
            assert not path.exists()
    assert not (model_root / "20260226").exists()


def test_enforce_herbie_cache_retention_preserves_unparsed_files(tmp_path: Path) -> None:
    herbie_root = tmp_path / "herbie_cache"
    model_root = herbie_root / "gfs"
    legacy_note = model_root / "20260226" / "README.txt"
    legacy_note.parent.mkdir(parents=True, exist_ok=True)
    legacy_note.write_text("keep me")

    run_ids = [
        "20260227_18z",
        "20260227_12z",
        "20260227_06z",
        "20260227_00z",
        "20260226_18z",
    ]
    for run_id in run_ids:
        day, hour = run_id.split("_")
        path = model_root / day / f"gfs.t{hour[:2]}z.pgrb2.0p25.f000"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run_id)

    scheduler_module._enforce_herbie_cache_retention(herbie_root, "gfs", 4)

    assert legacy_note.exists()
    assert not (model_root / "20260226" / "gfs.t18z.pgrb2.0p25.f000").exists()
    assert (model_root / "20260227" / "gfs.t18z.pgrb2.0p25.f000").exists()


def test_process_run_skips_loop_pregen_for_incomplete_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    run_dt = datetime(2026, 2, 27, 12, tzinfo=timezone.utc)
    model_id = "hrrr"

    built: set[tuple[str, int]] = set()
    available_up_to = {"tmp2m": 2}
    loop_pregen_calls = 0

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

    def fake_should_promote(*args: object, **kwargs: object) -> bool:
        del args, kwargs
        return ("tmp2m", 2) in built

    def fake_loop_pregen(*args: object, **kwargs: object) -> None:
        del args, kwargs
        nonlocal loop_pregen_calls
        loop_pregen_calls += 1

    monkeypatch.setattr(scheduler_module, "_frame_artifacts_exist", fake_frame_artifacts_exist)
    monkeypatch.setattr(scheduler_module, "_build_one", fake_build_one)
    monkeypatch.setattr(scheduler_module, "_should_promote", fake_should_promote)
    monkeypatch.setattr(scheduler_module, "_promote_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_module, "_write_run_manifest", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_module, "_write_latest_pointer", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_module, "_pregenerate_loop_webp_for_run", fake_loop_pregen)
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
        loop_pregenerate_enabled=True,
        loop_cache_root=tmp_path / "loop-cache",
        loop_workers=1,
        loop_tier0_quality=82,
        loop_tier0_max_dim=1600,
        loop_tier0_fixed_w=1600,
        loop_tier1_quality=86,
        loop_tier1_max_dim=2400,
        loop_tier1_fixed_w=2400,
    )

    assert loop_pregen_calls == 0


def test_process_run_pregenerates_loop_cache_when_run_is_complete(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    run_dt = datetime(2026, 2, 27, 12, tzinfo=timezone.utc)
    model_id = "hrrr"

    built: set[tuple[str, int]] = set()
    available_up_to = {"tmp2m": 4}
    loop_pregen_calls = 0

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

    def fake_should_promote(*args: object, **kwargs: object) -> bool:
        del args, kwargs
        return ("tmp2m", 2) in built

    def fake_loop_pregen(*args: object, **kwargs: object) -> None:
        del args, kwargs
        nonlocal loop_pregen_calls
        loop_pregen_calls += 1

    monkeypatch.setattr(scheduler_module, "_frame_artifacts_exist", fake_frame_artifacts_exist)
    monkeypatch.setattr(scheduler_module, "_build_one", fake_build_one)
    monkeypatch.setattr(scheduler_module, "_should_promote", fake_should_promote)
    monkeypatch.setattr(scheduler_module, "_promote_run", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_module, "_write_run_manifest", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_module, "_write_latest_pointer", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_module, "_pregenerate_loop_webp_for_run", fake_loop_pregen)
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
        loop_pregenerate_enabled=True,
        loop_cache_root=tmp_path / "loop-cache",
        loop_workers=1,
        loop_tier0_quality=82,
        loop_tier0_max_dim=1600,
        loop_tier0_fixed_w=1600,
        loop_tier1_quality=86,
        loop_tier1_max_dim=2400,
        loop_tier1_fixed_w=2400,
    )

    assert loop_pregen_calls == 1


def _write_sidecar(tmp_path: Path, run_id: str, var_id: str, fh: int, *, quality: str, quality_flags: list[str]) -> None:
    sidecar = tmp_path / "staging" / "hrrr" / run_id / var_id / f"fh{fh:03d}.json"
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    sidecar.write_text(
        scheduler_module.json.dumps(
            {
                "contract_version": "3.0",
                "model": "hrrr",
                "run": run_id,
                "var": var_id,
                "fh": fh,
                "quality": quality,
                "quality_flags": quality_flags,
            }
        )
    )


class _FakeKucheraPlugin:
    id = "hrrr"

    def normalize_var_id(self, var_id: str) -> str:
        return str(var_id)

    def scheduled_fhs_for_var(self, var_key: str, cycle_hour: int) -> list[int]:
        del var_key, cycle_hour
        return [0, 1]


def test_process_run_requeues_only_slr_fallback_degraded_frames(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    run_dt = datetime(2026, 3, 5, 17, tzinfo=timezone.utc)
    run_id = scheduler_module._run_id_from_dt(run_dt)

    _write_sidecar(
        tmp_path,
        run_id,
        "snowfall_kuchera_total",
        0,
        quality="degraded",
        quality_flags=["apcp_cumulative_fallback"],
    )
    _write_sidecar(
        tmp_path,
        run_id,
        "snowfall_kuchera_total",
        1,
        quality="degraded",
        quality_flags=["slr_fallback_10to1"],
    )

    attempted: list[tuple[str, int]] = []

    def fake_build_one(
        *,
        model_id: str,
        var_id: str,
        fh: int,
        run_dt: datetime,
        data_root: Path,
        plugin: object,
    ) -> tuple[str, int, bool]:
        del model_id, run_dt, plugin
        attempted.append((var_id, fh))
        _write_sidecar(
            data_root,
            run_id,
            var_id,
            fh,
            quality="full",
            quality_flags=[],
        )
        return var_id, fh, True

    monkeypatch.setattr(scheduler_module, "_frame_artifacts_exist", lambda *args, **kwargs: True)
    monkeypatch.setattr(scheduler_module, "_build_one", fake_build_one)
    monkeypatch.setattr(scheduler_module, "_should_promote", lambda *args, **kwargs: False)
    monkeypatch.setattr(scheduler_module, "_enforce_run_retention", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_module, "_kuchera_rebuild_profile_ready", lambda **kwargs: True)
    monkeypatch.setattr(scheduler_module, "_run_is_superseded", lambda **kwargs: False)

    scheduler_module._process_run(
        plugin=_FakeKucheraPlugin(),
        model_id="hrrr",
        vars_to_build=["snowfall_kuchera_total"],
        primary_vars=["snowfall_kuchera_total"],
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

    assert attempted == [("snowfall_kuchera_total", 1)]


def test_process_run_caps_degraded_rebuild_attempts_at_two(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    run_dt = datetime(2026, 3, 5, 17, tzinfo=timezone.utc)
    run_id = scheduler_module._run_id_from_dt(run_dt)

    _write_sidecar(
        tmp_path,
        run_id,
        "snowfall_kuchera_total",
        1,
        quality="degraded",
        quality_flags=["slr_fallback_10to1"],
    )

    attempted: list[tuple[str, int]] = []

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
        return var_id, fh, False

    monkeypatch.setattr(scheduler_module, "_frame_artifacts_exist", lambda *args, **kwargs: True)
    monkeypatch.setattr(scheduler_module, "_build_one", fake_build_one)
    monkeypatch.setattr(scheduler_module, "_should_promote", lambda *args, **kwargs: False)
    monkeypatch.setattr(scheduler_module, "_enforce_run_retention", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_module, "_kuchera_rebuild_profile_ready", lambda **kwargs: True)
    monkeypatch.setattr(scheduler_module, "_run_is_superseded", lambda **kwargs: False)

    scheduler_module._process_run(
        plugin=_FakeKucheraPlugin(),
        model_id="hrrr",
        vars_to_build=["snowfall_kuchera_total"],
        primary_vars=["snowfall_kuchera_total"],
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

    assert attempted == [
        ("snowfall_kuchera_total", 1),
        ("snowfall_kuchera_total", 1),
    ]


def test_process_run_abandons_rebuilds_when_superseded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    run_dt = datetime(2026, 3, 5, 17, tzinfo=timezone.utc)
    run_id = scheduler_module._run_id_from_dt(run_dt)

    _write_sidecar(
        tmp_path,
        run_id,
        "snowfall_kuchera_total",
        1,
        quality="degraded",
        quality_flags=["slr_fallback_10to1"],
    )

    attempted: list[tuple[str, int]] = []

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
        return var_id, fh, True

    monkeypatch.setattr(scheduler_module, "_frame_artifacts_exist", lambda *args, **kwargs: True)
    monkeypatch.setattr(scheduler_module, "_build_one", fake_build_one)
    monkeypatch.setattr(scheduler_module, "_should_promote", lambda *args, **kwargs: False)
    monkeypatch.setattr(scheduler_module, "_enforce_run_retention", lambda *args, **kwargs: None)
    monkeypatch.setattr(scheduler_module, "_kuchera_rebuild_profile_ready", lambda **kwargs: True)
    monkeypatch.setattr(scheduler_module, "_run_is_superseded", lambda **kwargs: True)

    scheduler_module._process_run(
        plugin=_FakeKucheraPlugin(),
        model_id="hrrr",
        vars_to_build=["snowfall_kuchera_total"],
        primary_vars=["snowfall_kuchera_total"],
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

    assert attempted == []

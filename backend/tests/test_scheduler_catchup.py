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
        loop_tier1_quality=86,
        loop_tier1_max_dim=2400,
    )

    assert processed_run_id == run_id
    assert total == 5
    assert available == 4
    assert attempted == [("tmp2m", 0), ("tmp2m", 1), ("tmp2m", 2), ("tmp2m", 3), ("tmp2m", 4)]

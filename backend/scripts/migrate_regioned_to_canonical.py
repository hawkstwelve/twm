from __future__ import annotations

import argparse
import json
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def _run_id_from_name(name: str) -> bool:
    if len(name) != 12:
        return False
    if name[8] != "_" or not name.endswith("z"):
        return False
    day = name[:8]
    hour = name[9:11]
    return day.isdigit() and hour.isdigit()


def _discover_region_runs(published_root: Path) -> dict[tuple[str, str], list[tuple[str, Path]]]:
    grouped: dict[tuple[str, str], list[tuple[str, Path]]] = defaultdict(list)
    if not published_root.is_dir():
        return grouped

    for model_dir in published_root.iterdir():
        if not model_dir.is_dir():
            continue
        for region_dir in model_dir.iterdir():
            if not region_dir.is_dir():
                continue
            for run_dir in region_dir.iterdir():
                if not run_dir.is_dir() or not _run_id_from_name(run_dir.name):
                    continue
                grouped[(model_dir.name, run_dir.name)].append((region_dir.name, run_dir))
    return grouped


def _pick_source(regions: list[tuple[str, Path]], prefer_region: str | None) -> tuple[str, Path]:
    if prefer_region:
        for region_name, run_dir in regions:
            if region_name == prefer_region:
                return region_name, run_dir
        raise RuntimeError(f"Preferred region {prefer_region!r} not found in candidates: {[r for r, _ in regions]}")

    if len(regions) > 1:
        raise RuntimeError(
            "Multiple region sources found for run; rerun with --prefer-region. "
            f"Candidates: {[r for r, _ in regions]}"
        )
    return regions[0]


def _copytree_once(src: Path, dst: Path, apply: bool) -> None:
    if dst.exists():
        raise RuntimeError(f"Destination already exists: {dst}")
    if apply:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(src, dst)


def _load_manifest(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _write_json(path: Path, payload: dict, apply: bool) -> None:
    if apply:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2) + "\n")


def _build_manifest_from_sidecars(model: str, run_id: str, run_dir: Path) -> dict:
    variables: dict[str, dict] = {}

    for var_dir in sorted(run_dir.iterdir()):
        if not var_dir.is_dir():
            continue
        frames: list[dict] = []
        units = ""
        kind = ""

        for sidecar_path in sorted(var_dir.glob("fh*.json")):
            try:
                sidecar = json.loads(sidecar_path.read_text())
            except Exception:
                continue

            fh_val = sidecar.get("fh")
            if not isinstance(fh_val, int):
                continue
            valid_time = sidecar.get("valid_time")
            entry: dict[str, object] = {"fh": fh_val}
            if isinstance(valid_time, str) and valid_time:
                entry["valid_time"] = valid_time
            frames.append(entry)

            if not units:
                units = str(sidecar.get("units", ""))
            if not kind:
                kind = str(sidecar.get("kind", ""))

        frames = sorted(frames, key=lambda row: row["fh"])
        variables[var_dir.name] = {
            "kind": kind,
            "units": units,
            "expected_frames": len(frames),
            "available_frames": len(frames),
            "frames": frames,
        }

    return {
        "contract_version": "3.0",
        "model": model,
        "run": run_id,
        "variables": variables,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def migrate(data_root: Path, *, apply: bool, prefer_region: str | None) -> None:
    published_root = data_root / "published"
    manifests_root = data_root / "manifests"

    grouped = _discover_region_runs(published_root)
    if not grouped:
        print("No regioned published runs found; nothing to migrate.")
        return

    print(f"Discovered {len(grouped)} model/run group(s) for migration")

    for (model, run_id), region_runs in sorted(grouped.items()):
        source_region, source_run_dir = _pick_source(region_runs, prefer_region)

        canonical_model_root = published_root / model
        canonical_run_dir = canonical_model_root / run_id
        _copytree_once(source_run_dir, canonical_run_dir, apply)
        print(f"published: {model}/{run_id} <- region {source_region}")

        region_manifest = manifests_root / model / source_region / f"{run_id}.json"
        manifest_payload = _load_manifest(region_manifest)
        if manifest_payload is None:
            manifest_payload = _build_manifest_from_sidecars(model, run_id, source_run_dir)
            print(f"manifest: synthesized from sidecars for {model}/{run_id}")
            canonical_manifest = manifests_root / model / f"{run_id}.json"
            if canonical_manifest.exists():
                raise RuntimeError(f"Canonical manifest already exists: {canonical_manifest}")
            _write_json(canonical_manifest, manifest_payload, apply)
        else:
            manifest_payload.pop("region", None)
            canonical_manifest = manifests_root / model / f"{run_id}.json"
            if canonical_manifest.exists():
                raise RuntimeError(f"Canonical manifest already exists: {canonical_manifest}")
            _write_json(canonical_manifest, manifest_payload, apply)
            print(f"manifest: {model}/{run_id}.json")

        latest_src = published_root / model / source_region / "LATEST.json"
        latest_dst = published_root / model / "LATEST.json"
        if latest_src.is_file() and not latest_dst.exists():
            latest_payload = _load_manifest(latest_src)
            if isinstance(latest_payload, dict):
                _write_json(latest_dst, latest_payload, apply)
                print(f"latest: {model}/LATEST.json")

    if apply:
        print("Migration applied.")
    else:
        print("Dry run complete. Re-run with --apply to execute.")


def main() -> int:
    parser = argparse.ArgumentParser(description="One-shot optional migration: regioned published/manifests -> canonical non-region layout")
    parser.add_argument("--data-root", default="./data/v3", help="Path to data root (default: ./data/v3)")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    parser.add_argument("--prefer-region", default=None, help="If multiple region runs exist for same model/run, select this region")
    args = parser.parse_args()

    try:
        migrate(Path(args.data_root), apply=bool(args.apply), prefer_region=args.prefer_region)
    except Exception as exc:
        print(f"Migration failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

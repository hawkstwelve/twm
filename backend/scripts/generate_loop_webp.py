#!/usr/bin/env python3
"""Pre-generate loop WebP artifacts from published RGBA COG frames.

Usage:
    PYTHONPATH=backend .venv/bin/python backend/scripts/generate_loop_webp.py \
      --model hrrr --run 20260223_14z --data-root ./data/v3

Optional:
    --var tmp2m          # only one variable
    --overwrite          # regenerate existing .loop.webp files
    --workers 6          # parallel conversion workers
"""

from __future__ import annotations

import argparse
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import rasterio
from PIL import Image
from rasterio.enums import Resampling


RUN_ID_RE = re.compile(r"^\d{8}_\d{2}z$")
DEFAULT_WEBP_QUALITY = int(os.environ.get("TWF_V3_LOOP_WEBP_QUALITY", "82"))
DEFAULT_WEBP_MAX_DIM = int(os.environ.get("TWF_V3_LOOP_WEBP_MAX_DIM", "1600"))


@dataclass
class Job:
    variable: str
    fh: str
    cog_path: Path
    webp_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pre-generate loop WebP frames from published RGBA COGs")
    parser.add_argument("--model", required=True, help="Model ID (e.g. hrrr)")
    parser.add_argument("--run", required=True, help="Run ID (e.g. 20260223_14z)")
    parser.add_argument("--var", dest="variable", default=None, help="Optional variable filter (e.g. tmp2m)")
    parser.add_argument(
        "--data-root",
        default=os.environ.get("TWF_V3_DATA_ROOT", "./data/v3"),
        help="Data root containing published/ (default: env TWF_V3_DATA_ROOT or ./data/v3)",
    )
    parser.add_argument("--quality", type=int, default=DEFAULT_WEBP_QUALITY, help="WebP quality (default: 82)")
    parser.add_argument(
        "--max-dim",
        type=int,
        default=DEFAULT_WEBP_MAX_DIM,
        help="Max output dimension in px (default: 1600)",
    )
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers (default: 4)")
    parser.add_argument("--overwrite", action="store_true", help="Regenerate existing .loop.webp files")
    return parser.parse_args()


def discover_jobs(run_dir: Path, variable_filter: str | None, overwrite: bool) -> list[Job]:
    jobs: list[Job] = []
    if not run_dir.is_dir():
        return jobs

    for var_dir in sorted([p for p in run_dir.iterdir() if p.is_dir()]):
        variable = var_dir.name
        if variable_filter and variable != variable_filter:
            continue

        for cog_path in sorted(var_dir.glob("fh*.rgba.cog.tif")):
            fh = cog_path.name.split(".")[0]
            webp_path = var_dir / f"{fh}.loop.webp"
            if webp_path.is_file() and not overwrite:
                continue
            jobs.append(Job(variable=variable, fh=fh, cog_path=cog_path, webp_path=webp_path))

    return jobs


def convert_job(job: Job, quality: int, max_dim: int) -> tuple[Job, bool, str | None]:
    try:
        job.webp_path.parent.mkdir(parents=True, exist_ok=True)

        with rasterio.open(job.cog_path) as ds:
            src_h = int(ds.height)
            src_w = int(ds.width)
            max_side = max(src_h, src_w)
            if max_side <= 0:
                return (job, False, "invalid source dimensions")

            scale = min(1.0, float(max_dim) / float(max_side))
            out_h = max(1, int(round(src_h * scale)))
            out_w = max(1, int(round(src_w * scale)))

            data = ds.read(
                indexes=(1, 2, 3, 4),
                out_shape=(4, out_h, out_w),
                resampling=Resampling.bilinear,
            )

        rgba = np.moveaxis(data, 0, -1)
        image = Image.fromarray(rgba, mode="RGBA")
        image.save(job.webp_path, format="WEBP", quality=quality, method=6)
        return (job, True, None)
    except Exception as exc:
        return (job, False, str(exc))


def main() -> int:
    args = parse_args()

    if not RUN_ID_RE.match(args.run):
        print(f"ERROR: invalid --run format: {args.run!r} (expected YYYYMMDD_HHz)")
        return 2
    if args.workers < 1:
        print("ERROR: --workers must be >= 1")
        return 2
    if args.max_dim < 64:
        print("ERROR: --max-dim must be >= 64")
        return 2

    data_root = Path(args.data_root)
    run_dir = data_root / "published" / args.model / args.run
    if not run_dir.is_dir():
        print(f"ERROR: run directory not found: {run_dir}")
        return 1

    jobs = discover_jobs(run_dir, args.variable, args.overwrite)
    if not jobs:
        msg_var = f" variable={args.variable}" if args.variable else ""
        print(f"No conversion jobs found for model={args.model} run={args.run}{msg_var}")
        return 0

    print(
        f"Generating loop WebP frames: model={args.model} run={args.run} "
        f"jobs={len(jobs)} workers={args.workers} quality={args.quality} max_dim={args.max_dim}"
    )

    ok = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(convert_job, job, args.quality, args.max_dim) for job in jobs]
        for future in as_completed(futures):
            job, success, error = future.result()
            if success:
                ok += 1
                print(f"OK   {job.variable}/{job.fh}.loop.webp")
            else:
                failed += 1
                print(f"FAIL {job.variable}/{job.fh}.loop.webp :: {error}")

    print(f"Done. success={ok} failed={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

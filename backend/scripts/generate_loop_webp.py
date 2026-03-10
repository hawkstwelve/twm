#!/usr/bin/env python3
"""Pre-generate loop WebP artifacts from published RGBA COG frames.

Usage:
    PYTHONPATH=backend .venv/bin/python backend/scripts/generate_loop_webp.py \
    --model hrrr --run 20260223_14z --data-root ./data

Optional:
    --var tmp2m          # only one variable
    --overwrite          # regenerate existing .loop.webp files
    --workers 6          # parallel conversion workers
    --output-root /tmp/cartosky_loop_webp_cache
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

from app.services.builder.colorize import float_to_rgba
from app.services.render_resampling import (
    compute_loop_output_shape,
    high_quality_loop_resampling,
    rasterio_resampling_for_loop,
    use_value_render_for_variable,
    variable_color_map_id,
)


RUN_ID_RE = re.compile(r"^\d{8}_\d{2}z$")
def _env_value(*names: str, default: str = "") -> str:
    for name in names:
        raw = os.environ.get(name)
        if raw is not None and raw != "":
            return raw
    return default


DEFAULT_WEBP_QUALITY = int(
    _env_value("CARTOSKY_LOOP_WEBP_QUALITY", "CARTOSKY_V3_LOOP_WEBP_QUALITY", "TWF_V3_LOOP_WEBP_QUALITY", default="82")
)
DEFAULT_WEBP_MAX_DIM = int(
    _env_value("CARTOSKY_LOOP_WEBP_MAX_DIM", "CARTOSKY_V3_LOOP_WEBP_MAX_DIM", "TWF_V3_LOOP_WEBP_MAX_DIM", default="1600")
)
DEFAULT_LOOP_OUTPUT_ROOT = _env_value(
    "CARTOSKY_LOOP_CACHE_ROOT",
    "CARTOSKY_V3_LOOP_CACHE_ROOT",
    "TWF_V3_LOOP_CACHE_ROOT",
    default="./data/loop_cache",
)


@dataclass
class Job:
    variable: str
    fh: str
    cog_path: Path
    value_cog_path: Path | None
    webp_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pre-generate loop WebP frames from published RGBA COGs")
    parser.add_argument("--model", required=True, help="Model ID (e.g. hrrr)")
    parser.add_argument("--run", required=True, help="Run ID (e.g. 20260223_14z)")
    parser.add_argument("--var", dest="variable", default=None, help="Optional variable filter (e.g. tmp2m)")
    parser.add_argument(
        "--data-root",
        default=_env_value("CARTOSKY_DATA_ROOT", "CARTOSKY_V3_DATA_ROOT", "TWF_V3_DATA_ROOT", default="./data"),
        help="Data root containing published/ (default: env CARTOSKY_DATA_ROOT or ./data)",
    )
    parser.add_argument("--quality", type=int, default=DEFAULT_WEBP_QUALITY, help="WebP quality (default: 82)")
    parser.add_argument(
        "--max-dim",
        type=int,
        default=DEFAULT_WEBP_MAX_DIM,
        help="Max output dimension in px (default: 1600)",
    )
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers (default: 4)")
    parser.add_argument(
        "--output-root",
        default=DEFAULT_LOOP_OUTPUT_ROOT,
        help="Directory to write loop WebP cache (default: env CARTOSKY_LOOP_CACHE_ROOT or ./data/loop_cache)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Regenerate existing .loop.webp files")
    return parser.parse_args()


def discover_jobs(
    run_dir: Path,
    output_root: Path,
    model: str,
    run_id: str,
    variable_filter: str | None,
    overwrite: bool,
) -> list[Job]:
    jobs: list[Job] = []
    if not run_dir.is_dir():
        return jobs

    for var_dir in sorted([p for p in run_dir.iterdir() if p.is_dir()]):
        variable = var_dir.name
        if variable_filter and variable != variable_filter:
            continue

        for cog_path in sorted(var_dir.glob("fh*.rgba.cog.tif")):
            fh = cog_path.name.split(".")[0]
            value_cog_path = var_dir / f"{fh}.val.cog.tif"
            webp_path = output_root / model / run_id / variable / f"{fh}.loop.webp"
            if webp_path.is_file() and not overwrite:
                continue
            jobs.append(
                Job(
                    variable=variable,
                    fh=fh,
                    cog_path=cog_path,
                    value_cog_path=value_cog_path if value_cog_path.is_file() else None,
                    webp_path=webp_path,
                )
            )

    return jobs


def convert_job(job: Job, model_id: str, quality: int, max_dim: int) -> tuple[Job, bool, str | None]:
    try:
        job.webp_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(job.cog_path) as ds:
            src_h = int(ds.height)
            src_w = int(ds.width)
            out_h, out_w, _fixed = compute_loop_output_shape(
                model_id=model_id,
                var_key=job.variable,
                src_h=src_h,
                src_w=src_w,
                max_dim=max_dim,
                fixed_width=max_dim,
            )
            if out_h <= 0 or out_w <= 0:
                return (job, False, "invalid source dimensions")
        use_value_render = use_value_render_for_variable(model_id=model_id, var_key=job.variable)
        resampling = rasterio_resampling_for_loop(model_id=model_id, var_key=job.variable)
        prefer_high_quality_resize = use_value_render and (out_h < src_h or out_w < src_w)
        if prefer_high_quality_resize and resampling != Resampling.nearest:
            resampling = high_quality_loop_resampling()

        if use_value_render and job.value_cog_path is not None:
            color_map_id = variable_color_map_id(model_id, job.variable)
            if not color_map_id:
                return (job, False, "missing color_map_id for value render")
            with rasterio.open(job.value_cog_path) as value_ds:
                sampled_values = value_ds.read(
                    1,
                    out_shape=(out_h, out_w),
                    resampling=resampling,
                ).astype(np.float32, copy=False)
            rgba, _ = float_to_rgba(sampled_values, color_map_id, meta_var_key=job.variable)
            rgba = np.moveaxis(rgba, 0, -1)
        else:
            with rasterio.open(job.cog_path) as ds:
                if resampling == Resampling.nearest:
                    data = ds.read(
                        indexes=(1, 2, 3, 4),
                        out_shape=(4, out_h, out_w),
                        resampling=resampling,
                    )
                else:
                    rgb = ds.read(
                        indexes=(1, 2, 3),
                        out_shape=(3, out_h, out_w),
                        resampling=resampling,
                    )
                    alpha = ds.read(
                        indexes=4,
                        out_shape=(out_h, out_w),
                        resampling=Resampling.nearest,
                    )
                    data = np.concatenate((rgb, alpha[np.newaxis, :, :]), axis=0)
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

    output_root = Path(args.output_root)
    jobs = discover_jobs(
        run_dir=run_dir,
        output_root=output_root,
        model=args.model,
        run_id=args.run,
        variable_filter=args.variable,
        overwrite=args.overwrite,
    )
    if not jobs:
        msg_var = f" variable={args.variable}" if args.variable else ""
        print(f"No conversion jobs found for model={args.model} run={args.run}{msg_var}")
        return 0

    print(
        f"Generating loop WebP frames: model={args.model} run={args.run} "
        f"jobs={len(jobs)} workers={args.workers} quality={args.quality} max_dim={args.max_dim} "
        f"output_root={output_root}"
    )

    ok = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(convert_job, job, args.model, args.quality, args.max_dim) for job in jobs]
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

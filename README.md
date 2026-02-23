# TWF Models V3

## Pre-generate loop WebP frames

To avoid first-play latency in loop mode, pre-generate loop frames after publish:

```bash
PYTHONPATH=backend .venv/bin/python backend/scripts/generate_loop_webp.py \
	--model hrrr \
	--run 20260223_14z \
	--data-root ./data/v3 \
	--output-root /tmp/twf_v3_loop_webp_cache \
	--workers 6
```

Optional flags:

- `--var tmp2m` to process a single variable
- `--overwrite` to regenerate existing `fhNNN.loop.webp`
- `--quality` and `--max-dim` to tune output size/quality

By default loop WebP files are written to `TWF_V3_LOOP_CACHE_ROOT` (or `/tmp/twf_v3_loop_webp_cache`) so production `published/` can remain read-only.

For production scheduler automation, set:

```bash
export TWF_V3_LOOP_PREGENERATE_ENABLED=1
export TWF_V3_LOOP_CACHE_ROOT=/opt/twf_v3/data/v3/loop_cache
export TWF_V3_LOOP_PREGENERATE_WORKERS=4
```

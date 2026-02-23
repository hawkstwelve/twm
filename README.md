# TWF Models V3

## Pre-generate loop WebP frames

To avoid first-play latency in loop mode, pre-generate loop frames after publish:

```bash
PYTHONPATH=backend .venv/bin/python backend/scripts/generate_loop_webp.py \
	--model hrrr \
	--run 20260223_14z \
	--data-root ./data/v3 \
	--workers 6
```

Optional flags:

- `--var tmp2m` to process a single variable
- `--overwrite` to regenerate existing `fhNNN.loop.webp`
- `--quality` and `--max-dim` to tune output size/quality

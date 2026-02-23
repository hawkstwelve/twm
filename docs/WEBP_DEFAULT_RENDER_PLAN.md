# WebP-Default Render Path — Gate-Based Implementation Plan

## Scope

Implement a WebP-first render architecture with two WebP tiers and deterministic fallback to tiles, while keeping sampling authoritative from data artifacts (`val.cog.tif`) in all render modes.

This document is execution-gated (not calendar-based).

## Feature Flag and Rollback

- Single top-level flag: `TWF_V3_WEBP_DEFAULT_ENABLED`.
- Flag **on**: enable state machine routing (`webp_tier0 | webp_tier1 | tiles`), WebP tier manifests, WebP playback engine, and mode telemetry.
- Flag **off**: force current tile-first behavior; app functions without loop endpoints or tier manifests.
- Telemetry for this feature is also gated by the same flag.

## Gate 1 — Single Render State Machine

### Required invariant

Render mode is derived only from one state machine with exactly three states:

- `webp_tier0`
- `webp_tier1`
- `tiles`

No other component performs independent zoom-based routing.

### Routing inputs

- `effectiveZoom = zoom + log2(dpr)` (deterministic DPR-aware rule).
- `Z_TIER0_MAX`, `Z_TIER1_MAX`, hysteresis margin `HYST`, dwell `DWELL_MS`.
- Optional user override: `Interactive/High detail` forces `tiles`.

### Tier selection stability rule

- Tier selection runs only after dwell completes.
- The selected target mode is latched for the current gesture cycle and reused until gesture end.
- Mid-gesture zoom jitter does not cause mode oscillation.

### Initial values

- `Z_TIER0_MAX = 5.8`
- `Z_TIER1_MAX = 6.6`
- `HYST = 0.2`
- `DWELL_MS = 200`

## Gate 2 — Transition Semantics (No Thrash, No Partial Paint)

### Hysteresis + dwell rules

Mode transitions require both:

1. crossing a threshold by `HYST`, and
2. remaining across it for `DWELL_MS` after the last zoom event.

During active pinch/scroll gesture, state machine does not switch modes mid-gesture.

### UI transition behavior (explicit)

When transitioning to `tiles`:

1. Keep current WebP frame visible.
2. Begin loading tiles for current `fh`.
3. Evaluate tile readiness (defined below).
4. Once readiness threshold is met, perform instant layer swap to tiles.
5. Remove/hide WebP layer.

No fade, no blend.

When transitioning from `tiles` to WebP (`webp_tier0` or `webp_tier1`):

1. Keep current tiles visible.
2. Begin preload/decode for target tier at current `fh`.
3. Once target WebP frame is decode-ready, perform instant layer swap to WebP.
4. Remove/hide tiles layer.

No fade, no blend.

### Tiles frame-ready definition

A tiles frame is considered ready when MapLibre signals `idle` for the active weather source/layer set.

Implementation detail:

- Readiness must be bound to the specific active source ID for the current frame selection (`fh`) so unrelated map activity does not trigger early swap.

Until ready, WebP stays on-screen.

## Gate 3 — Tiered WebP Artifacts and Compatibility

### Artifact model

Generate two tiers per `{model, run, var, region, fh}`:

- Tier 0: lower resolution (speed/bandwidth default)
- Tier 1: higher resolution (sharper moderate zoom/high DPR)

Both tiers must share identical spatial extent/projection invariants.

### Manifest contract (versioned)

Add explicit `manifest_version` and include alignment invariants:

- `manifest_version`
- `bbox` (shared invariant)
- `projection` (shared invariant)
- `loop_tiers` array with tier metadata and per-frame URLs

Example shape (illustrative):

```json
{
  "manifest_version": 1,
  "bbox": [-125.0, 24.0, -66.5, 49.5],
  "projection": "EPSG:4326",
  "loop_tiers": [
    {"tier": 0, "width": 960, "height": 540, "frames": [{"fh": 0, "url": "..."}]},
    {"tier": 1, "width": 1920, "height": 1080, "frames": [{"fh": 0, "url": "..."}]}
  ]
}
```

### Tier failover

If Tier 1 is unavailable for selection scope, fail over to Tier 0 (not `tiles`) and continue playback/scrub.

- Emit one failover event per selection cycle (no spam).

## Gate 4 — Atomic WebP Playback and Scrub Correctness

### Atomic readiness

In WebP modes, frame is ready only when decoded into `ImageBitmap` and available for immediate swap.

No partial draw is exposed.

### Latest-wins cancelation

Scrub/decode uses latest-wins tokens:

- In-flight fetch/decode for superseded target is canceled where possible (`AbortController`) or ignored on completion.
- Final displayed frame must match final slider position within one UI tick.

### Decoded cache budget (bytes)

Use LRU keyed by `(tier, fh)` with strict byte budget.

- Evict immediately on insert until `<= budget`.
- Temporary overshoot allowed up to one frame size only.
- Track high-water bytes for telemetry.

Initial defaults:

- Desktop memory cap: `256 MiB`
- Mobile memory cap: `128 MiB`

## Gate 5 — Prefetch, Buffering, and Tiles-Mode Animation Policy

### Predictable prefetch window

In WebP modes:

- Maintain `AHEAD_READY` target (initial: `8`) with bounded concurrency.
- If ahead-ready drops below minimum playable threshold, autoplay pauses and shows buffering state.
- Autoplay resumes automatically when threshold recovers.

### Tiles mode animation policy

Default behavior in `tiles` mode:

- Autoplay disabled unless user explicitly enables it.
- Scrub behavior either:
  - disabled with UX hint (`Zoom out for smooth loop`), or
  - enabled with explicit label (`May be less smooth`) and latest-wins enforcement.

## Gate 6 — Authoritative Sampling API (`val.cog.tif`)

### Contract

Implement one sampling endpoint/service backed by `val.cog.tif` for selected `{model, run, var, fh}`.

Response minimum:

- `value`
- `units`
- `noData` (bool)
- optional `label`, `desc`

### Correctness invariants

Sampling source must match the same timestep/transform used to generate:

- WebP frame for `fh`
- tiles for `fh`

Numeric readouts never use WebP pixel inspection.

### Missing `val.cog.tif` policy (integrity failure)

Treat missing `val.cog.tif` for existing frame as publish integrity error (not normal fallback path):

- deterministic API result (e.g., 404 or structured no-data error),
- explicit error telemetry/event,
- UI degrades gracefully (tooltip remains stable, no app breakage).

## Gate 7 — Sampling Performance, Coalescing, and API Boundary Controls

### Frontend controls

- Latest-wins cancellation for hover/click/scrub sampling calls.
- Deduplicate repeated requests for unchanged target key.

### Backend controls

Use coalescing + short cache first, then light rate controls:

- Request coalescing window per key `(model,run,var,fh,row,col)` for ~`50–150ms`.
- Small in-memory TTL cache for recent sample responses.
- Light guardrails (rate-limit) only as safety net; avoid UX-degrading 429 spam.

Key precision rule:

- `row,col` are computed from the selected `val.cog.tif` geotransform for the request `lat,lon` using the same spatial mapping rule as sampling.
- Coalescing/caching keys are grid-cell deterministic (not screen-pixel dependent), so behavior is consistent across devices and DPR values.

### Performance SLO (initial)

- `p95 <= 150ms` warm-cache
- `p95 <= 400ms` cold-cache

### Spatial semantics (explicit)

Sampling semantics must be deterministic and documented:

- Coordinate transform: `lat/lon (EPSG:4326) -> EPSG:3857` before raster indexing.
- Cell selection: nearest grid cell via dataset index mapping (`row,col` from geotransform), no screen-pixel dependence.
- Interpolation: point sample reads the selected cell value (no bilinear interpolation for numeric tooltip value).
- NoData behavior:
  - out-of-bounds requests return structured payload with `noData=true` and `value=null`;
  - in-bounds NaN cell values return structured payload with `noData=true` and `value=null`.
- Edge behavior: bounds checks use dataset dimensions and never throw for valid API input ranges.

## Gate 8 — URL Versioning, Alignment, and Telemetry

### Cache busting

Loop manifest and frame URLs must include version identity (publish timestamp/run/version hash) so reruns cannot reuse stale cached frames.

Switching runs must always produce distinct URLs.

### Alignment invariants

WebP tiers and tiles must align to same geographic extent/projection.

Mode/tier switches must not introduce visible positional jump; only sharpness/detail changes.

### Telemetry minimums

Emit/log at least:

- mode transitions (`tier0↔tier1↔tiles`)
- WebP readiness/decode timings
- sample request latency
- decoded cache high-water bytes

Decode timing definitions:

- `webp_ready_ms`: fetch start → decode complete (primary UX metric)
- `webp_fetch_ms`: fetch start → fetch complete
- `webp_decode_ms`: fetch complete → decode complete

Use `webp_ready_ms` for p50/p95 reporting; keep `webp_fetch_ms` and `webp_decode_ms` for diagnosis.

### Percentile computation policy (explicit)

Pick one implementation path before coding:

1. **Raw timing logs + offline aggregation** (recommended initial path), or
2. **In-app rolling histogram** that periodically emits p50/p95 aggregates.

Do not claim p50/p95 without one of the above wired.

## Acceptance Validation Matrix

Validate all criteria by gate; do not proceed to default enablement until all pass.

1. State machine exclusivity: only three states, no independent zoom switching.
2. Hysteresis+dwell and no mid-gesture switch confirmed.
3. Deterministic effective zoom routing verified across rerenders, with dwell-latched selection per gesture cycle.
4. Tier1-missing fallback -> Tier0 with one-cycle telemetry.
5. Atomic WebP swaps only after decode-ready.
6. Scrub latest-wins correctness under rapid input.
7. Byte-budget LRU enforcement and immediate eviction behavior.
8. Prefetch/playback pause-resume threshold behavior.
9. Tiles-mode autoplay/scrub policy and UX hinting.
10. Sampling always authoritative/data-backed in every mode.
11. Versioned URLs/manifest prevent stale rerun images.
12. Mode switch preserves geospatial alignment.
13. Telemetry emitted and suppressible via feature flag.
14. Feature flag off fully reverts to tile-first path.
15. Sampling endpoint contract fields and val.cog source.
16. Sampling source invariants across loop/tiles timestep transform.
17. Sampling p95 meets SLO under expected load.
18. Spatial semantics documented (interpolation + NoData + edge behavior).
19. API boundary controls: coalescing/cache + safety rate guardrails.
20. Sampling UX remains available and consistent in all modes.
21. `tiles -> WebP` transition keeps tiles visible until decode-ready WebP frame swap.
22. Tile-readiness swap is gated by source-scoped MapLibre `idle` for current frame selection.
23. Decode telemetry includes `webp_ready_ms`, `webp_fetch_ms`, and `webp_decode_ms` with explicit p50/p95 basis.

## Execution Order

Execute by gates in order:

1. Gate 1–2 (state machine + transition semantics)
2. Gate 3–5 (tier artifacts + playback/cache/policy)
3. Gate 6–7 (sampling correctness + performance controls)
4. Gate 8 (cache-busting/alignment/telemetry)
5. Acceptance matrix run
6. Enable feature flag by default only after matrix pass

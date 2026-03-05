# Kuchera APCP Cumulative-Step Mix Fix — Implementation Spec

**Status:** Draft  **Date:** 2026-03-05  
**Affected code:** `backend/app/services/builder/derive.py` — `_ApcpCumDiffState`, `_resolve_apcp_step_data`, `_cumulative_apcp_loop`  
**Affected tests:** `test_kuchera_apcp_exact.py`, `test_kuchera_apcp_fallback.py` (new file: `test_kuchera_apcp_cumdiff.py`)

---

## 1  Bug Description

`_ApcpCumDiffState.prev_cum` is only updated when a **cumulative 0→N** window is
fetched (section 5 of `_resolve_apcp_step_data`).  Step-mode windows (e.g. `1-2
hour acc`) do **not** advance the state.  When a later step falls back to a
cumulative window, the difference baseline is stale:

```
step 1: fetch 0-1 step  → increment = 0-1      ← prev_cum set to 0-1 grid
step 2: fetch 1-2 step  → increment = 1-2      ← prev_cum NOT updated
step 3: fetch 0-3 cum   → diff = 0-3 − 0-1     ← BUG: yields hours 1–3
                                                    but hours 1–2 already counted
```

Result: hours 1–2 precipitation is double-counted.

---

## 2  Invariants (Must Hold Post-Fix)

| ID | Invariant |
|----|-----------|
| **I1** | **No double-counting.** Each hour of liquid-water-equivalent (LWE) precipitation contributes to exactly one step's increment. |
| **I2** | **Per-step snow = increment × SLR.** `snow_step[i] = apcp_increment[i] * slr[i]`. The SLR of step *j* is never applied to precipitation that belongs to step *k ≠ j*. |
| **I3** | **Deterministic.** For a fixed set of available APCP windows per forecast hour, the output is identical regardless of fetch timing or concurrency. |
| **I4** | **Monotone non-negative.** Each `apcp_increment[i] ≥ 0` (clip negative diffs to 0, matching current behavior). |
| **I5** | **Identity under uniform availability.** If all hours provide step-mode APCP, behavior is unchanged from today. If all hours provide cumulative 0→N, behavior matches consecutive differencing. |

---

## 3  Correct Baseline Logic

### Data Structure Change

Replace the single `prev_cum` / `prev_cum_fh` fields in `_ApcpCumDiffState`
with a **running sum of all step-mode increments** already consumed:

```
@dataclass
class _ApcpCumDiffState:
    # Running sum of step-mode APCP grids applied so far (always in native
    # LWE units, pre-warp or post-warp depending on use_warped).
    consumed_sum: np.ndarray | None = None
    consumed_sum_valid: np.ndarray | None = None
    consumed_sum_crs: rasterio.crs.CRS | None = None
    consumed_sum_transform: rasterio.transform.Affine | None = None
    consumed_through_fh: int = 0       # highest fh whose increment is in consumed_sum
```

### Per-Step Resolution Rules

Given `step_fhs = [1, 2, 3, …, N]`, for each `step_fh`:

1. **Fetch APCP** using existing priority chain (exact-guess → inventory → selector regex).
2. **Classify** the returned window via `_classify_apcp_mode_for_kuchera`.
3. **Compute increment:**

| Case | Condition | Increment formula |
|------|-----------|-------------------|
| **Step-mode** | Window = `(expected_start, step_fh)` | `increment = fetched_data` (direct use). |
| **Cumulative-mode, has consumed_sum** | Window = `(0, step_fh)` and `consumed_sum is not None` | `increment = clip(fetched_data − consumed_sum, 0, ∞)` |
| **Cumulative-mode, no consumed_sum** | Window = `(0, step_fh)` and first step | `increment = fetched_data` (nothing consumed yet). |

4. **Update state:**

```python
# After computing increment for this step:
if consumed_sum is None:
    consumed_sum  = increment.copy()
    consumed_valid = step_valid.copy()
else:
    consumed_sum  = consumed_sum + increment
    consumed_valid = consumed_valid & step_valid
consumed_through_fh = step_fh
```

This means `consumed_sum` always equals the total LWE applied so far (0→step_fh), regardless of whether individual steps used step-mode or cumulative-mode APCP.

### Scenario Walk-Throughs

**A) Steps 0-1, 1-2 available; fh3 only has 0-3:**

| step_fh | fetched | mode | consumed_sum before | increment | consumed_sum after |
|---------|---------|------|--------------------:|----------:|-------------------:|
| 1 | 0-1 = 2.0 | step | — | 2.0 | 2.0 |
| 2 | 1-2 = 3.0 | step | 2.0 | 3.0 | 5.0 |
| 3 | 0-3 = 8.0 | cumulative | 5.0 | clip(8.0 − 5.0) = 3.0 | 8.0 |

Total = 2.0 + 3.0 + 3.0 = 8.0 ✓ (equals cum 0-3)

**B) Steps 0-1, 1-2, 2-3 available; fh4 only has 0-4:**

| step_fh | fetched | mode | consumed_sum before | increment | consumed_sum after |
|---------|---------|------|--------------------:|----------:|-------------------:|
| 1 | 0-1 = 1.0 | step | — | 1.0 | 1.0 |
| 2 | 1-2 = 2.0 | step | 1.0 | 2.0 | 3.0 |
| 3 | 2-3 = 1.5 | step | 3.0 | 1.5 | 4.5 |
| 4 | 0-4 = 7.0 | cumulative | 4.5 | clip(7.0 − 4.5) = 2.5 | 7.0 |

Total = 1.0 + 2.0 + 1.5 + 2.5 = 7.0 ✓

**C) Only cumulative 0-N windows available:**

| step_fh | fetched | mode | consumed_sum before | increment | consumed_sum after |
|---------|---------|------|--------------------:|----------:|-------------------:|
| 1 | 0-1 = 2.0 | cumulative | — | 2.0 | 2.0 |
| 2 | 0-2 = 5.0 | cumulative | 2.0 | 3.0 | 5.0 |
| 3 | 0-3 = 8.0 | cumulative | 5.0 | 3.0 | 8.0 |

Total = 2.0 + 3.0 + 3.0 = 8.0 ✓

**D) Step windows resume after a cumulative fallback:**

| step_fh | fetched | mode | consumed_sum before | increment | consumed_sum after |
|---------|---------|------|--------------------:|----------:|-------------------:|
| 1 | 0-1 = 2.0 | step | — | 2.0 | 2.0 |
| 2 | 0-2 = 5.0 | cumulative | 2.0 | 3.0 | 5.0 |
| 3 | 2-3 = 1.0 | step | 5.0 | 1.0 | 6.0 |
| 4 | 3-4 = 0.5 | step | 6.0 | 0.5 | 6.5 |

Total = 2.0 + 3.0 + 1.0 + 0.5 = 6.5 ✓

---

## 4  Acceptance Test Vectors

All tests use a 2×2 grid. SLR is fixed at 10.0 (use `kuchera_min_levels=99` to force 10:1 fallback so the SLR is deterministic). Units: APCP in kg/m², snowfall output in inches (multiply kg/m² by 0.03937… after applying SLR).

### Test 1 — Mixed step then cumulative (the primary bug)

```
step_fhs = [1, 2, 3]

APCP windows:
  fh1: "0-1 hour acc" → [[2.0, 1.0], [0.5, 0.0]]   (step)
  fh2: "1-2 hour acc" → [[3.0, 2.0], [1.0, 0.0]]   (step)
  fh3: "0-3 hour acc" → [[8.0, 4.0], [2.5, 0.0]]   (cumulative)

Expected increments:
  step 1: [2.0, 1.0, 0.5, 0.0]
  step 2: [3.0, 2.0, 1.0, 0.0]
  step 3: clip([8.0, 4.0, 2.5, 0.0] − [5.0, 3.0, 1.5, 0.0]) = [3.0, 1.0, 1.0, 0.0]

Expected total LWE = [8.0, 4.0, 2.5, 0.0]  (must equal the 0-3 cum)
Expected snowfall (inches) = total_LWE * 10.0 * 0.03937007874015748
```

### Test 2 — All cumulative (consecutive differencing)

```
step_fhs = [1, 2, 3]

APCP windows:
  fh1: "0-1 hour acc" → [[1.0, 1.0], [1.0, 1.0]]   (cumulative — starts at 0)
  fh2: "0-2 hour acc" → [[3.0, 3.0], [3.0, 3.0]]   (cumulative)
  fh3: "0-3 hour acc" → [[6.0, 6.0], [6.0, 6.0]]   (cumulative)

Expected increments: [1.0, 2.0, 3.0] per pixel
Expected total LWE = 6.0 per pixel
Expected snowfall (inches) = 6.0 * 10.0 * 0.03937…
```

### Test 3 — Cumulative then step resumes

```
step_fhs = [1, 2, 3, 4]

APCP windows:
  fh1: "0-1 hour acc" → [[2.0, ...]]   (step)
  fh2: "0-2 hour acc" → [[5.0, ...]]   (cumulative)
  fh3: "2-3 hour acc" → [[1.0, ...]]   (step)
  fh4: "3-4 hour acc" → [[0.5, ...]]   (step)

Expected increments: [2.0, 3.0, 1.0, 0.5]
Expected total = 6.5
```

### Test 4 — Negative diff clipped to zero

```
step_fhs = [1, 2]

APCP windows:
  fh1: "0-1 hour acc" → [[5.0, ...]]   (step/cumulative)
  fh2: "0-2 hour acc" → [[4.0, ...]]   (cumulative — less than fh1 due to rounding)

Expected increment for step 2: clip(4.0 − 5.0, 0) = 0.0
Expected total = 5.0
```

### Test 5 — NaN pixel propagation

```
step_fhs = [1, 2]

APCP windows:
  fh1: "0-1 hour acc" → [[1.0, NaN], [2.0, 3.0]]   (step)
  fh2: "1-2 hour acc" → [[1.0, 1.0], [NaN, 1.0]]   (step)

Expected per-pixel validity:
  pixel (0,0): both valid  → total = 2.0
  pixel (0,1): fh1 invalid → total = NaN
  pixel (1,0): fh2 invalid → total = NaN (valid_mask ORed, see _cumulative_apcp_loop)
  pixel (1,1): both valid  → total = 4.0

NOTE: Existing _cumulative_apcp_loop uses `valid_mask = OR(valid_mask, step_valid)` —
verify this matches the intended "any-step-contributed" semantics for the final NaN
mask. If changed to AND, update this test accordingly.
```

### Test 6 — Single forecast hour (no differencing needed)

```
step_fhs = [1]

APCP windows:
  fh1: "0-1 hour acc" → [[4.5, 2.0], [0.0, 1.0]]

Expected increment = input (no differencing).
Expected total LWE = [4.5, 2.0, 0.0, 1.0]
Expected snowfall (inches) = total * 10.0 * 0.03937…
```

---

## 5  Degraded Frame Semantics

### 5.1  Degradation Flags

A frame is **DEGRADED** if any of the following occurred during its build:

| Flag key | Trigger |
|----------|---------|
| `slr_fallback_10to1` | `kuchera_profile insufficient_levels` — fewer than `min_levels` profile levels available for any step in the accumulation. |
| `apcp_cumulative_fallback` | Any step used cumulative differencing instead of a native step window. |

Both flags are informational; neither blocks publishing.

### 5.2  Sidecar JSON

Add to the frame sidecar JSON (`fhNNN.json`):

```json
{
  "quality": "degraded",
  "quality_flags": ["slr_fallback_10to1", "apcp_cumulative_fallback"]
}
```

When no degradation occurred: `"quality": "full"`, `"quality_flags": []`.

### 5.3  Rebuild Policy

| Condition | Action |
|-----------|--------|
| Frame has `slr_fallback_10to1` | Re-queue on the **next catchup round** that reaches this fh. Rebuild only if the frame artifact still has `"quality": "degraded"` in its sidecar AND the profile levels are now available (check via Herbie precheck before entering the build). |
| Frame has `apcp_cumulative_fallback` only | Do **not** rebuild. The cumulative diff produces correct totals (post-fix). |
| Retry limit | Max **2 rebuild attempts** per frame per run. After 2 failures, accept the degraded frame. Track attempt count in an in-memory dict keyed by `(run_id, var_id, fh)`. |
| Run superseded | If a newer run is detected, abandon all pending rebuilds for the old run. |

### 5.4  Scheduler Integration

In `scheduler.py`'s catchup loop, after the first complete pass:

1. Scan published sidecar files for `"quality": "degraded"` with `"slr_fallback_10to1"`.
2. Collect `(var_id, fh)` pairs that are eligible for rebuild (attempt count < 2).
3. Insert them at the front of the next catchup round's work list.
4. If a rebuild succeeds with `"quality": "full"`, overwrite the existing artifacts.

This adds at most `O(degraded_frames)` extra builds per run and is bounded by the retry cap.

---

## 6  Implementation Checklist

### derive.py changes

- [ ] Replace `_ApcpCumDiffState` fields with `consumed_sum`, `consumed_sum_valid`, `consumed_sum_crs`, `consumed_sum_transform`, `consumed_through_fh`.
- [ ] In `_resolve_apcp_step_data`, after computing `step_apcp_data`:
  - **Always** update `consumed_sum += increment` (for both step and cumulative modes).
  - For cumulative mode: `increment = clip(fetched − consumed_sum_before, 0)`.
  - For step mode: `increment = fetched_data` (no change from today).
- [ ] Remove the section-5 conditional that only updates `prev_cum` on `window[0] == 0`.
- [ ] Return a `bool` flag alongside the step data indicating whether cumulative fallback was used (for sidecar degradation tagging).
- [ ] Thread a `fallback_used: bool` accumulator through `_cumulative_apcp_loop` → `_derive_snowfall_kuchera_total_cumulative` and surface it to `build_frame`.

### pipeline.py changes

- [ ] Accept quality flags from the derive path.
- [ ] Write `quality` and `quality_flags` into the sidecar JSON.

### scheduler.py changes

- [ ] After first catchup pass, scan for degraded sidecars.
- [ ] Re-queue eligible frames with attempt-count tracking.
- [ ] Cap rebuilds at 2 per `(run_id, var_id, fh)`.

### Code-Review Regression Checklist

- [ ] **Masking:** `consumed_sum` must use `consumed_valid & step_valid` (AND) for the validity mask used in differencing; the final NaN mask in `_cumulative_apcp_loop` remains OR (any-contribution semantics).
- [ ] **NaN propagation:** `np.where(valid, data, 0.0)` before accumulation — verify no NaN leaks into `consumed_sum`.
- [ ] **Unit conversion:** SLR multiplication happens *per-step* before accumulation; the `* 0.03937…` to-inches conversion happens *once* at the end. Verify no double-conversion.
- [ ] **Cumulative differencing:** `clip(fetched − consumed_sum, 0)` — confirm the clip target is `0.0` not `None` or `NaN`.
- [ ] **Grid alignment:** Assert `consumed_sum.shape == fetched.shape` and matching CRS/transform before differencing. Raise on mismatch (do not silently skip).
- [ ] **Concurrency:** `_ApcpCumDiffState` is created per-call in `_cumulative_apcp_loop`, never shared across threads. Verify `build_frame_bundle` does not mutate it from multiple threads.
- [ ] **dtype:** All intermediate arrays must remain `float32`. Verify no promotion to `float64` during `consumed_sum + increment`.
- [ ] **Backward compatibility:** Existing `test_kuchera_apcp_fallback.py` tests for all-cumulative GFS scenarios must still pass. The GFS 6-hourly case (`0-6`, `0-12`) should produce identical results with the new state model.

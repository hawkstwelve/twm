# CONUS Canonical Rewrite Plan

Date: 2026-02-22  
Status: Approved implementation plan (breaking cutover)

## Objective

Move from region-scoped datasets to a single canonical CONUS dataset per frame identity:

- Canonical identity: `{model, run, var, fh}`
- Canonical artifacts:
	- `fhXXX.rgba.cog.tif` (CONUS)
	- `fhXXX.val.cog.tif` (CONUS, 4× downsample for hover)
- Regions become UI/view presets only (viewport + prefetch constraints), not data identity.

## Decisions (Locked)

1. **Immediate breaking cutover** — no parallel generation/storage of legacy region products.
2. **CONUS-only canonical builds** for every model supported on-site.
3. **Backend-owned region registry** as single source of truth, exposed via `/api/regions`.
4. **Manifest-first backend discovery** (authoritative, deterministic); filesystem scan may remain only as debug/admin fallback.

## Non-negotiable implementation safeguards

These are required to avoid production regressions during the cutover.

1. **Grid/warp invariants must remain stable across runs/vars**
	- Canonical output CRS: `EPSG:3857`
	- Canonical extent: CONUS bbox constants from `cog_writer.py`
	- Resolution policy: per-model fixed CONUS target resolution (existing `TARGET_GRID_METERS` policy)
	- Alignment: target-aligned pixels (equivalent to `-tap`) must remain enabled
	- No per-run/per-var drift in transform/shape for a given model+coverage

2. **Atomic publish semantics must guarantee consistency**
	- API must never advertise a run until run directory + manifest + `LATEST.json` are all committed
	- Publish sequence must use temp path + atomic rename/replace semantics
	- Manifest-first APIs should only read committed run state (no half-published visibility)

3. **Breaking-contract deploy must be one coordinated window**
	- Backend route break + frontend identity break deploy together
	- Do not leave backend/frontend on mixed contracts between releases

4. **Out-of-bounds behavior must be quiet and bounded**
	- OOB tiles remain `204` (not treated as errors)
	- Frontend must not create retry storms for repeated 204 responses
	- Logging should avoid noisy error-level events for expected OOB responses

## Why this change

Today, region is baked into pipeline identity and path layout, causing product multiplication if more regions are added.
This rewrite eliminates duplicate compute/storage and simplifies scheduling + API contracts while preserving region UX in the frontend.

---

## Current Coupling Map (to remove)

### Builder / Scheduler / Storage Identity

- Region-scoped staging + published + manifests paths are built in:
	- `backend/app/services/scheduler.py`
		- `_frame_sidecar_path`, `_frame_rgba_path`, `_frame_value_path`
		- `_promote_run`, `_write_latest_pointer`, `_write_run_manifest`
- Builder writes region-scoped output dirs in:
	- `backend/app/services/builder/pipeline.py` (`build_frame`, `staging_dir`)

### API Discovery + Sampling + Contours

- Region in endpoint shape and lookup paths in:
	- `backend/app/main.py`
		- `/api/v3/{model}/regions`
		- `/api/v3/{model}/{region}/runs`
		- `/api/v3/{model}/{region}/{run}/vars`
		- `/api/v3/{model}/{region}/{run}/{var}/frames`
		- `/api/v3/sample?region=...`
		- `/api/v3/{model}/{region}/{run}/{var}/{fh}/contours/{key}`

### Tile Serving

- Region in tile URL and COG resolution in:
	- `backend/app/services/tile_server.py`
		- `/tiles/v3/{model}/{region}/{run}/{var}/{fh}/{z}/{x}/{y}.png`
		- `_resolve_cog_path(model, region, ...)`

### Frontend Identity Coupling

- Region included in all discovery and data requests in:
	- `frontend/models-v3/src/lib/api.ts`
- Region included in fallback tile URL in:
	- `frontend/models-v3/src/lib/tiles.ts`
- Region change currently triggers dataset discovery/refetch cascade in:
	- `frontend/models-v3/src/App.tsx`
- Region view is hardcoded (`REGION_VIEWS`) in:
	- `frontend/models-v3/src/components/map-canvas.tsx`

---

## Target Contracts

## 1) Canonical filesystem layout (no region dimension)

```text
data/v3/
├── staging/
│   └── {model}/{run}/{var}/
│       ├── fh000.rgba.cog.tif
│       ├── fh000.val.cog.tif
│       ├── fh000.json
│       └── ...
├── published/
│   └── {model}/
│       ├── LATEST.json
│       └── {run}/{var}/...
└── manifests/
		└── {model}/{run}.json
```

## 2) Canonical API shapes (breaking)

### Discovery (manifest-first)

- `GET /api/v3/models`
- `GET /api/v3/{model}/runs`
- `GET /api/v3/{model}/{run}/vars`
- `GET /api/v3/{model}/{run}/manifest`
- `GET /api/v3/{model}/{run}/{var}/frames`

### Regions metadata

- `GET /api/regions`
	- backend-owned presets (bbox, defaultCenter, defaultZoom, optional min/max zoom, label)
	- ETag + Cache-Control for frontend local cache validation

### Sampling / contours / tiles

- `GET /api/v3/sample?model={m}&run={r}&var={v}&fh={fh}&lat={lat}&lon={lon}`
- `GET /api/v3/{model}/{run}/{var}/{fh}/contours/{key}`
- `GET /tiles/v3/{model}/{run}/{var}/{fh}/{z}/{x}/{y}.png`

### Runtime behavior requirements

- OOB tile/sample responses are expected control flow (`204`) and must not trigger error retries.
- Frontend prefetch logic should treat `204` as terminal for that request window/frame.

## 3) Manifest schema (authoritative)

Run manifest remains the primary source for run/var/frame discovery:

```json
{
	"contract_version": "3.0",
	"model": "hrrr",
	"run": "20260222_00z",
	"variables": {
		"tmp2m": {
			"kind": "continuous",
			"units": "°F",
			"expected_frames": 49,
			"available_frames": 49,
			"frames": [{"fh": 0, "valid_time": "2026-02-22T00:00:00Z"}]
		}
	},
	"last_updated": "2026-02-22T01:00:00Z"
}
```

`LATEST.json` remains per-model (not per-model/region).

---

## Backend region registry

Create a single backend source of truth, e.g. `backend/app/config/regions.py` (or JSON/YAML), example:

```json
{
	"pnw": {
		"label": "Pacific Northwest",
		"bbox": [-126.0, 41.5, -116.0, 49.5],
		"defaultCenter": [-120.8, 45.6],
		"defaultZoom": 6,
		"minZoom": 3,
		"maxZoom": 10
	},
	"north_central": {
		"label": "North Central",
		"bbox": [-106.0, 40.0, -92.0, 50.0],
		"defaultCenter": [-99.0, 45.0],
		"defaultZoom": 6
	}
}
```

The frontend consumes `/api/regions`; region data is no longer hardcoded in TS.

---

## Implementation Phases

## Phase 1 — Canonical backend identity

1. Remove region from scheduler path builders/promote/latest/manifest write paths.
2. Update builder output directories to canonical `{model}/{run}/{var}`.
3. Keep warping/output bounds fixed to CONUS for supported models.
4. Ensure scheduler runtime defaults/CLI/systemd target CONUS-only operation.
5. Add grid-invariant assertion checks:
	- for each built frame, validate transform + shape match canonical model CONUS grid
	- fail build if drift is detected

### Key files

- `backend/app/services/scheduler.py`
- `backend/app/services/builder/pipeline.py`
- `backend/app/services/builder/cog_writer.py`
- `deployment/systemd/twf-v3-scheduler.service`
- `backend/app/models/hrrr.py` (must include conus in supported regions/spec)

## Phase 2 — Manifest-first API

1. Add manifest read/cache layer in API service.
2. Replace list endpoints to read from manifest (runs/vars/frames).
3. Keep filesystem scanning only under explicit debug/admin path.
4. Move latest resolution to `published/{model}/LATEST.json`.
5. Enforce commit visibility rule: only committed manifests/runs are returned by discovery endpoints.

### Key file

- `backend/app/main.py`

## Phase 3 — Tile/sample/contour contract break

1. Change tile route shape and resolver to canonical non-region identity.
2. Remove region query coupling from sample route.
3. Remove region path coupling from contours route.
4. Confirm OOB responses remain `204` and are logged at non-error level.

### Key files

- `backend/app/services/tile_server.py`
- `backend/app/main.py`

## Phase 4 — Regions metadata API

1. Add backend registry module + `/api/regions` endpoint.
2. Add ETag/Cache-Control for low-cost metadata polling.

### Key files

- `backend/app/main.py`
- `backend/app/config/*` (new)

## Phase 5 — Frontend decoupling

1. Consume `/api/regions` to populate dropdown.
2. Cache regions metadata via localStorage + ETag conditional requests.
3. Remove region from all discovery, tile, sample, contour identity requests.
4. Region change updates viewport + prefetch bounds only.
5. Keep default UX region = PNW preset.

### Key files

- `frontend/models-v3/src/lib/api.ts`
- `frontend/models-v3/src/lib/tiles.ts`
- `frontend/models-v3/src/App.tsx`
- `frontend/models-v3/src/components/map-canvas.tsx`
- `frontend/models-v3/src/lib/config.ts`

## Phase 6 — Data migration + cleanup

1. Build/publish canonical CONUS run(s).
2. Verify API + frontend against canonical paths only.
3. Delete old region-scoped trees in `data/v3/published/*/*/...` and `data/v3/manifests/*/*...`.
4. Update docs to remove regioned contracts.

### Cutover deploy requirement

- PR2 + PR4 + PR6 must be deployed in a single coordinated release window to avoid contract stranding.
- If any critical check fails during window, rollback both backend and frontend to prior compatible versions.

---

## tmp2m contour requirement

Requirement: keep smooth contours while hover value COG remains downsampled.

Current status: already aligned with requirement.

- Contours are generated from full-resolution in-memory warped values (`warped_data`) before value downsample persistence.
- Hover COG remains downsampled 4× (`VALUE_HOVER_DOWNSAMPLE_FACTOR = 4`).

No algorithmic change required; only path/identity routing changes are needed.

---

## Validation checklist (acceptance)

## Build + artifacts

- [ ] Exactly one CONUS artifact set exists per `{model, run, var, fh}`.
- [ ] No region directories are created in staging/published/manifests.
- [ ] Sidecars and manifest entries resolve correctly for every available frame.

## API behavior

- [ ] Discovery endpoints return data without region dimension.
- [ ] API discovery is manifest-driven (deterministic, not hot-path filesystem scan).
- [ ] `/api/regions` returns presets with ETag and cache headers.
- [ ] Sample endpoint works against canonical CONUS `val.cog.tif`.
- [ ] Contour endpoint returns tmp2m contours from canonical frame metadata.

## Tile behavior

- [ ] Tiles serve from canonical CONUS COGs at `/tiles/v3/{model}/{run}/{var}/{fh}/{z}/{x}/{y}.png`.
- [ ] Out-of-bounds tile requests return 204 (no region-specific assumptions).

## Frontend behavior

- [ ] Default region preset is PNW (same initial feel).
- [ ] Switching region changes viewport/prefetch behavior only.
- [ ] Dataset identity (run/var/frame URLs) does not change with region selection.
- [ ] Animation + hover remain functional.

## tmp2m quality

- [ ] tmp2m contours remain smooth (full-res contour source retained).
- [ ] Hover sampling remains fast via downsampled value COG.

---

## Risks and mitigations

1. **Breaking API route migration risk**
	 - Mitigation: update backend + frontend in same deploy unit; no dual schema.

2. **Manifest authority drift** (manifest out of sync with files)
	 - Mitigation: manifest write occurs in scheduler publish transaction; add startup/runtime manifest integrity checks.

3. **Scheduler/plugin region assumptions**
	 - Mitigation: ensure all supported model plugins include/normalize CONUS target and scheduler defaults reflect CONUS-only mode.

4. **Frontend stale region config cache**
	 - Mitigation: ETag conditional fetch + fallback to cached payload only when network fails.

5. **Pixel alignment drift across runs/vars**
	- Mitigation: explicit invariant checks on transform/shape per model CONUS grid; reject drift at build validation.

6. **Half-published run visibility**
	- Mitigation: strict atomic publish ordering and discovery read constraints to committed state only.

7. **OOB retry/log storms**
	- Mitigation: treat `204` as expected behavior in tile server and frontend request handling; suppress noisy error logs.

---

## Out of scope

- Dual-write or compatibility layer for legacy region-based product trees.
- Keeping region in canonical data identity.
- New data products beyond this identity migration.

---

## Primary references

- `backend/app/services/builder/cog_writer.py`
- `backend/app/services/builder/pipeline.py`
- `backend/app/services/scheduler.py`
- `backend/app/services/tile_server.py`
- `backend/app/main.py`
- `backend/app/models/hrrr.py`
- `frontend/models-v3/src/lib/api.ts`
- `frontend/models-v3/src/lib/tiles.ts`
- `frontend/models-v3/src/App.tsx`
- `frontend/models-v3/src/components/map-canvas.tsx`
- `deployment/systemd/twf-v3-scheduler.service`
- `docs/ROADMAP_V3.md`

---

## PR-by-PR execution breakdown

This sequence is optimized for an **immediate breaking cutover** with minimal ambiguity.
Each PR has a narrow purpose, explicit blast radius, and a clear rollback checkpoint.

## PR 1 — Canonical path/identity primitives (backend core)

**Goal**

Introduce canonical (no-region) path builders and identity helpers in backend services.

**Scope**

- Add/replace canonical path helpers in scheduler + API/tile resolver internals.
- Keep old routes untouched in this PR; focus only on internal path primitives.

**Likely files**

- `backend/app/services/scheduler.py`
- `backend/app/main.py`
- `backend/app/services/tile_server.py`

**Blast radius**

- Medium (internal path plumbing only).

**Validation**

- Unit/functional checks for helper outputs and latest-pointer resolution paths.

**Rollback checkpoint**

- Safe rollback if no route/CLI contract changes merged yet.

## PR 2 — Builder + scheduler CONUS canonical output cutover

**Goal**

Make frame build/publish/manifests write to canonical non-region layout and force CONUS target grid.

**Scope**

- Builder staging paths change from `{model}/{region}/{run}/{var}` to `{model}/{run}/{var}`.
- Scheduler promote/latest/manifest writers adopt canonical layout.
- Scheduler runtime defaults/systemd move to CONUS-only operation.
- Ensure model plugin region support includes conus where needed.
- Add and enforce grid-invariant checks (CRS/extent/resolution/alignment stability).

**Likely files**

- `backend/app/services/builder/pipeline.py`
- `backend/app/services/scheduler.py`
- `backend/app/models/hrrr.py`
- `deployment/systemd/twf-v3-scheduler.service`

**Blast radius**

- High (artifact location + publish semantics).

**Validation**

- Build one frame and one short run into canonical layout.
- Confirm `published/{model}/LATEST.json` and `manifests/{model}/{run}.json` are written.
- Verify transform/shape invariants are identical across at least two vars and multiple FHs in same run.
- Run:
  - `PYTHONPATH=backend .venv/bin/python -m app.services.builder.pipeline ...`
  - `.venv/bin/python backend/scripts/test_pipeline.py`
  - `.venv/bin/python backend/scripts/test_cog_write.py`

**Rollback checkpoint**

- If deploy is not yet switched to new API/tile routes, revert this PR and remove canonical test data.

## PR 3 — Manifest-first API discovery (authoritative)

**Goal**

Switch discovery endpoints to manifest-backed reads, not hot-path directory scans.

**Scope**

- Implement manifest read/cache layer.
- Rework runs/vars/frames/manifest endpoints to canonical route shape.
- Keep scan fallback only behind explicit debug/admin endpoint.
- Gate latest/run discoverability to committed publish state only.

**Likely files**

- `backend/app/main.py`

**Blast radius**

- High (API contract + data source semantics).

**Validation**

- Verify deterministic API responses from manifest even under partial filesystem churn.
- Verify `latest` resolution uses model-level pointer.
- Ensure cache headers/ETag behavior remains sane.

**Rollback checkpoint**

- Revert endpoint layer if frontend is not migrated yet.

## PR 4 — Tile/sample/contour contract break to canonical identity

**Goal**

Remove region from tile, sample, and contour identity paths and lookups.

**Scope**

- Tile route to `/tiles/v3/{model}/{run}/{var}/{fh}/{z}/{x}/{y}.png`.
- Sample endpoint removes `region` query coupling.
- Contour endpoint removes region path dimension.
- Harden expected OOB `204` handling with low-noise logging.

**Likely files**

- `backend/app/services/tile_server.py`
- `backend/app/main.py`

**Blast radius**

- High (public route break).

**Validation**

- Tile 200 for in-bounds requests; 204 for outside-bounds requests.
- Sample/contour responses succeed for canonical run/var/fh.
- tmp2m contour remains available and smooth.
- Verify no retry storm behavior from API clients when receiving repeated 204s.

**Rollback checkpoint**

- Revert this PR if frontend rollout is not simultaneous.

## PR 5 — Backend regions registry + `/api/regions`

**Goal**

Introduce backend-owned region preset metadata API.

**Scope**

- Add single source-of-truth regions config.
- Add `/api/regions` endpoint with ETag + caching headers.

**Likely files**

- `backend/app/config/regions.py` (or JSON/YAML equivalent)
- `backend/app/main.py`

**Blast radius**

- Low/medium (additive API endpoint).

**Validation**

- Endpoint returns presets with expected schema.
- ETag conditional requests return 304 when unchanged.

**Rollback checkpoint**

- Low risk; endpoint can remain even if frontend not yet switched.

## PR 6 — Frontend canonical identity migration

**Goal**

Remove region from dataset identity in frontend and consume backend regions metadata.

**Scope**

- Discovery/data requests use canonical backend routes (no region in identity).
- Tile/sample/contour URL builders updated.
- Region dropdown sourced from `/api/regions`.
- Region selection updates viewport + prefetch bounds only.
- Add localStorage + ETag caching for regions metadata.
- Ensure 204 tile/sample responses do not trigger noisy retries.

**Likely files**

- `frontend/models-v3/src/lib/api.ts`
- `frontend/models-v3/src/lib/tiles.ts`
- `frontend/models-v3/src/App.tsx`
- `frontend/models-v3/src/components/map-canvas.tsx`
- `frontend/models-v3/src/lib/config.ts`

**Blast radius**

- High (core app data-flow).

**Validation**

- `npm run build` succeeds.
- Switching region no longer causes run/var/frame identity changes.
- Default load still “feels” PNW.
- Animation and tooltip sampling still work.

**Rollback checkpoint**

- Revert frontend bundle only if backend still has previous contract in same deployment window.

## PR 7 — Deployment/doc cutover + old data cleanup

**Goal**

Finalize rollout and remove region-scoped artifacts/contracts.

**Scope**

- Update service/env/docs to canonical contract.
- Delete old regioned published/manifests after verification.
- Retain optional debug/admin scan fallback only.

**Likely files**

- `deployment/systemd/*`
- `docs/ROADMAP_V3.md`
- `docs/ARTIFACT_CONTRACT.md`

**Blast radius**

- Medium (ops + docs + storage cleanup).

**Validation**

- End-to-end smoke with latest run + region switching behavior.
- Disk tree contains only canonical non-region artifacts.

**Rollback checkpoint**

- Data cleanup step should be last; do not delete legacy trees until frontend/backend canonical path checks are green.

---

## Recommended merge/deploy choreography

1. Merge PR1 → PR2 → PR3 → PR4 on backend branch.
2. Merge PR5 (regions metadata) before frontend switch.
3. Prepare PR6 and stage backend/frontend artifacts for one release window.
4. Deploy **PR2+PR4+PR6 contract set together** (single coordinated window).
5. Run acceptance checklist immediately.
6. If any critical check fails, rollback backend+frontend together.
7. Merge PR7 cleanup/docs and delete old regioned data only after green validation.

---

## Fast acceptance smoke script (post-PR6)

1. Build/publish one canonical run for default model.
2. Confirm:
	- discovery endpoints return canonical data
	- tiles render for multiple FHs
	- sampling returns values from canonical `val.cog`
	- tmp2m contours load
3. Open UI:
	- default region = PNW preset
	- changing region updates viewport only
	- run/var/frame identity remains unchanged
4. Verify no region dimension exists under `data/v3/published` and `data/v3/manifests`.

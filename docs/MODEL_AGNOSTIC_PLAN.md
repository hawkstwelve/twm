# Model-Agnostic Expansion Plan (Backend + Frontend)

## Summary
This plan makes model behavior metadata-driven across backend and frontend so adding a new model is fast and low-risk.

Decisions locked:
- Compatibility approach: controlled breaking cleanup
- Source of truth: plugin-first model metadata
- Frontend integration: new backend capabilities endpoints
- Deliverable shape: single document with embedded execution checklist

Target outcome:
- Adding a model should not require hardcoded updates in scheduler/API/frontend constants.
- New model onboarding should be mostly: plugin definition plus optional colormap or derive registration.
- Frontend initialization should use one bootstrap capability call, not per-model N+1 requests.

## Version Lock (API)
This refactor is an explicit API-version transition:
1. Introduce `/api/v4/*` as the model-agnostic contract.
2. Migrate frontend and scheduler integrations to `/api/v4/*` in the same refactor window.
3. Deprecate and remove previous-version runtime endpoints within the same refactor stream (no long-lived dual-contract maintenance).

## Goals
1. Eliminate backend model-id branching outside model plugins and registry.
2. Eliminate frontend hardcoded model and variable mappings.
3. Introduce explicit model capability contracts consumed by scheduler, API, and frontend.
4. Keep build, tile, and sample behavior stable while improving extensibility.

## Non-Goals
1. No redesign of artifact contract (`rgba.cog`, `val.cog`, sidecar).
2. No change to tile URL shape or sampling endpoint semantics.
3. No major frontend UX redesign.

## Current Coupling to Remove
1. Scheduler hardcodes per-model run heuristics and special rules.
2. API hardcodes model labels and variable ordering.
3. Builder conversion and grid logic are split across global maps and model files.
4. Frontend hardcodes defaults, labels, variable priority, and model behavior.

## Variable Identity Guarantee
Variable identity is always:
`(model_id, var_key)`

Rules:
1. No global variable-id lookups are allowed for build, API, or frontend bootstrap behavior.
2. Colormap, units, derive metadata, and frontend labels must resolve through model-scoped variable identity.
3. Any cache keys or registries that reference variables must include `model_id`.

## Palette and Variable Metadata Boundary
`backend/app/services/colormaps.py` should contain palette data and LUT-building logic only, keyed by `color_map_id`.

Examples:
1. `temp_f_-60_120_tmp2m` -> anchors and range
2. `wind_mph_0_100` -> anchors and range
3. `radar_ptype_v1` -> indexed palette configuration

Model plugins (or a model-aware var registry) should contain:
1. `(model_id, var_key)` -> units
2. `(model_id, var_key)` -> legend title and display metadata
3. `(model_id, var_key)` -> conversion key
4. `(model_id, var_key)` -> `color_map_id`

Runtime rule:
1. Plugin capabilities are authoritative for model-variable metadata.
2. `VAR_SPECS` fallback paths are retired from runtime codepaths.
3. `colormaps.py` remains the palette and LUT builder plus palette data store.

## Target Architecture

### 1) Plugin-First Capability Contract
Define a typed capability model in `backend/app/models/base.py` and require each plugin to expose it.

Required capability domains:
1. Model identity and product:
   - `id`, `name`, `product`
2. Run discovery policy:
   - probe var, lookback and cycle policy, fallback behavior
3. Scheduling policy:
   - target FHs by cycle, per-var min and max FH rules
4. Grid policy:
   - canonical region, grid meters by region
5. Variable catalog:
   - `var_key`, display label, kind, units, primary and derived, derive key, selectors
6. Frontend hints:
   - variable order, default variable, per-var default forecast hour, constraints

Frontend hints rule (strict):
1. Frontend hints may only express defaults and constraints.
2. Frontend hints may not include styling or UX tuning parameters.
3. UI look-and-feel changes must remain frontend-owned and deployable without backend changes.

### 2) Scheduler and Builder Become Capability Consumers
Scheduler and builder resolve behavior from plugin capabilities only.
No direct `if model == "hrrr"` or `if model == "gfs"` logic outside plugins.

### 3) Derive Strategy Interface
Introduce a formal derive-strategy registry and reference model derivations by strategy ID.

Derive strategy contract:
- `id`
- `required_inputs`
- `output_var_key`
- `execute` function

Rules:
1. Plugins reference `derive_strategy_id` only.
2. Plugins do not embed inline derive execution logic.
3. Builder dispatches by registered strategy ID and validates required inputs before execution.

### 4) API Becomes Capability-Aware
Add explicit capability endpoints and remove hardcoded ordering and name maps in API service.

### 5) Frontend Bootstraps From Capabilities
Frontend loads defaults, labels, order, and constraints from a single bootstrap capabilities endpoint rather than static constants.

## Public Interface Changes

### Primary Bootstrap Endpoint
`GET /api/v4/capabilities`

Purpose:
- Single frontend bootstrap payload (no N+1 model capability fetching during app init).

Response shape:
1. `supported_models` (from registry):
   - list of supported model IDs
2. `model_catalog` (from registry):
   - full model entries with variable catalogs and defaults
3. `availability` (from disk/manifests):
   - per-model published state, including `published_runs` and `latest_run`
4. `contract_version`:
   - capability schema version for client validation

Response invariants:
1. `supported_models` must equal `Object.keys(model_catalog)` (derived field).

### Optional Per-Model Endpoint
`GET /api/v4/models/{model}/capabilities`

Response includes:
1. Model metadata:
   - id, name, canonical region, default variable, constraints
2. Variable metadata:
   - `var_key`, `display_name`, `kind`, `units`, `order`, `default_fh`, `buildable`
3. Model defaults and constraints only

Notes:
1. Per-model endpoint is optional and primarily for debug or focused tooling.
2. Frontend should use `/api/v4/capabilities` as the default bootstrap source.

### Capabilities schema v1 (JSON example)
```json
{
  "contract_version": "v1",
  "supported_models": ["hrrr", "gfs"],
  "model_catalog": {
    "hrrr": {
      "model_id": "hrrr",
      "name": "HRRR",
      "defaults": {
        "default_var_key": "tmp2m"
      },
      "variables": {
        "tmp2m": {
          "var_key": "tmp2m",
          "display_name": "2m Temperature",
          "kind": "continuous",
          "units": "F",
          "default_fh": 1,
          "buildable": true,
          "color_map_id": "temp_f_-60_120_tmp2m",
          "constraints": {
            "min_fh": 0,
            "max_fh": 48
          }
        }
      }
    }
  },
  "availability": {
    "hrrr": {
      "latest_run": "20260226_12z",
      "published_runs": ["20260226_12z", "20260226_06z"]
    },
    "gfs": {
      "latest_run": null,
      "published_runs": []
    }
  }
}
```

### Existing Endpoint Behavior Changes
1. `GET /api/v4/models`:
   - source: plugin registry (supported models), not artifact availability.
2. `GET /api/v4/{model}/{run}/vars`:
   - ordered and labeled using capabilities, intersected with manifest availability.
3. Previous-version runtime endpoints are deprecated and removed during this refactor sequence after `/api/v4/*` consumers are switched.
4. API contract must explicitly separate:
   - supported model existence (`supported_models`, registry) and detailed model metadata (`model_catalog`)
   - published data availability (`published_runs`, `latest_run`, disk/manifests)

## Implementation Phases

## Phase 1: Capability Contract Foundation
Files:
- `backend/app/models/base.py`
- `backend/app/models/hrrr.py`
- `backend/app/models/gfs.py`
- `backend/app/models/registry.py`

Work:
1. Add typed capability structures in `base.py`.
2. Refactor HRRR and GFS plugins to define capabilities as source of truth.
3. Add registry helpers to fetch plugin capabilities.
4. Add `color_map_id` to model-scoped var metadata and resolve palettes through that ID.

## Phase 2: Backend Decoupling
Files:
- `backend/app/services/scheduler.py`
- `backend/app/services/builder/pipeline.py`
- `backend/app/services/builder/fetch.py`
- `backend/app/services/builder/cog_writer.py`
- `backend/app/services/builder/derive.py`
- `backend/app/services/colormaps.py`

Work:
1. Move run resolution policy into plugin methods and capabilities.
2. Replace scheduler special-case per-var rules with capability metadata.
3. Make unit conversion lookup model-plus-var aware via capability metadata.
4. Move grid resolution ownership to plugin capabilities.
5. Replace derive `if` and `elif` branch chain with derive-handler registry keyed by derive strategy ID.
6. Enforce variable identity lookups via `(model_id, var_key)` at all builder and scheduler call sites.
7. Refactor builder colorization inputs to resolve palette by `color_map_id` from plugin var metadata.
8. Remove runtime dependence on `VAR_SPECS` and require plugin capability metadata.

## Phase 3: API Capability Surface
Files:
- `backend/app/main.py`

Work:
1. Add `GET /api/v4/capabilities` as the primary bootstrap endpoint.
2. Keep `GET /api/v4/models/{model}/capabilities` as optional debug-focused endpoint.
3. Remove hardcoded model names and var order maps in favor of capability lookup.
4. Make `/vars` return capability labels and order with manifest intersection.
5. Add `/api/v4/*` route set and remove previous-version route usage in active consumers before retirement.
6. Ensure response model explicitly separates `supported_models` and `model_catalog` from `availability`.
7. Enforce invariant: `supported_models == Object.keys(model_catalog)`.

## Phase 4: Frontend Migration
Files:
- `frontend/src/lib/api.ts`
- `frontend/src/lib/config.ts`
- `frontend/src/App.tsx`
- `frontend/src/components/map-canvas.tsx`

Work:
1. Add capabilities API client for `GET /api/v4/capabilities`.
2. Remove static variable and model constants and priority arrays.
3. Bootstrap defaults, order, labels, and constraints from single bootstrap payload.
4. Avoid per-model capability fetch at startup; use per-model endpoint only for diagnostics.
5. Replace model-specific render branching with frontend-owned logic constrained by capability defaults and constraints.

## Phase 4.5: Runtime API Cutover
Files:
- `backend/app/main.py`
- `frontend/src/lib/api.ts`
- `frontend/src/App.tsx`
- `frontend/src/lib/use-sample-tooltip.ts`
- `docs/NGINX_V3.md`

Work:
1. Add `/api/v4/*` runtime endpoints covering the full runtime contract:
   - `/api/v4/{model}/runs`
   - `/api/v4/{model}/{run}/manifest`
   - `/api/v4/{model}/{run}/vars`
   - `/api/v4/{model}/{run}/{var}/frames`
   - `/api/v4/{model}/{run}/{var}/loop-manifest`
   - `/api/v4/{model}/{run}/{var}/{fh:int}/loop.webp`
   - `/api/v4/sample`
   - `/api/v4/{model}/{run}/{var}/{fh:int}/contours/{key}`
2. Migrate frontend runtime calls from previous-version endpoints to `/api/v4/*` for runs, manifests, vars, frames, sample, loop-manifest, loop-webp, and contours.
3. Keep `/tiles/v3/*` unchanged for this phase (tile contract is out of scope for API-version cutover).
4. Update edge routing so `api.theweathermodels.com` proxies `/api/v4/*` with the same CORS/header behavior used for runtime APIs.
5. Run production smoke tests on one model/var/run/FH across all runtime endpoints.
6. Retire previous-version runtime endpoints in the same refactor stream (no long-lived dual runtime contract).

## Phase 5: Guardrails and Docs
Files:
- `backend/tests/*`
- `docs/ROADMAP_V3.md`
- `docs/MODEL_AGNOSTIC_PLAN.md`

Work:
1. Add contract and regression tests.
2. Document onboarding steps and invariants.

## Testing Plan

### Backend tests
1. Plugin capability schema validation for all registered models.
2. Scheduler uses plugin run policy and var scheduling metadata.
3. API bootstrap capabilities endpoint schema and content validation.
4. `/vars` ordering and labels reflect capabilities and manifest intersection.
5. Variable lookup tests prove `(model_id, var_key)` identity enforcement.
6. Derive strategy registry tests validate required inputs and strategy dispatch by ID.
7. Existing frame cache behavior unchanged for retained semantics.
8. Availability contract tests validate clear distinction between `supported_models` and published run state.
9. Capability schema tests validate `model_catalog` structure, `var_key` usage, and `supported_models == Object.keys(model_catalog)`.
10. Legacy fallback tests validate there is no runtime metadata fallback path outside plugin capabilities.
11. Palette resolution tests validate `(model_id, var_key)` -> `color_map_id` -> palette behavior.

### Frontend tests and smoke
1. App initializes using a single `GET /api/v4/capabilities` call.
2. Model switch updates vars and defaults without static fallback maps.
3. Labels and order match backend response.
4. No N+1 per-model capability requests occur during initial app bootstrap.
5. Existing frame loading and loop playback still work.

### Runtime cutover smoke (Phase 4.5 gate)
1. Frontend runtime network calls resolve via `/api/v4/*` (except `/tiles/v3/*`).
2. `GET /api/v4/capabilities` succeeds from public edge (not just localhost).
3. One-model runtime checks pass for runs, manifest, vars, frames, sample, and loop-manifest on `/api/v4/*`.
4. No empty-selector runtime requests are emitted (for example `model=&var=&fh=Infinity`).
5. Previous-version runtime paths return 404 and no frontend runtime calls target legacy API routes.

### New model acceptance scenario
1. Add synthetic plugin with one simple variable.
2. No frontend code edits.
3. Model appears in `supported_models` from `/api/v4/capabilities` and UI dropdown.
4. Model details and variable definitions appear under `model_catalog` with `var_key` entries.
5. Published availability fields are correctly empty before first run, then populated after publish.
6. Scheduler resolves run and FHs from plugin policy and builds a frame.

## Risks and Mitigations
1. Risk: capability schema drift across plugins.
   - Mitigation: strict schema validator test plus registry load validation at startup.
2. Risk: frontend boot depends on capability availability.
   - Mitigation: explicit error handling and fallback messaging for missing capability payload.
3. Risk: migration churn touches cross-cutting files.
   - Mitigation: phase-gated rollout with test gate per phase.

## Acceptance Criteria
1. No model-specific branch logic remains in scheduler, API, or frontend bootstrap paths.
2. Frontend bootstrap is driven by `GET /api/v4/capabilities` in one request.
3. Frontend runtime API calls use `/api/v4/*` (tiles remain `/tiles/v3/*`).
4. Variable identity is enforced everywhere as `(model_id, var_key)` with no global var lookup paths.
5. Derive execution uses registered strategy IDs, not inline plugin logic.
6. Previous-version runtime API is removed or formally retired in the same refactor stream after `/api/v4/*` cutover.
7. API explicitly distinguishes supported model existence from published data availability.
8. `colormaps.py` is palette-focused (`color_map_id` keyed) and variable metadata is model-scoped.
9. Runtime metadata fallback through `VAR_SPECS` is retired.
10. Existing HRRR and GFS behavior remains functionally equivalent for data loading and rendering.

## Assumptions
1. Canonical coverage remains `conus`.
2. Sidecar and manifest contract remains authoritative for frame availability.
3. Controlled breaking cleanup is acceptable for capability and discovery surfaces.

## Embedded Execution Checklist
1. Define capability types in model base.
2. Migrate HRRR plugin to capability-first.
3. Migrate GFS plugin to capability-first.
4. Refactor scheduler to consume plugin policy only.
5. Introduce derive strategy interface and registry keyed by strategy ID.
6. Refactor builder conversion, grid, and derive lookups to `(model_id, var_key)`.
7. Add model-scoped `color_map_id` mapping and route palette lookup through `colormaps.py`.
8. Remove runtime dependence on `VAR_SPECS` and require plugin capability metadata.
9. Add `GET /api/v4/capabilities` bootstrap endpoint plus optional per-model endpoint.
10. Implement explicit `supported_models`, `model_catalog`, and availability (`published_runs`, `latest_run`) contract.
11. Add and lock a capabilities schema v1 JSON example in docs.
12. Migrate frontend bootstrap from legacy endpoints to single-call `/api/v4/capabilities`.
13. Add `/api/v4` runtime endpoint parity for runs, manifest, vars, frames, sample, loops, and contours.
14. Migrate frontend runtime API calls from previous-version endpoints to `/api/v4`.
15. Validate production edge routing for `/api/v4/*` and run v4 runtime smoke tests.
16. Remove previous-version routes in the same refactor stream.
17. Add backend capability, identity, derive-registry, schema, and palette-resolution tests.
18. Run backend tests and frontend smoke checks.
19. Update roadmap references.

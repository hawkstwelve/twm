# ROADMAP V3 — Pre-Styled RGBA COG Architecture

## Overview

V3 replaces the current split-system architecture (webp offline images in `twf_models` + runtime TiTiler colorization in `twf_models_legacy`) with a unified pipeline:

**GRIB → normalize/derive → colorize to RGBA → COG → dumb tile server → MapLibre**

Plus a parallel float32 value-grid artifact for hover/sampling.

This eliminates runtime science logic from the tile-serving path, makes adding variables a single-spec change, and enables CONUS-wide zoomable, animatable, hover-for-data weather maps.

### Why V3 over the current systems

| Problem | V2 (webp offline) | V2 (TiTiler legacy) | V3 |
|---|---|---|---|
| Zoom/pan quality | Fixed zoom levels, no pan beyond precomputed tiles | Full zoom but runtime colorization causes bugs | Full zoom, pre-styled, no runtime colormap logic |
| Hover for values | Not possible (pixels are pre-rendered) | Not implemented | First-class `/sample` endpoint from float32 COG |
| Adding variables | Touches multiple files across 2 repos | Touches 3+ registries + build_cog branches | Single variable spec + build step |
| Break/fix rate | Low (simple pipeline) | High (LUT/band-count branching, overview quirks) | Low (RGBA in = RGBA out, no transformation) |
| Animation | Works but limited zoom | Works well (double-buffer exists) | Same double-buffer, better tile quality |

### Supersedes

- [OFFLINE_TILES_REFACTOR_PLAN.md](OFFLINE_TILES_REFACTOR_PLAN.md) — PMTiles + z≤7 cap is incompatible with CONUS-wide zoomable goals
- [ROADMAP.md](ROADMAP.md) — P0/P1 items are already complete; P2/P3 fold into V3

---

## Non-Goals for V3

These are explicitly out of scope. If a proposed change falls into this list, it is rejected unless this section is formally amended with justification.

- **No vector tiles.** Raster-only. Vector tile complexity is not justified for gridded weather data.
- **No server-side dynamic colormap switching.** The colormap is baked into the RGBA artifact at build time. There is no query parameter, header, or endpoint that selects a different palette at serve time.
- **No user-selectable color ramps.** Users see the palette the builder chose. Frontend does not offer a "change colors" control. If a palette needs updating, it is a builder change and a new run publish.
- **No partial-run incremental styling.** A frame's RGBA artifact is built once and never re-rendered. If the colormap spec changes, it takes effect on the next run, not retroactively on already-published frames.
- **No runtime derivation.** The tile server and sampling API never compute derived quantities (e.g., wind speed from u/v, heat index from temp/humidity). All derivation happens in the builder.
- **No multi-band or multi-variable compositing at serve time.** The tile server serves exactly one pre-built RGBA artifact per request. Layer compositing is the frontend's job (MapLibre handles this natively).
- **No server-side rendering of legends, labels, or annotations.** Legends are rendered client-side from sidecar JSON metadata.

---

## Repository Consolidation

### Current state (two repos, two prod directories)

```
/opt/twf_models          (github.com/hawkstwelve/twf_models)
├── frontend (sodakweather.com/models-v2, webp method, "Legacy tiles" checkbox)
├── backend (webp offline image pipeline)
└── data/v2/             (COGs for TiTiler + webp images)

/opt/twf_legacy          (github.com/hawkstwelve/twf_models_legacy)
├── backend_v2/          (TiTiler service, schedulers, build_cog, model plugins)
├── frontend_v2/         (MapLibre frontend consumed by twf_models)
└── docs/
```

### Target state (single repo, single prod directory)

```
github.com/hawkstwelve/twf_models_v3

/opt/twf_v3/
├── backend/
│   ├── app/
│   │   ├── api/              (FastAPI discovery + sampling endpoints)
│   │   ├── models/           (model plugins: hrrr.py, gfs.py, etc.)
│   │   └── services/
│   │       ├── builder/      (modular build pipeline — replaces monolithic build_cog.py)
│   │       │   ├── fetch.py          (GRIB acquisition via Herbie + unit conversion)
│   │       │   ├── derive.py         (variable derivation: passthrough, wspd hypot, ptype argmax/blend)
│   │       │   ├── colorize.py       (float → RGBA using var spec)
│   │       │   ├── cog_writer.py     (GeoTIFF → warp → overviews (gdaladdo subprocess) → COG (gdal_translate))
│   │       │   └── pipeline.py       (orchestrator: fetch → warp → colorize → write → validate + sidecar JSON)
│   │       ├── scheduler.py          (model_scheduler, run promotion, retention)
│   │       ├── colormaps.py          (VAR_SPECS: encoding ranges, colors, legend config)
│   │       ├── discovery.py          (manifest-based discovery)
│   │       └── tile_server.py        (dumb RGBA COG → PNG, ~100 lines)
│   ├── scripts/              (CLI tools, validation, debug)
│   └── tests/
├── frontend/
│   └── models-v3/           (sodakweather.com/models-v3)
│       └── src/
│           ├── components/   (map-canvas, toolbar, legend, forecast-controls)
│           └── lib/          (config, api, tiles — adds /sample client)
├── deployment/
│   └── systemd/             (v3 scheduler services)
├── data/
│   └── v3/
│       ├── staging/         (build writes here first)
│       └── published/       (atomic promotion from staging)
└── docs/
```

### Consolidation procedure (do once, during Phase 0)

There are no other users. Nothing needs to run side-by-side. V2 is torn down immediately and all work moves to V3.

#### 1. GitHub

```bash
# Create the single V3 repo
git init twf_models_v3
cd twf_models_v3
git remote add origin git@github.com:hawkstwelve/twf_models_v3.git
```

Do NOT fork or branch from either existing repo. Start clean with the target directory structure and selectively copy files (see "What Gets Carried Forward" section). History from the old repos is preserved in their archived state.

After V3 repo has its first working commit:
- Archive `hawkstwelve/twf_models` on GitHub (Settings → Archive)
- Archive `hawkstwelve/twf_models_legacy` on GitHub (Settings → Archive)

One repo. No forks. No submodules. No monorepo tooling needed at this scale.

#### 2. Production server

```bash
# Stop all V2 services
sudo systemctl stop twf-hrrr-v2-scheduler twf-gfs-v2-scheduler
sudo systemctl disable twf-hrrr-v2-scheduler twf-gfs-v2-scheduler
# Stop any running TiTiler or API processes on ports 8099/8101

# Remove V2 directories (or move to /opt/twf_archive/ if you want a safety net)
sudo mv /opt/twf_models /opt/twf_archive/twf_models_$(date +%Y%m%d)
sudo mv /opt/twf_legacy /opt/twf_archive/twf_legacy_$(date +%Y%m%d)

# Clone V3
cd /opt
git clone git@github.com:hawkstwelve/twf_models_v3.git twf_v3

# Create data directories
mkdir -p /opt/twf_v3/data/v3/{staging,published,manifests}
mkdir -p /opt/twf_v3/herbie_cache

# Python environment
cd /opt/twf_v3
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt

# Frontend build
cd /opt/twf_v3/frontend/models-v3
npm install && npm run build

# Install systemd services
sudo cp /opt/twf_v3/deployment/systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable twf-v3-hrrr-conus-scheduler twf-v3-gfs-conus-scheduler twf-v3-tile-server twf-v3-api
sudo systemctl start twf-v3-tile-server twf-v3-api
# Start schedulers once builder is validated (Phase 1 checkpoint)

# Update nginx
# Replace /models-v2, /api/v2, /tiles/v2 blocks with V3 equivalents (see Nginx section below)
sudo nginx -t && sudo systemctl reload nginx
```

After 30 days with no issues, delete `/opt/twf_archive/`.

#### 3. Local development machine

```bash
# Clone
cd ~/projects  # or wherever you keep repos
git clone git@github.com:hawkstwelve/twf_models_v3.git
cd twf_models_v3

# Python environment
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
pip install -r backend/requirements-dev.txt   # pytest, ruff, etc.

# Frontend
cd frontend/models-v3
npm install

# Local data directory (gitignored)
mkdir -p data/v3/{staging,published,manifests}
mkdir -p herbie_cache
```

**Local dev workflow:**

```bash
# Terminal 1: API server (discovery + sampling)
cd ~/projects/twf_models_v3
source .venv/bin/activate
TWF_V3_DATA_ROOT=./data/v3 uvicorn backend.app.main:app --reload --port 8200

# Terminal 2: Tile server
TWF_V3_DATA_ROOT=./data/v3 uvicorn backend.app.services.tile_server:app --reload --port 8201

# Terminal 3: Frontend dev server
cd frontend/models-v3
npm run dev   # Vite on localhost:5173

# Terminal 4: Run a test build (one frame)
source .venv/bin/activate
python -m backend.app.services.builder.pipeline --model hrrr --region pnw --var tmp2m --fh 0 --data-root ./data/v3
```

**Frontend config for local dev** (`frontend/models-v3/src/lib/config.ts`):

```typescript
const isLocal = window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1";
export const API_BASE = isLocal ? "http://127.0.0.1:8200/api/v3" : "https://api.sodakweather.com/api/v3";
export const TILES_BASE = isLocal ? "http://127.0.0.1:8201" : "https://api.sodakweather.com";
```

**Cleanup on local machine:**

```bash
# Remove old repo clones once V3 is your working repo
rm -rf ~/path/to/twf_models_legacy
rm -rf ~/path/to/twf_models
```

#### 4. Resulting state after consolidation

| What | Before | After |
|---|---|---|
| GitHub repos | `twf_models` + `twf_models_legacy` (active) | `twf_models_v3` (active); old repos archived |
| Prod directories | `/opt/twf_models` + `/opt/twf_legacy` | `/opt/twf_v3` |
| Local clones | 2 repos, unclear which has authoritative code | 1 repo |
| Systemd services | `twf-hrrr-v2-scheduler`, `twf-gfs-v2-scheduler` | `twf-v3-*` services |
| Ports | 8099 (API), 8101 (tiles) | 8200 (API), 8201 (tiles) |
| Frontend route | `/models-v2` | `/models-v3` (or `/models` once ready) |
| Data | `/opt/twf_models/data/v2/` | `/opt/twf_v3/data/v3/` |

---

## System Components

### A. Build Pipeline (batch, offline)

**What it does:** GRIB → normalize → derive → colorize → RGBA COG + float32 value COG + sidecar JSON

**Runs as:** systemd services per model (same pattern as current `twf-hrrr-v2-scheduler.service`)

**Key change from V2:** Colorization happens here, not at serve time. The `encode_to_byte_and_alpha()` + `get_lut()` chain from `colormaps.py` merges into a single `float_to_rgba()` step in `builder/colorize.py`.

**Derivation paths (carried forward from build_cog.py):**

| Type | Variables | Logic |
|---|---|---|
| Simple | tmp2m, refc | fetch → open → colorize |
| Vector magnitude | wspd10m | fetch u+v → `np.hypot()` → colorize |
| Categorical combo | radar_ptype (HRRR) | fetch refc + 4 ptype masks → argmax → colorize |
| Categorical blend | precip_ptype (GFS) | fetch PRATE + 4 ptype masks → blend → colorize |

Each path produces two artifacts:
- `fh{NNN}.rgba.cog.tif` — 4-band uint8 RGBA, EPSG:3857, 512×512 internal tiles, internal overviews
- `fh{NNN}.val.cog.tif` — single-band float32, same CRS/grid, explicit nodata

### B. Tile Server (online, stateless)

**What it does:** Reads pre-styled RGBA COGs, returns `{z}/{x}/{y}.png` tiles

**What it does NOT do:** No colormap logic, no LUT lookup, no band-count branching, no science

**Implementation:** ~100 lines of FastAPI + rio-tiler. Reads 4 bands, returns PNG. That's it.

#### Hard Rule: No Runtime Transformation

> **The tile server is forbidden from applying any variable-dependent transformation.**

The tile server MUST NOT perform:
- Scaling or value remapping
- Masking or alpha manipulation
- Colormap application or LUT lookup
- Band reordering or selection
- Nodata remapping or substitution
- Any `if var == "..."` branching

The tile server MUST ONLY:
- Resolve the COG path from URL parameters
- Read 4-band RGBA via `COGReader.tile()`
- Encode to PNG
- Return with cache headers

The `var` parameter in the URL exists only for path resolution — it is never used to change rendering behavior. If a code review finds any `var`-dependent conditional in the tile server, it is a bug.

This is the single rule that prevents regression into the V2 break/fix pattern. All variable-specific logic lives in the builder. The tile server is a dumb pipe.

**Current TiTiler pain eliminated:**
- `_image_data_to_rgba()` with its 4 band-configuration branches → deleted
- `get_lut()` import and per-variable LUT application → deleted
- precip_ptype index-shifting special case → deleted
- Single-band vs 2-band vs 4-band detection → deleted

### C. Sampling API (online, new)

**What it does:** Point query against value-grid COGs for hover-for-data

```
GET /api/v3/sample?model=hrrr&region=conus&run=latest&var=tmp2m&fh=3&lat=47.6&lon=-122.3

Response:
{
  "value": 42.5,
  "units": "°F",
  "model": "hrrr",
  "var": "tmp2m",
  "fh": 3,
  "valid_time": "2026-02-17T09:00:00Z",
  "lat": 47.6,
  "lon": -122.3
}
```

**Implementation:** FastAPI endpoint, uses `rasterio` point query on `fh{NNN}.val.cog.tif`. ~50 lines.

### D. Frontend (sodakweather.com/models-v3)

**Carried forward from current frontend (already implemented):**
- Double-buffer overlay swap (buffer A/B) with micro-crossfade
- 4-layer prefetch for upcoming frames
- Per-variable resampling (linear for radar/ptype, nearest for others)
- Discovery chain: models → regions → runs → vars → frames
- Autoplay with readiness-aware advancement
- Dark toolbar, legend component (gradient + discrete + ptype segmented)

**New:**
- Hover handler calls `/api/v3/sample` on mousemove → shows tooltip with numeric value + units (see "Sampling Cost Control" below)
- Reads from V3 tile URLs and manifests
- No "Legacy tiles" checkbox — V3 is the only rendering path
- CONUS as default region option

#### Sampling Cost Control

Hover sampling can self-DOS the API if every pixel of mouse movement fires a request. These rules are defined once and not deferred.

**Debounce:** 150ms trailing debounce on `mousemove`. No request fires until the cursor has been stationary for 150ms. This alone reduces request volume by ~90% during active mouse movement.

**Request coalescing (drop stale):** Each debounced sample call sets a generation counter. When the response arrives, if the generation has advanced (cursor moved again), the response is silently discarded — no tooltip update, no wasted render. Implementation:

```typescript
let sampleGen = 0;

async function onHover(lat: number, lon: number) {
  const gen = ++sampleGen;
  const result = await fetchSample({ model, region, run, var: varId, fh, lat, lon });
  if (gen !== sampleGen) return; // stale — cursor already moved
  showTooltip(result);
}
```

**Client-side cache:** LRU cache keyed by `${model}/${region}/${run}/${var}/${fh}/${roundedLat}/${roundedLon}`, where lat/lon are rounded to 2 decimal places (~1.1 km precision — well within any model grid cell). Cache capacity: 256 entries. TTL: lifetime of the current run selection (cache is cleared on run/var/model change).

**Tooltip hide:** Tooltip disappears immediately on `mouseleave` from the map container. No lingering stale values.

**Server-side guard (belt-and-suspenders):** The `/api/v3/sample` endpoint does not need rate limiting for a single-user project, but if it ever becomes public, add a simple per-IP rate limit (e.g., 10 req/s) via nginx `limit_req`.

---

## Artifact Contract

### RGBA tile artifact (per model/region/run/var/fh)

| Property | Value |
|---|---|
| CRS | EPSG:3857 |
| Bands | 4 (R, G, B, A), uint8 |
| Alpha | 0 = nodata/outside; 255 = valid data |
| Internal tiling | 512×512 |
| Overviews | Internal (see locked overview strategy below) |
| Filename | `fh{NNN}.rgba.cog.tif` |

#### Locked Overview Strategy

Overview resampling is defined exactly twice — once for continuous, once for categorical — and encoded in `VarSpec.kind`. There are no per-variable overrides, no special cases, no hacks.

| `VarSpec.kind` | Bands 1–3 (RGB) | Band 4 (Alpha) | `gdaladdo` flags |
|---|---|---|---|
| `continuous` | `average` | `nearest` | `-r average` for RGB; alpha band overview built with `nearest` via separate pass or `--config GDAL_TIFF_OVR_BLOCKSIZE 512` |
| `discrete` | `nearest` | `nearest` | `-r nearest` for all bands |

**Rules:**

1. The builder reads `VarSpec.kind` and selects the corresponding row. No other input affects overview resampling.
2. There is no `overview_resampling` field on `VarSpec`. The two strategies above are the only ones that exist.
3. If a variable produces visually incorrect overviews, the fix is to change its `kind` classification (continuous ↔ discrete), not to add a per-variable override.
4. Alpha is always `nearest` — never averaged, interpolated, or thresholded. This is non-negotiable.
5. This strategy is locked at Phase 0 and not revisited unless there is a measured, reproducible visual defect with evidence attached.

This eliminates the historical pain of per-variable overview hacks and the 3-fallback cascade in the current `run_gdaladdo_overviews()` function.

### Value-grid artifact (per model/region/run/var/fh)

| Property | Value |
|---|---|
| CRS | EPSG:3857 (same grid as RGBA) |
| Bands | 1, float32 |
| Nodata | Explicitly set (NaN or sentinel per var spec) |
| Overviews | Internal, nearest resampling |
| Filename | `fh{NNN}.val.cog.tif` |

### Sidecar metadata (per frame)

Filename: `fh{NNN}.json`

```json
{
  "contract_version": "3.0",
  "model": "hrrr",
  "region": "conus",
  "run": "20260217_06z",
  "var": "tmp2m",
  "fh": 3,
  "valid_time": "2026-02-17T09:00:00Z",
  "units": "°F",
  "kind": "continuous",
  "min": -40.0,
  "max": 122.5,
  "legend": {
    "type": "gradient",
    "stops": [[-40, "#7f00ff"], [0, "#0000ff"], [32, "#00ffff"], [70, "#ffff00"], [100, "#ff0000"], [122.5, "#8b0000"]]
  }
}
```

For categorical variables, `legend.type` is `"discrete"` with named category stops.

---

## Builder Correctness Gates

Every frame artifact must pass validation before it is written to staging. If any gate fails, the frame is skipped (logged as a build error) and not promoted. This is the "fail fast" mechanism that prevents broken artifacts from reaching the tile server.

### Gate 1: `gdalinfo` structural validation

After the COG is written, run `gdalinfo -json` and assert:

| Check | RGBA COG | Value COG |
|---|---|---|
| Band count | 4 | 1 |
| Band type | Byte (uint8) | Float32 |
| CRS | EPSG:3857 | EPSG:3857 |
| Internal tiling | 512×512 | 512×512 |
| Overviews present | ≥1 level | ≥1 level |
| Extent matches region bbox | ±1 pixel tolerance | ±1 pixel tolerance |
| Pixel size matches model grid | exact match | exact match |

Implementation: a `validate_cog(path, expected_bands, expected_dtype, region_bbox, grid_meters)` function in `builder/pipeline.py`. ~40 lines. Calls `gdalinfo -json` via subprocess and checks the parsed output.

### Gate 2: Pixel statistics sanity check

Read band statistics from the built COG and assert:

| Check | Rule | Catches |
|---|---|---|
| Alpha coverage | >5% of pixels have alpha=255 | All-transparent frames (empty data, bad mask) |
| Alpha coverage | <99.9% of pixels have alpha=255 (for regional clips) | Missing nodata masking |
| RGB not constant | At least 2 distinct values in each RGB band | Solid-color frames (colormap bug, constant input) |
| Value COG range | min ≠ max, and within `VarSpec.value_range` ± 20% | Flat fields, unit conversion errors |
| Nodata ratio | <95% of value COG pixels are nodata | Grid misalignment, empty fetch |

Thresholds are intentionally loose — the goal is to catch catastrophic failures (all-black, all-transparent, constant-value), not fine-tune quality. If a gate fires, the artifact is broken in an obvious way.

### Gate 3: Golden test per derivation path

The test suite includes one known-good input → expected output test for each derivation type:

| Derivation | Test |
|---|---|
| `simple` (tmp2m) | Fixed 5×5 float32 grid → expected RGBA pixels (spot-check 4 corners + center) |
| `wspd` (wspd10m) | Fixed u=3, v=4 grid → expected wspd=5 → expected RGBA |
| `radar_ptype` | Fixed refc + ptype masks → expected winner-takes-all category → expected RGBA |
| `precip_ptype` | Fixed PRATE + ptype masks → expected blend index → expected RGBA |

These run in CI (or `pytest` locally) and catch regressions in the colorize/derive chain without needing real GRIB data. They are not run per-build — they are run per-code-change.

### Gate enforcement

In `builder/pipeline.py`:

```python
rgba_path = write_rgba_cog(...)
val_path = write_value_cog(...)

if not validate_cog(rgba_path, expected_bands=4, expected_dtype="Byte", ...):
    logger.error("RGBA COG validation failed for %s — skipping frame", rgba_path)
    rgba_path.unlink()
    val_path.unlink()
    return None  # frame not promoted

if not validate_cog(val_path, expected_bands=1, expected_dtype="Float32", ...):
    logger.error("Value COG validation failed for %s — skipping frame", val_path)
    rgba_path.unlink()
    val_path.unlink()
    return None

if not check_pixel_sanity(rgba_path, val_path, var_spec):
    logger.error("Pixel sanity check failed for %s — skipping frame", rgba_path)
    rgba_path.unlink()
    val_path.unlink()
    return None

# Only now: move to staging, update manifest
```

---

## Directory Layout (published)

```
/opt/twf_v3/data/v3/
├── staging/
│   └── {model}/{region}/{run}/{var}/
│       ├── fh000.rgba.cog.tif
│       ├── fh000.val.cog.tif
│       ├── fh000.json
│       └── ...
├── published/
│   └── {model}/{region}/
│       ├── LATEST.json
│       └── {run}/
│           └── {var}/
│               ├── fh000.rgba.cog.tif
│               ├── fh000.val.cog.tif
│               ├── fh000.json
│               ├── fh003.rgba.cog.tif
│               └── ...
└── manifests/
    └── {model}/{region}/{run}.json
```

### LATEST.json

```json
{
  "run_id": "20260217_06z",
  "cycle_utc": "2026-02-17T06:00:00Z",
  "updated_utc": "2026-02-17T07:45:00Z",
  "source": "scheduler_v3"
}
```

Atomic write via `tmp → rename` (same pattern as current V2).

### Run manifest (`{run}.json`)

```json
{
  "contract_version": "3.0",
  "model": "hrrr",
  "region": "conus",
  "run": "20260217_06z",
  "variables": {
    "tmp2m": {
      "kind": "continuous",
      "units": "°F",
      "expected_frames": 49,
      "available_frames": 49,
      "frames": [
        {"fh": 0, "valid_time": "2026-02-17T06:00:00Z"},
        {"fh": 1, "valid_time": "2026-02-17T07:00:00Z"}
      ]
    }
  },
  "last_updated": "2026-02-17T07:45:00Z"
}
```

---

## URL Contracts

### Tiles

```
GET /tiles/v3/{model}/{region}/{run}/{var}/{fh}/{z}/{x}/{y}.png
```

Same pattern as current V2 tiles — frontend wiring is minimal change.

### Discovery API

```
GET /api/v3/models
GET /api/v3/{model}/regions
GET /api/v3/{model}/{region}/runs
GET /api/v3/{model}/{region}/{run}/manifest
GET /api/v3/{model}/{region}/{run}/{var}/frames
```

### Sampling API

```
GET /api/v3/sample?model={model}&region={region}&run={run}&var={var}&fh={fh}&lat={lat}&lon={lon}
```

---

## Caching Strategy

| Resource | Cache-Control | Rationale |
|---|---|---|
| Tile 200 | `public, max-age=31536000, immutable` | Run-scoped URL = content never changes |
| Tile 204/404 | `public, max-age=15` | Avoid lock-in on missing frames |
| LATEST.json | `public, max-age=60` | Short TTL, polled for new runs |
| Run manifest | `public, max-age=31536000, immutable` | Immutable once published |
| Sidecar JSON | `public, max-age=31536000, immutable` | Immutable per run/fh |

Already matches current V2 caching headers — no change needed.

---

## Variable Definition Unification

### Current state (fragmented across 3 systems)

| System | File | Defines |
|---|---|---|
| Model plugins | `models/hrrr.py`, `models/gfs.py` | `VarSpec`: GRIB selectors, derivation type, component hints |
| Colormap specs | `services/colormaps.py` | `VAR_SPECS`: encoding range, colors, units, kind |
| Legacy registry | `services/variable_registry.py` | `VARIABLE_ALIASES`, `VARIABLE_SELECTORS`, `HERBIE_SEARCH` |

### Target state (single source of truth)

Extend `VarSpec` in the model plugin to include colormap/encoding fields:

```python
@dataclass
class VarSpec:
    # Fetch
    var_id: str
    selectors: VarSelectors
    derivation: str                   # "simple" | "wspd" | "radar_ptype" | "precip_ptype"

    # Encoding + display
    kind: str                         # "continuous" | "discrete"
    units: str
    value_range: tuple[float, float]  # for continuous: min/max of encoding range
    colors: list[str]                 # hex color stops
    legend_type: str                  # "gradient" | "discrete" | "segmented"

    # Optional
    levels: list[float] | None = None              # for discrete: threshold levels
    frontend_resampling: str = "nearest"            # "nearest" | "linear"
    component_hints: dict | None = None             # for multi-component derivation
    # NOTE: No overview_resampling field. Overview strategy is locked by `kind`:
    #   continuous → average (RGB) + nearest (alpha)
    #   discrete  → nearest (all bands)
    # See "Locked Overview Strategy" section. No per-variable overrides.
```

Adding a new variable becomes:

1. Add a `VarSpec` entry to the model plugin (one place — covers fetch, derivation, encoding, display)
2. Builder automatically produces `rgba.cog` + `val.cog` using the spec
3. Manifest auto-updates
4. Frontend auto-discovers via API — no frontend code changes needed

`variable_registry.py` is deprecated and removed.

---

## Phased Rollout

### Phase 0 — Repo Setup + Contract Lock (1–2 days) ✅ COMPLETE

**Objective:** Create the V3 repo, tear down V2 entirely, lock the artifact contract.

**Completed:** 2026-02-17

**Steps:**

1. ✅ **Run the consolidation procedure** (see "Consolidation procedure" section above) — creates repo, stops V2 services, sets up prod and local dev
2. ✅ Copy model plugins (`hrrr.py`, `gfs.py`, `base.py`, `registry.py`) from archived `twf_models_legacy`
3. ✅ Copy colormap specs from `colormaps_v2.py` → renamed to `colormaps.py` — begin unifying into extended `VarSpec`
4. ✅ Copy frontend from `frontend_v2/models-v2/` → adapt config for V3 API/tile URLs
5. ✅ Commit artifact contract (this document's "Artifact Contract" section)
6. ✅ Verify: 512 tileSize (already decided), float32 COG for value grid (confirmed), overview rules locked by `VarSpec.kind`
7. ✅ Archive `twf_models` and `twf_models_legacy` repos on GitHub

**Additional completions beyond Phase 0 scope:**
- ✅ Nginx routing switched to `/models-v3`, `/api/v3/`, `/tiles/v3/`
- ✅ V2 endpoints return 410 Gone
- ✅ Ports migrated (8200 API / 8201 tiles)
- ✅ systemd services deployed: `twf-v3-api`, `twf-v3-tile-server`
- ✅ API health endpoint live (`/api/v3/health`)
- ✅ Tile server health endpoint live (`/tiles/v3/health`)
- ✅ Tile server enforces "Hard Rule: No Runtime Transformation" — no colormap logic, no var-branching
- ✅ Frontend builds against V3 routes exclusively (V2 URL leakage fixed)

**Checkpoint:** ~~Single repo exists on GitHub, cloned to prod and local machine. V2 is stopped. Frontend skeleton loads (no data pipeline yet).~~ **PASSED.**

### Phase 1 — One Model + One Variable End-to-End (2–4 days) 🚧 IN PROGRESS

**Objective:** HRRR tmp2m producing RGBA + value COGs, served via dumb tile server, rendered in frontend, with hover sampling.

**Status:** Builder complete. First artifacts passing all gates on prod. Tile server + sample API implemented.

**Steps:**

1. ✅ Implement `builder/pipeline.py` — orchestrates fetch → derive → colorize → write for the simple (tmp2m) path only
2. ✅ Implement `builder/colorize.py` — `float_to_rgba()` merging `encode_to_byte_and_alpha()` + `get_lut()` into one step
3. ✅ Implement `builder/value_grid.py` — `write_value_cog()` lives in `cog_writer.py` (float32 single-band COG alongside RGBA)
4. ✅ Implement `builder/cog_writer.py` — GeoTIFF write + warp + overviews (gdaladdo subprocess) + COG (gdal_translate)
5. ✅ Write HRRR tmp2m artifacts to staging — `gdalinfo` confirms contract-compliant RGBA + value COGs with correct CRS, bands, overviews
6. ✅ Implement dumb tile server (~100 lines): read 4-band RGBA COG via rio-tiler → PNG tile with immutable cache headers
7. ✅ Implement `/api/v3/sample` endpoint (~50 lines): read float32 COG → point query → JSON with units from sidecar
8. ✅ Wire frontend to V3 tile URL and add hover tooltip calling `/sample` — *debounce + generation counter + LRU cache per sampling cost control spec*
9. ❌ Validate: tiles return 200 at z2–z10; hover returns correct temperature values

**Checkpoint:** Single variable works end-to-end. Tiles are crisp at all zooms. Hover shows real values.

### Phase 2 — Expand Variables for HRRR (3–5 days)

**Objective:** wspd10m, refc, radar_ptype all working through V3 pipeline.

**Steps:**

1. Implement `builder/derive.py` — extract derivation logic from `build_cog.py`:
   - `derive_wspd()`: fetch u/v → `np.hypot()`
   - `derive_radar_ptype()`: fetch refc + 4 ptype masks → argmax
   - Wire each into `pipeline.py` as a dispatch based on `VarSpec.derivation`
2. Verify categorical overview handling: radar_ptype uses nearest for all bands
3. Verify value-grid semantics: wspd stores derived float mph; radar_ptype stores category index
4. Run all HRRR variables through scheduler, validate tiles + hover for each
5. Confirm legend rendering for all types (gradient, discrete, segmented)

**Checkpoint:** All current HRRR variables work. Adding a new simple variable is a one-spec addition.

### Phase 3 — Expand Models (iterative, 2–3 days per model)

**Objective:** GFS (conus + pnw), then additional models.

**Prerequisite for each new model:** Before writing a plugin or scheduler, prove acquisition:
1. Successfully download one full run's GRIB for one variable using whatever feed/tool the model requires
2. Open with cfgrib/xarray and confirm variable names, coordinate names, and grid projection
3. Warp one frame to EPSG:3857 and confirm `gdalinfo` output matches the artifact contract

Do not schedule a model until this prerequisite passes. This gates ~30 minutes of validation before committing days of plugin work.

**Steps per model:**

1. Complete acquisition prerequisite (above)
2. Copy/adapt model plugin (GFS already exists; ECMWF/NAM need new plugins)
3. Set grid resolution in `TARGET_GRID_METERS_BY_MODEL_REGION` (GFS: 25km, ECMWF: ~9km)
4. Add scheduler systemd service
5. Validate tiles render correctly — particularly at low zoom where GFS coarseness is visible
6. Implement `precip_ptype` derivation path for GFS (the blend path from current `build_cog.py`)

**Model priority:**
1. GFS (already has a working plugin + scheduler — acquisition proven)
2. NAM (publicly available via NOMADS/Herbie — acquisition straightforward, prove once)
3. ECMWF — **feed reality warning:**
   - ECMWF Open Data (free, 0.25° resolution, limited variables, 6-hour delay) may be sufficient for initial support
   - Full ECMWF HRES/IFS requires a paid license or MARS/CDS API access with approved credentials
   - Do not assume Herbie supports ECMWF the same way it supports GFS/HRRR — test `Herbie(model="ecmwf")` first
   - If open data is inadequate and licensing is not justified, defer ECMWF indefinitely

**Checkpoint:** Multiple models selectable in frontend dropdown, each with correct resolution and rendering. Each model passed the acquisition prerequisite before any plugin code was written.

### Phase 4 — CONUS Scale + Performance Hardening (1–2 weeks)

**Objective:** Full CONUS coverage with acceptable tile performance.

**Steps:**

1. Generate HRRR CONUS grid (~5000×3500 pixels at 3km) — verify COG size and overview quality
2. Measure tile request latency at z2–z10 for CONUS extent
3. Tune overview levels to ensure low-zoom tiles are served from overviews (not full-res reads)
4. Set up Cloudflare caching for tile URLs — verify immutable cache headers work correctly
5. Verify nginx proxy config (or direct uvicorn) handles concurrent tile requests under load
6. Add region selector to frontend: PNW (default), CONUS, custom regions
7. Monitor disk usage with 2-run retention at CONUS scale for multiple models

### Disk Budget (all models, full cadence)

Estimates use deflate-compressed COG sizes from the grid dimensions table. Per-frame size = RGBA + value COG.

| Model | Region | Per-frame (rgba + val) | Frames/run | Vars | Per-run total | 2-run retention |
|---|---|---|---|---|---|---|
| HRRR | CONUS | ~20 MB | 49 (fh0–48) | 4 | ~3.9 GB | ~7.8 GB |
| HRRR | PNW | ~3 MB | 49 | 4 | ~0.6 GB | ~1.2 GB |
| GFS | CONUS | ~1.5 MB | 65 (fh0–384, 3h/6h steps) | 4 | ~0.4 GB | ~0.8 GB |
| GFS | PNW | ~0.5 MB | 65 | 4 | ~0.1 GB | ~0.2 GB |
| ECMWF | CONUS | ~4 MB | 41 (fh0–240, 6h steps) | 4 | ~0.7 GB | ~1.4 GB |
| **Total** | | | | | | **~11.4 GB** |

**Notes:**
- HRRR runs every hour but only synoptic runs (00z/06z/12z/18z) extend to fh48; off-synoptic runs go to fh18 (19 frames). Budget above uses the longer synoptic run as worst case.
- GFS frame count assumes: fh0–240 at 3h steps (81 frames) + fh252–384 at 12h steps (12 frames) = ~93. Budget uses 65 as a conservative subset of commonly useful hours.
- Herbie GRIB cache is separate and ephemeral — cleaned after each build. Not counted in retention.
- Total retention across all models/regions: **~12 GB** with 2-run retention. Well within a 100 GB VPS.
- If more vars are added (P3 expansion to ~8 vars), double the estimate → ~24 GB. Still comfortable.

**Action:** Validate these estimates against real COG output sizes during Phase 1 (HRRR tmp2m). If actual sizes differ by >2×, revise the table before Phase 4.

**Checkpoint:** CONUS tiles render at all zooms without latency spikes. Disk usage is sustainable.

### Phase 5 — Final Cleanup (1 day)

**Objective:** Remove archived V2 leftovers and finalize production.

**Steps:**

1. Delete `/opt/twf_archive/` (the safety-net copies moved during Phase 0 consolidation)
2. Remove any stale V2 nginx config blocks
3. Remove old systemd service files from `/etc/systemd/system/twf-*-v2-*`
4. Confirm `sodakweather.com/models-v3` is the canonical URL (optionally alias to `/models`)
5. Verify no cron jobs, logrotate configs, or monitoring references point to old paths

**Checkpoint:** Zero V2 artifacts on disk, in systemd, or in nginx config. `twf_models_v3` is the only codebase anywhere.

---

## What Gets Carried Forward vs. Rewritten

### Carried forward (logic preserved, refactored into new modules)

| Component | Source | Destination |
|---|---|---|
| Model plugins + VarSpec | `app/models/*.py` | `backend/app/models/*.py` (extended with colormap fields) |
| Herbie fetch + priority logic | `services/fetch_engine.py`, `services/herbie_priority.py` | `backend/app/services/builder/fetch.py` |
| Variable derivation logic | `scripts/build_cog.py` (embedded) | `backend/app/services/builder/derive.py` |
| Colormap specs + encoding | `services/colormaps_v2.py` | `backend/app/services/colormaps.py` → `builder/colorize.py` |
| GeoTIFF → warp → overview → COG | `scripts/build_cog.py` (embedded) | `backend/app/services/builder/cog_writer.py` |
| Scheduler + run promotion + retention | `services/model_scheduler_v2.py` | `backend/app/services/scheduler.py` |
| LATEST.json atomic write | `services/model_scheduler_v2.py` | `backend/app/services/scheduler.py` |
| Grid detection + normalization | `services/grid.py` | `backend/app/services/builder/cog_writer.py` |
| Frontend double-buffer animation | `components/map-canvas.tsx` | `frontend/models-v3/src/components/map-canvas.tsx` |
| Frontend discovery chain | `lib/api.ts` | `frontend/models-v3/src/lib/api.ts` |
| Frontend legend component | `components/map-legend.tsx` | `frontend/models-v3/src/components/map-legend.tsx` |

### Deleted / not carried forward

| Component | Reason |
|---|---|
| `titiler_service/main.py` `_image_data_to_rgba()` (70+ lines) | RGBA COGs eliminate runtime colorization |
| `colormaps.py` `get_lut()` at serve time | LUT applied at build time now; `float_to_rgba()` in `builder/colorize.py` replaces both |
| `variable_registry.py` | Unified into extended VarSpec |
| `build_cog.py` monolithic `main()` | Decomposed into builder modules |
| `services/mbtiles.py` | PMTiles approach abandoned |
| Frontend "Legacy tiles" checkbox | V3 is the only path |
| `tiles-titiler/` compatibility route | No V2 compatibility needed |

### New (does not exist today)

| Component | Purpose |
|---|---|
| `builder/colorize.py` | float → RGBA at build time |
| `builder/value_grid.py` | float → float32 COG for hover |
| `builder/pipeline.py` | Orchestrator dispatching fetch → derive → colorize → write |
| `/api/v3/sample` endpoint | Point query for hover-for-data |
| Frontend hover tooltip | Calls `/sample`, shows value + units |
| Run manifests (`{run}.json`) | Rich manifest with expected/available frames |

---

## Target Grid Resolutions

### CONUS Bounding Box (authoritative)

All CONUS-region artifacts use the same bounding box, defined once:

```python
# WGS84 (EPSG:4326)
CONUS_BBOX_4326 = (-125.0, 24.0, -66.5, 50.0)  # (west, south, east, north)

# Web Mercator (EPSG:3857) — exact projection of the above
CONUS_BBOX_3857 = (-13914936.35, 2764607.34, -7403013.94, 6446275.84)
```

### PNW Bounding Box (authoritative)

```python
# WGS84 (EPSG:4326)
PNW_BBOX_4326 = (-126.0, 41.5, -116.0, 49.5)  # (west, south, east, north)

# Web Mercator (EPSG:3857) — exact projection of the above
PNW_BBOX_3857 = (-14026255.80, 5096324.37, -12913060.93, 6378137.00)
```

### Grid alignment rules

1. **All variables for a given model/region share an identical pixel grid.** The warp target extent and resolution are defined by `(BBOX, GRID_METERS)` — not per variable.
2. **`gdalwarp` always uses `-tap` (target-aligned pixels).** This snaps the grid origin to a multiple of the pixel size, guaranteeing that RGBA and value COGs for the same model/region are pixel-aligned across all variables and forecast hours.
3. **The warp command template is:**
   ```bash
   gdalwarp -t_srs EPSG:3857 \
     -te {xmin} {ymin} {xmax} {ymax} \
     -tr {res_x} {res_y} \
     -tap \
     -r {resampling} \
     input.tif output.tif
   ```
4. **The sampling API relies on this alignment.** Because RGBA and value COGs share the exact grid, a pixel coordinate in one maps to the same geographic location in the other. No per-request reprojection is needed.
5. **Bounding boxes and grid meters are defined in code as constants** (`REGION_BBOX` dict + `TARGET_GRID_METERS_BY_MODEL_REGION` dict). They are not inferred from input data.

### Grid dimensions

| Model | Region | Grid (meters) | Bbox | Approx pixels | Approx COG size (RGBA, deflate) |
|---|---|---|---|---|---|
| HRRR | PNW | 3,000 × 3,000 | PNW_BBOX_3857 | ~371 × 426 | ~2 MB |
| HRRR | CONUS | 3,000 × 3,000 | CONUS_BBOX_3857 | ~2171 × 1227 | ~15 MB |
| GFS | PNW | 25,000 × 25,000 | PNW_BBOX_3857 | ~45 × 51 | <1 MB |
| GFS | CONUS | 25,000 × 25,000 | CONUS_BBOX_3857 | ~261 × 147 | <1 MB |
| ECMWF | CONUS | 9,000 × 9,000 | CONUS_BBOX_3857 | ~724 × 409 | ~3 MB |

---

## Animation Strategy (frontend)

Carried forward from current implementation with minor refinements:

1. **Preload N future frames** (currently 4 prefetch sources) — keep as-is
2. **Double-buffer layer swap** (buffer A/B with micro-crossfade) — keep as-is
3. `raster-fade-duration: 0` — already set
4. **Autoplay pacing** (400ms tick, 1000ms hold for readiness) — keep as-is
5. **Per-variable resampling** (linear for radar/ptype, nearest for continuous) — keep as-is
6. **GFS fade-out at high zoom** (opacity → 0 above z7) — keep, revisit threshold for CONUS

No fundamental animation changes needed — the current frontend already implements the target behavior.

---

## Production Deployment

V2 is stopped and removed during Phase 0 consolidation (see "Consolidation procedure" above). There is no parallel operation period.

### Systemd services

```ini
# /etc/systemd/system/twf-v3-tile-server.service
[Unit]
Description=TWF V3 Tile Server
After=network.target

[Service]
User=brian
WorkingDirectory=/opt/twf_v3
Environment=TWF_V3_DATA_ROOT=/opt/twf_v3/data/v3
Environment=TWF_V3_TILE_SIZE=512
ExecStart=/opt/twf_v3/.venv/bin/uvicorn backend.app.services.tile_server:app --host 127.0.0.1 --port 8201 --workers 2
Restart=on-failure
RestartSec=4

[Install]
WantedBy=multi-user.target
```

```ini
# /etc/systemd/system/twf-v3-api.service
[Unit]
Description=TWF V3 API (Discovery + Sampling)
After=network.target

[Service]
User=brian
WorkingDirectory=/opt/twf_v3
Environment=TWF_V3_DATA_ROOT=/opt/twf_v3/data/v3
ExecStart=/opt/twf_v3/.venv/bin/uvicorn backend.app.main:app --host 127.0.0.1 --port 8200 --workers 2
Restart=on-failure
RestartSec=4

[Install]
WantedBy=multi-user.target
```

```ini
# /etc/systemd/system/twf-v3-hrrr-conus-scheduler.service
[Unit]
Description=TWF V3 HRRR CONUS Scheduler
After=network.target

[Service]
User=brian
WorkingDirectory=/opt/twf_v3
Environment=TWF_V3_DATA_ROOT=/opt/twf_v3/data/v3
Environment=TWF_V3_WORKERS=4
ExecStart=/opt/twf_v3/.venv/bin/python -m backend.app.services.scheduler --model hrrr --region conus --vars tmp2m,wspd10m,refc,radar_ptype --primary-vars tmp2m
Restart=on-failure
RestartSec=4

[Install]
WantedBy=multi-user.target
```

GFS scheduler follows the same pattern with `--model gfs`.

### Nginx routing

Replace all V2 location blocks with:

```nginx
# V3 frontend
location /models-v3/ {
    alias /opt/twf_v3/frontend/models-v3/dist/;
    try_files $uri $uri/ /models-v3/index.html;
}

# V3 API (discovery + sampling)
location /api/v3/ {
    proxy_pass http://127.0.0.1:8200;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
}

# V3 tiles
location /tiles/v3/ {
    proxy_pass http://127.0.0.1:8201;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
}

# Remove or comment out all /models-v2, /api/v2, /tiles/v2 blocks
```

### Environment variables

```bash
TWF_V3_DATA_ROOT=/opt/twf_v3/data/v3
TWF_V3_WORKERS=4
TWF_V3_TILE_SIZE=512
```

### Deploy workflow (ongoing, after initial setup)

```bash
# On prod, pull and restart
cd /opt/twf_v3
git pull origin main

# Backend changes
source .venv/bin/activate
pip install -r backend/requirements.txt  # only if deps changed
sudo systemctl restart twf-v3-api twf-v3-tile-server

# Frontend changes
cd frontend/models-v3
npm install && npm run build  # only if frontend changed
# Static files served by nginx — no restart needed

# Scheduler changes
sudo systemctl restart twf-v3-hrrr-conus-scheduler twf-v3-gfs-conus-scheduler
```

No CI/CD pipeline needed at this stage. Manual `git pull` + restart is appropriate for a single-developer project.

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| HRRR CONUS COGs too large for disk | Medium | High | Monitor early in Phase 4; can reduce to ~6km grid if needed |
| RGBA overview quality for categorical vars (noise at low zoom) | Medium | Medium | Test nearest-only overviews; consider 2-layer approach from ROADMAP P1.4 |
| Herbie upstream outages during build | Low | Low | Already handled with retry logic in fetch_engine.py |
| GDAL version differences in overview behavior | Low | Medium | Pin GDAL version in Dockerfile; carry forward version-detection fallbacks |
| Frontend hover self-DOS | Low | Medium | 150ms debounce + generation counter + 256-entry LRU cache (see "Sampling Cost Control") |
| Schema drift between sidecar JSON versions | Low | Medium | `contract_version` field enforced in all metadata |

---

## Success Criteria

- [ ] HRRR tmp2m tiles return 200 at z2–z10 for CONUS extent
- [ ] Hover over any tile shows correct numeric value with units
- [ ] Adding a new simple variable requires only a `VarSpec` entry (one file, one place)
- [x] Tile server has zero colormap/science logic (under 150 lines total)
- [ ] Animation is smooth (double-buffer swap, no flash between frames)
- [x] Build pipeline produces both RGBA + value COGs per frame
- [x] Builder correctness gates (gdalinfo + pixel sanity) reject broken artifacts before promotion
- [ ] Run promotion is atomic via `LATEST.json` pointer
- [ ] 2-run retention keeps disk usage within budgeted ~12 GB (validated against disk budget table)
- [ ] All current variables (tmp2m, wspd10m, refc, radar_ptype, precip_ptype) work in V3
- [x] Single repo (`twf_models_v3`), single prod directory (`/opt/twf_v3`), no V2 remnants anywhere
- [x] Old repos archived on GitHub, old directories removed from prod and local machine

---

## Immediate Next Steps (do these first)

1. ~~**Create the `twf_models_v3` GitHub repo** with the target directory structure~~ ✅
2. ~~**Copy model plugins + colormap specs** from this repo into V3 structure~~ ✅
3. ~~**Implement `builder/colorize.py`** — the `float_to_rgba()` function~~ ✅
4. ~~**Implement `builder/cog_writer.py`** — GeoTIFF write + warp + overviews + COG output~~ ✅
5. ~~**Implement `builder/fetch.py`** — GRIB acquisition via Herbie + unit conversion~~ ✅
6. ~~**Implement `builder/pipeline.py`** — orchestrator for fetch → warp → colorize → write → validate~~ ✅
7. ~~**Generate one `fh000.rgba.cog.tif` + `fh000.val.cog.tif`** for HRRR tmp2m and validate with `gdalinfo`~~ ✅
8. ~~**Upgrade tile server stub** to read 4-band RGBA COGs via rio-tiler and return PNG tiles~~ ✅
9. ~~**Add `/api/v3/sample`** reading `val.cog.tif`~~ ✅
10. ~~**Wire frontend hover tooltip** calling `/sample`~~ ✅
11. **Validate end-to-end:** tiles at z2–z10, hover returns correct values ← **START HERE**

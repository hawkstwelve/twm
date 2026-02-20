# Performance Improvement Plan — TWF V3

Findings and implementation plans from code review of the frontend, API server, and tile server.
Organized by impact tier. All recommendations are constrained to the V3 architecture — no suggestions
here violate the Non-Goals in ROADMAP_V3.md (no runtime colormap logic, no vector tiles, etc.).

---

## Status Key

- `[ ]` Not started
- `[~]` Partially done / existing partial mitigation
- `[x]` Complete

---

## HIGH IMPACT

---

### H1 — API Server: Missing Cache-Control Headers

**Area:** `backend/app/main.py`  
**Impact:** Every variable/run selector interaction re-fetches from the server on every page load.
Browser heuristic caching is unreliable and short. The ROADMAP's caching table specifies exact
TTLs for each response type — none are currently implemented in the API server.

**Current state:** Zero `Cache-Control` headers on any API response. The tile server correctly
sets `public, max-age=31536000, immutable` on PNG hits, but the discovery and sample endpoints do not.

**ROADMAP target (from "Caching Strategy" table):**

| Endpoint | Target Cache-Control |
|---|---|
| `GET /api/v3/{model}/{region}/runs` | `public, max-age=60` |
| `GET /api/v3/{model}/{region}/{run}/{var}/frames` (resolved run) | `public, max-age=31536000, immutable` |
| `GET /api/v3/{model}/{region}/{run}/{var}/frames` (run=`latest`) | `public, max-age=60` |
| `GET /api/v3/sample` 200 (resolved run) | `private, max-age=86400` |
| `GET /api/v3/sample` 204 (nodata / out-of-bounds) | `private, max-age=300` |

**`/sample` cache policy:**

`/sample` is `private` — do not make it `public` even with coordinate rounding. The reasons:

- **High cardinality, low reuse.** At 2-decimal-place rounding (~1.1 km cells), PNW alone has
  ~800,000 possible cells × models × vars × forecast hours. Reuse probability per cell per user
  session is low; shared cache hit rate is negligible. A shared cache entry that's never reused
  is just wasted memory and eviction pressure on entries that actually benefit from sharing.
- **Weaponizable.** A bot hovering a systematic grid pattern generates a perfectly uniform key
  distribution that fills a shared cache zone with zero benefit to real users.
- **Frontend LRU + nginx rate limit is the right layer.** The 256-entry LRU in
  `use-sample-tooltip.ts` already handles repeated hovers over the same area within a session.
  The nginx `limit_req` (M5) caps per-IP request rate. Together these are sufficient and don't
  require a shared cache at all.

If shared `public` caching is revisited in the future, the prerequisites are: server-side
coordinate quantization (not just frontend-LRU quantization), a separate nginx `proxy_cache`
zone with a fixed hard size limit, and stricter rate limiting.

**Known frontend bug (fix independently of caching):** `use-sample-tooltip.ts` applies
`roundCoord()` to the LRU cache key but sends raw full-precision `lat`/`lon` in the
`fetchSample()` call. This means LRU hits avoid the network correctly, but LRU misses still
generate unique high-precision URLs. Fix: pass the rounded coordinates to `fetchSample` too:

```typescript
const roundedLat = roundCoord(lat);
const roundedLon = roundCoord(lon);
const key = cacheKey(ctx.model, ctx.region, ctx.run, ctx.varId, ctx.fh, roundedLat, roundedLon);

fetchSample({
  lat: roundedLat,  // ← was: lat (raw)
  lon: roundedLon,  // ← was: lon (raw)
  ...
})
```

This is a correctness fix for the LRU semantics and bounds the URL key space, but it does not
change the cache directive — `/sample` stays `private`.

**Implementation plan:**

1. **Apply the frontend `fetchSample` fix first** (see bug note above) — improves LRU semantics
   and bounds the URL key space. Independent of all server changes.
2. In `list_runs()`: return `Response` with `Cache-Control: public, max-age=60`. Also return an
   `ETag` derived from the run list content (e.g., `hashlib.md5(json.dumps(run_ids).encode()).hexdigest()[:8]`).
   Clients will get a cheap `304 Not Modified` on refresh instead of re-downloading the body.
3. In `list_frames()`: detect whether `run` is `"latest"` or a resolved run ID (matches
   `_RUN_ID_RE`). Resolved runs → `public, max-age=31536000, immutable`. ETag is optional here —
   `immutable` tells the browser never to revalidate so the ETag won't be used in normal browser
   behavior, but adding one is harmless and simplifies code if a single ETag-generation path is
   shared across all `list_frames` variants. `"latest"` → `public, max-age=60` with an ETag so
   clients revalidate cheaply after the TTL expires (this is the important case).
4. In `sample()`: return `private, max-age=86400` on 200 and `private, max-age=300` on 204/OOB.
   ETag is optional — `private` responses live only in the browser cache and the TTLs are short
   enough that stale-reads are not a concern. Include one if the implementation is trivial to
   share, skip it otherwise.
5. Add `Vary: Accept-Encoding` where gzip applies (nginx may handle this transparently).

**Why ETag on `/runs` and latest `/frames`:** These TTLs are short (60s) so clients revalidate
frequently. Without an ETag, every revalidation re-downloads the full body even when the run
list or frame list hasn't changed. With ETag, unchanged responses cost one RTT at ~200 bytes
instead of the full JSON payload.

**Constraint:** The `frames` endpoint already embeds full sidecar metadata (legend stops, units,
contour specs) in each `FrameRow.meta`. Once that payload is cached immutably in the browser,
variable switches within the same run require zero additional API calls.

---

### H2 — Frontend: Waterfall API Chain on Initial Load

**Area:** `frontend/models-v3/src/App.tsx`  
**Impact:** First-paint data path is 4 sequential round trips before any tile can render:
`fetchModels` → `fetchRegions` → `fetchRuns` + `fetchVars` → `fetchFrames`. On a 50ms RTT
connection this is 200–400ms of dead time before MapLibre starts loading tiles.

**Current state:** Each `useEffect` fires only after its dependency state settles from the previous
effect. The chain is fully sequential even though the defaults are known at compile time.

**Implementation plan:**

1. Add a parallel "fast path" on mount: fire `fetchRuns(DEFAULTS.model, DEFAULTS.region, "latest")`
   and `fetchVars(DEFAULTS.model, DEFAULTS.region, "latest")` and `fetchFrames(DEFAULTS.model,
   DEFAULTS.region, "latest", DEFAULTS.variable)` simultaneously alongside `fetchModels` and
   `fetchRegions`.
2. If all succeed and the user hasn't changed any selector, apply all results in one synchronous
   batch — skipping the waterfall entirely.
3. If any fast-path fetch fails, fall back to the existing sequential chain (already in place).
4. The waterfall chain remains as-is for non-default selections (user changed model/region/variable
   before initial load completes) — no behavioral change in that case.

**Note:** This does not change tile URLs, run resolution, or any artifact paths. It only
parallelizes the discovery fetches that already happen on every page load.

---

### H3 — CDN Layer for Tile Serving (Pre-Launch Gate)

**Area:** Infrastructure — Cloudflare in front of `api.sodakweather.com`  
**Impact:** Tile URLs are already served with `public, max-age=31536000, immutable` — they are
designed for edge caching. Without a CDN, every tile request hits the VPS regardless of how many
users have already loaded the same tile. Under concurrent load (multiple forum users animating
the same run simultaneously), the tile server becomes the bottleneck even though every response
is byte-for-byte identical.

**Current state:** Tiles are served directly from uvicorn behind nginx on the VPS. ROADMAP Phase 4
explicitly calls out Cloudflare setup, but no CDN is in place yet.

**Why this is HIGH for multi-user:** A single forum post linking to a weather animation can drive
O(10–100) simultaneous users. Each user at full zoom generates ~15–30 tile requests per frame
advance. Without a CDN, 50 concurrent users scrubbing through frames means thousands of rasterio
pixel reads per second on a single VPS. With a CDN, popular tiles (current frame at the default
view) are served from edge with zero origin hits after the first user warms them.

**Implementation plan (from ROADMAP Phase 4):**

1. Point the `api.sodakweather.com` DNS to Cloudflare (orange-cloud proxy).
2. Set a Cloudflare Cache Rule: URLs matching `/tiles/v3/*` → Cache Everything, Edge TTL = Respect
   Origin (the `immutable` header will be honoured — tiles are cached for one year at edge).
3. Verify with `curl -I` that `cf-cache-status: HIT` appears on the second request for a tile.
4. For discovery endpoints, add a second Cache Rule: `/api/v3/*` → "Eligible for Cache" (respect
   origin headers). This lets Cloudflare edge-cache `/api/v3/*/runs` and `/api/v3/*/frames`
   responses that carry `public` headers (set by H1), while leaving `/api/v3/sample` uncached —
   its `private` directive prevents shared caching automatically. For extra explicitness, add a
   bypass rule ahead of it: URI path contains `/api/v3/sample` → Bypass Cache. This makes the
   intent unambiguous regardless of future header changes.
5. Do NOT apply Cache Everything to `/api/v3/*`. That rule must be scoped to `/tiles/v3/*` only.
   Discovery endpoints must respect their own `Cache-Control` headers; `/sample` must never be
   edge-cached.

**Note:** The tile server's existing `CACHE_HIT = "public, max-age=31536000, immutable"` header
is already CDN-ready. No server code changes are needed — this is purely infrastructure.

---

## MEDIUM IMPACT

---

### M1 — Vite Build: No Code-Splitting for maplibre-gl

**Area:** `frontend/models-v3/vite.config.ts`  
**Impact:** `maplibre-gl` is ~1.5 MB minified and currently bundles into the single output chunk.
Any app code change busts the entire bundle cache. Splitting it into a separate chunk allows
the browser to cache `maplibre-gl` independently across deploys.

**Current state:** `vite.config.ts` has no `build.rollupOptions`. All dependencies land in one chunk.

**Implementation plan:**

```typescript
// vite.config.ts
build: {
  rollupOptions: {
    output: {
      manualChunks: {
        maplibre: ["maplibre-gl"],
      },
    },
  },
},
```

No application logic changes. Run `npm run build` to verify chunk split in `dist/assets/`.

---

### M2 — Frontend: Prefetch Count / Buffer Count Mismatch

**Area:** `frontend/models-v3/src/App.tsx` + `map-canvas.tsx`  
**Impact:** For radar/ptype variables the `prefetchTileUrls` memo produces up to 6 URLs
(`isRadarLike ? 6 : 4`), but `PREFETCH_BUFFER_COUNT = 4` in `map-canvas.tsx`. The last 2 URLs
are silently dropped — the prefetch effect slices `prefetchTileUrls[idx]` where `idx` goes 0..3.

**Current state:** `PREFETCH_BUFFER_COUNT = 4`. Radar-like prefetch count = 6.

**Implementation plan (two options — pick one):**

- **Option A (preferred):** Reduce radar prefetch count from 6 to 4 to match the existing buffer
  count. Radar variables have large tiles (discrete coloration fills entire grid) and rarely benefit
  from 6-frame lookahead vs. 4.
- **Option B:** Increase `PREFETCH_BUFFER_COUNT` to 6 in `map-canvas.tsx` and add 2 more
  `prefetchSourceId(5)` / `prefetchSourceId(6)` source + layer declarations in `styleFor()` and
  the prefetch effect. More invasive — increases MapLibre source/layer count.

---

### M3 — Frontend: Autoplay Tick Rate

**Area:** `frontend/models-v3/src/App.tsx`  
**Impact:** `AUTOPLAY_TICK_MS = 400` is fixed regardless of tile cache state. Since the ready-ahead
gate (`AUTOPLAY_READY_AHEAD = 2`) already holds playback until frames are available, the tick rate
only adds latency when tiles are already cached. Reducing to 200–250ms would make animation feel
more responsive after the first few frames are warm.

**Current state:** 400ms fixed interval. The ready-ahead gate correctly prevents advancing to
unloaded frames.

**Implementation plan:**

1. Reduce `AUTOPLAY_TICK_MS` from `400` to `250`.
2. No other changes needed — `autoplayPrimedRef` and `isTileReady()` logic continues to gate
   advancement on actual readiness regardless of tick rate.
3. If 250ms feels too fast for certain variables, it can be made per-variable (e.g., 250ms for
   `tmp2m`, 350ms for radar) by reading `VarSpec.kind` from the frame metadata — but start with
   a uniform 250ms and adjust empirically.

---

### M4 — Frontend: `<link rel="preconnect">` for External Domains

**Area:** `frontend/models-v3/index.html`  
**Impact:** On cold load, the browser must perform DNS + TCP + TLS for `api.sodakweather.com` and
`*.basemaps.cartocdn.com` before the first tile or API call can complete. Preconnect eliminates
this from the critical path — typically saves 100–300ms on first paint.

**Current state:** No preconnect hints in `index.html`.

**Implementation plan:**

```html
<!-- In <head>, before the Vite script tags -->
<link rel="preconnect" href="https://api.sodakweather.com" />
<link rel="preconnect" href="https://a.basemaps.cartocdn.com" crossorigin />
<link rel="dns-prefetch" href="https://b.basemaps.cartocdn.com" />
<link rel="dns-prefetch" href="https://c.basemaps.cartocdn.com" />
<link rel="dns-prefetch" href="https://d.basemaps.cartocdn.com" />
```

The four CARTO subdomains are used for basemap tile load-balancing — DNS prefetch (not full
preconnect) is sufficient since they share the same CDN infrastructure.

---

### M5 — nginx Rate Limiting for `/sample` (Pre-Launch Gate)

**Area:** `deployment/` — nginx config  
**Impact:** The `/sample` endpoint performs a rasterio pixel read on every request. Under
concurrent load from multiple users hovering simultaneously, unthrottled `/sample` traffic can
saturate uvicorn workers. The ROADMAP already anticipates this: "if it ever becomes public, add
a simple per-IP rate limit (e.g., 10 req/s) via nginx `limit_req`."

**Current state:** No rate limiting on any API endpoint. Acceptable for single-user dev; should
be in place before public launch.

**Implementation plan:**

```nginx
# In nginx.conf or a conf.d include, in the http {} block:
limit_req_zone $binary_remote_addr zone=sample_api:10m rate=10r/s;

# In the location block that proxies /api/v3/:
location /api/v3/ {
    limit_req zone=sample_api burst=20 nodelay;
    limit_req_status 429;
    proxy_pass http://127.0.0.1:8200;
}
```

Parameter rationale:
- `rate=10r/s` — handles aggressive hover without hammering the server; well above normal usage
- `burst=20` — allows a short burst (e.g., quick pan across the map) without immediately 429ing
- `nodelay` — burst requests are served immediately rather than queued (a queued sample response
  that arrives 2 seconds late is stale and useless)

The 80ms debounce + generation counter in `use-sample-tooltip.ts` already limits a single user
to ~12 requests/second at maximum hover speed, so `rate=10r/s` will not affect normal usage.
The rate limit only fires under actual abuse or pathological concurrent-hover storms.

---

## LOW IMPACT

---

### L1 — Frontend: `buildLegend` Recomputes on Opacity Change

**Area:** `frontend/models-v3/src/App.tsx`  
**Impact:** The `legend` memo lists `opacity` as a dependency alongside `currentFrame` and
`frameRows`. The stop/entry computation in `buildLegend()` (iterating ptype_order, mapping
levels/colors, etc.) re-runs on every opacity slider move even though opacity has no effect on
the computed entries — only on the final `LegendPayload.opacity` field.

**Current state:**
```tsx
const legend = useMemo(
  () => buildLegend(normalizedMeta, opacity),
  [currentFrame, frameRows, opacity]  // opacity causes full recompute
);
```

**Implementation plan:**

Split into two memos:
```tsx
const legendEntries = useMemo(
  () => buildLegend(normalizedMeta, 1),  // compute structure once, opacity=1 as placeholder
  [currentFrame, frameRows]
);

const legend = useMemo(
  () => legendEntries ? { ...legendEntries, opacity } : null,
  [legendEntries, opacity]
);
```

`buildLegend` signature does not need to change — only the call site.

---

### L2 — Frontend: LRU Cache O(n) `get()` / `set()`

**Area:** `frontend/models-v3/src/lib/use-sample-tooltip.ts`  
**Impact:** `LRUCache.get()` calls `this.keys.filter(k => k !== key)` on every cache hit — this
copies the full array. At 256 entries this is negligible (~microseconds), but it's incorrect
semantics for LRU and would degrade at larger capacities.

**Current state:** `keys: string[]` array, O(n) filter on every access.

**Implementation plan:**

Replace the `keys: string[]` array with `Map` insertion-order promotion:

```typescript
class LRUCache {
  private map = new Map<string, SampleResult | null>();

  get(key: string): SampleResult | null | undefined {
    if (!this.map.has(key)) return undefined;
    const value = this.map.get(key)!;
    this.map.delete(key);
    this.map.set(key, value); // re-insert = move to most-recent
    return value;
  }

  set(key: string, value: SampleResult | null): void {
    if (this.map.has(key)) this.map.delete(key);
    else if (this.map.size >= LRU_CAPACITY) {
      this.map.delete(this.map.keys().next().value); // evict oldest (first key)
    }
    this.map.set(key, value);
  }

  clear(): void { this.map.clear(); }
}
```

`Map` in JS preserves insertion order — `keys().next().value` is always the oldest entry. Fully O(1).

---

### L3 — Frontend: Prefetch Source Init URLs Cause Duplicate Tile Requests

**Area:** `frontend/models-v3/src/components/map-canvas.tsx`  
**Impact:** `styleFor()` initializes all 6 sources (`a`, `b`, `prefetch-1..4`) with the same
`overlayUrl`. Before the prefetch effect runs and calls `setTiles()`, MapLibre issues tile
requests for the active frame's URL from all 6 sources simultaneously. These are duplicate requests
that complete and are immediately discarded when the prefetch effect overwrites the source URLs.

**Current state:** All sources in `styleFor()` receive `tiles: [overlayUrl]`.

**Implementation plan:**

Pass a no-op placeholder URL for prefetch sources during initial style creation:

```typescript
// In styleFor(), for prefetch sources:
[prefetchSourceId(1)]: {
  type: "raster",
  tiles: ["about:blank"],  // or any URL that returns fast 404
  tileSize: 512,
},
```

Alternatively, set `tiles: []` (empty array) if MapLibre supports it without erroring. The
prefetch effect will call `source.setTiles([realUrl])` on its first run before any real tile
load occurs.

---

### L4 — Frontend: Scrub State Passed via `useEffect` Instead of Direct Call

**Area:** `frontend/models-v3/src/components/bottom-forecast-controls.tsx`  
**Impact:** `onScrubStateChange` is called inside a `useEffect` watching `isScrubbing`, adding
a render cycle between the user grabbing the slider and `App` receiving the scrub signal. This
delays `isScrubbing ? 2 : 0` prefetch optimization by one React render (~16ms).

**Current state:**
```tsx
useEffect(() => {
  onScrubStateChange?.(isScrubbing);
}, [isScrubbing, onScrubStateChange]);
```

**Implementation plan:**

Call `onScrubStateChange` directly at the point `setIsScrubbing` is called:

```tsx
// In onValueChange handler:
if (!isScrubbing) {
  setIsScrubbing(true);
  onScrubStateChange?.(true);  // fire immediately, not deferred
}

// In onValueCommit handler:
setIsScrubbing(false);
onScrubStateChange?.(false);  // fire immediately
```

Remove the `useEffect` for `onScrubStateChange`. Keep the `isPlaying && isScrubbing` effect
that resets scrub state when playback starts.

---

### L5 — Frontend: Background Frame Refresh Uses Polling; Should Use `visibilitychange`

**Area:** `frontend/models-v3/src/App.tsx`  
**Impact:** The 30-second refresh interval runs continuously and checks `document.hidden` on every
tick. The interval keeps the JS runtime awake unnecessarily when the tab is backgrounded. Modern
browsers throttle `setInterval` in hidden tabs but it still consumes a timer slot.

**Current state:** `window.setInterval(... if (document.hidden) return; ... , 30000)`.

**Implementation plan:**

Suspend the interval when the tab is hidden, resume on `visibilitychange`:

```tsx
useEffect(() => {
  let intervalId: number | null = null;
  let cancelled = false;

  const refresh = () => {
    if (cancelled || !model || !region || !variable) return;
    fetchFrames(model, region, resolvedRunForRequests, variable)
      .then((rows) => { if (!cancelled) setFrameRows(rows); })
      .catch(() => {});
  };

  const start = () => {
    if (intervalId !== null) return;
    intervalId = window.setInterval(refresh, 30_000);
  };

  const stop = () => {
    if (intervalId !== null) { window.clearInterval(intervalId); intervalId = null; }
  };

  if (!document.hidden) start();
  document.addEventListener("visibilitychange", () => {
    document.hidden ? stop() : start();
  });

  return () => {
    cancelled = true;
    stop();
    document.removeEventListener("visibilitychange", () => {});
  };
}, [model, region, run, variable, resolvedRunForRequests]);
```

---

### L6 — Frontend: AbortController for In-Flight API Fetches

**Area:** `frontend/models-v3/src/lib/api.ts`  
**Impact:** `fetchJson` does not accept an `AbortSignal`. When the user rapidly switches
models/variables, the stale requests complete and their results are discarded by the
`cancelled = true` flag — but the network requests are not cancelled. For small JSON payloads
on a same-datacenter API this is negligible. It becomes meaningful if frames payloads grow
(large sidecar metadata, many frames) or if the API is geographically distant.

**Current state:** `fetchJson` calls `fetch(url, { credentials: "omit" })` with no signal.
The cancellation flag in each `useEffect` is correct but only prevents state writes —
it doesn't abort the underlying request.

**Implementation plan:**

Add an optional `signal` parameter to `fetchJson`:

```typescript
async function fetchJson<T>(url: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(url, { credentials: "omit", signal });
  if (!response.ok) throw new Error(`Request failed: ${response.status}`);
  return response.json() as Promise<T>;
}
```

In each `useEffect` in `App.tsx` that fires fetches, create an `AbortController` and pass
`controller.signal` to each fetch call. In the cleanup function, call `controller.abort()`.
The existing `cancelled = true` flag can then be removed — `AbortError` thrown by `fetch`
is already ignored by the `if (cancelled) return` guard (since the promise rejects and is
caught by the error handler).

**Priority note:** For single-user dev this is negligible — orphaned requests are short-lived
and the server is idle otherwise. Under concurrent public users it becomes more meaningful:
50 users each rapidly switching variables means 50× as many orphaned in-flight requests tying
up uvicorn workers until they naturally complete. Still not a crisis at this scale (uvicorn is
async and the requests finish quickly), but tag it for Phase 4 hardening alongside CDN and
rate-limiting work (H3, M5).

---

### L7 — Frontend: Local Dev URL Detection Not Implemented

**Area:** `frontend/models-v3/src/lib/config.ts`  
**Impact:** Developer experience / prevents accidental prod traffic during local dev. The ROADMAP
already specifies the exact implementation.

**Current state:** `API_BASE` and `TILES_BASE` are hardcoded to production URLs.

**Implementation plan (verbatim from ROADMAP):**

```typescript
const isLocal =
  window.location.hostname === "localhost" ||
  window.location.hostname === "127.0.0.1";

export const API_BASE = isLocal
  ? "http://127.0.0.1:8200/api/v3"
  : "https://api.sodakweather.com/api/v3";

export const TILES_BASE = isLocal
  ? "http://127.0.0.1:8201"
  : "https://api.sodakweather.com";
```

---

## Items Investigated but NOT Recommended

The following were examined and found to either already be in place or not apply:

| Suggestion | Finding |
|---|---|
| Throttle source updates during scrubbing | Already done — `DRAG_UPDATE_MS = 200` in `bottom-forecast-controls.tsx` with `emitForecastHour(next, false)` during drag and forced emit on `onValueCommit`. The throttle is the correct mechanism; do not rely on `setTiles()` to cancel in-flight requests — it does not reliably do so. |
| Stable tile cache keys / no accidental cache-busters | Tile URLs are fully content-addressed: `/tiles/v3/{model}/{region}/{run}/{var}/{fh}/{z}/{x}/{y}.png` with no query params. `run=latest` is resolved to the real run ID before tile URLs are built (`resolvedRunForRequests` in `App.tsx`). |
| Request coalescing for hover samples | Already done — 80ms debounce + generation counter in `use-sample-tooltip.ts`. Stale responses are silently discarded. |

---

## Implementation Order

Items marked **PRE-LAUNCH** are correctness or safety gates that must be in place before opening
the site to forum users, regardless of their impact tier.

| Priority | ID | Item | File(s) | Effort | Pre-Launch? | Status |
|---|---|---|---|---|---|---|
| HIGH | H1-pre | Fix LRU key/URL mismatch in fetchSample | `use-sample-tooltip.ts` | 5 min | — | `[x]` |
| HIGH | H1 | API cache headers | `backend/app/main.py` | ~30 min | ✅ | `[x]` |
| HIGH | H2 | Parallel API fetches on mount | `src/App.tsx` | ~1 hr | — | `[x]` |
| HIGH | H3 | Cloudflare CDN for tile serving | Infrastructure / DNS | ~1 hr | ✅ | `[x]` |
| MEDIUM | M1 | Vite `manualChunks` for maplibre-gl | `vite.config.ts` | 5 min | — | `[x]` |
| MEDIUM | M2 | Fix prefetch count mismatch | `App.tsx`, `map-canvas.tsx` | 10 min | — | `[x]` |
| MEDIUM | M3 | Reduce autoplay tick to 250ms | `App.tsx` | 2 min | — | `[ ]` |
| MEDIUM | M4 | `<link rel="preconnect">` hints | `index.html` | 5 min | — | `[ ]` |
| MEDIUM | M5 | nginx `limit_req` for `/sample` | nginx config | 15 min | ✅ | `[x]` |
| LOW | L1 | Split `buildLegend` memo from opacity | `App.tsx` | 15 min | — | `[ ]` |
| LOW | L2 | O(1) LRU cache | `use-sample-tooltip.ts` | 15 min | — | `[ ]` |
| LOW | L3 | Prefetch source init URLs | `map-canvas.tsx` | 10 min | — | `[ ]` |
| LOW | L4 | Scrub state direct callback | `bottom-forecast-controls.tsx` | 10 min | — | `[ ]` |
| LOW | L5 | `visibilitychange`-based refresh | `App.tsx` | 20 min | — | `[ ]` |
| LOW | L6 | AbortController for fetches | `api.ts` + `App.tsx` | 30 min | — | `[ ]` |
| LOW | L7 | Local dev URL detection | `config.ts` | 5 min | — | `[ ]` |

**Suggested sequencing:**

1. **Now (current phase):** H1-pre → H1 → H2 → M1–M4 + L1–L5, L7. These improve the experience
   for the current single-user workflow and are all low-risk/reversible.
2. **Before public launch:** H3 (Cloudflare DNS change) + M5 (nginx config reload). These protect
   the server under concurrent load. Smoke-test both in staging before the forum post goes out.
3. **Phase 4 hardening:** L6 (AbortController). Low urgency at launch scale but correct to have
   before sustained concurrent traffic.

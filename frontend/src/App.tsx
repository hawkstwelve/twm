import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AlertCircle } from "lucide-react";

import { BottomForecastControls } from "@/components/bottom-forecast-controls";
import { MapCanvas, type BasemapMode } from "@/components/map-canvas";
import { type LegendPayload, MapLegend } from "@/components/map-legend";
import { WeatherToolbar } from "@/components/weather-toolbar";
import {
  buildContourUrl,
  type CapabilitiesResponse,
  type CapabilityModel,
  type CapabilityVariable,
  type FrameRow,
  type LegendMeta,
  type LoopManifestResponse,
  type RegionPreset,
  type RunManifestResponse,
  fetchManifest,
  fetchCapabilities,
  fetchFrames,
  fetchLoopManifest,
  fetchRegionPresets,
  fetchRuns,
} from "@/lib/api";
import {
  API_ORIGIN,
  getPlaybackBufferPolicy,
  isWebpDefaultRenderEnabled,
  MAP_VIEW_DEFAULTS,
  OVERLAY_DEFAULT_OPACITY,
  WEBP_RENDER_MODE_THRESHOLDS,
} from "@/lib/config";
import { buildRunOptions } from "@/lib/run-options";
import { buildTileUrlFromFrame } from "@/lib/tiles";
import { useSampleTooltip } from "@/lib/use-sample-tooltip";

const AUTOPLAY_TICK_MS = 250;
const AUTOPLAY_READY_AHEAD = 2;
const AUTOPLAY_SKIP_WINDOW = 3;
const FRAME_STATUS_BADGE_MS = 900;
const READY_URL_TTL_MS = 30_000;
const READY_URL_LIMIT = 160;
const INFLIGHT_FRAME_TTL_MS = 12_000;
const PRELOAD_START_RATIO = 0.7;
const PRELOAD_STALL_MS = 8000;
const FRAME_MAX_RETRIES = 3;
const FRAME_HARD_DEADLINE_MS = 30_000;
const FRAME_RETRY_BASE_MS = 1200;
const LOOP_PRELOAD_MIN_READY = 2;
const LOOP_AHEAD_READY_TARGET = 8;
const LOOP_MIN_PLAYABLE_AHEAD = 2;
const MAX_CONCURRENT_DECODES = 1;
const WEBP_DECODE_CACHE_BUDGET_DESKTOP_BYTES = 256 * 1024 * 1024;
const WEBP_DECODE_CACHE_BUDGET_MOBILE_BYTES = 128 * 1024 * 1024;
const EMPTY_TILE_DATA_URL = "data:image/gif;base64,R0lGODlhAQABAAAAACwAAAAAAQABAAA=";

type RenderModeState = "webp_tier0" | "webp_tier1" | "tiles";

type BufferSnapshot = {
  totalFrames: number;
  bufferedCount: number;
  bufferedAheadCount: number;
  terminalCount: number;
  terminalAheadCount: number;
  failedCount: number;
  inFlightCount: number;
  queueDepth: number;
  statusText: string;
  version: number;
};

type Option = {
  value: string;
  label: string;
};

type VariableEntry = {
  id: string;
  displayName?: string;
  order?: number | null;
  defaultFh?: number | null;
  buildable?: boolean;
  kind?: string | null;
};

type ModelEntry = {
  id: string;
  displayName?: string;
  order?: number | null;
};

const BASEMAP_MODE_STORAGE_KEY = "twf.map.basemap_mode";
const MODEL_ORDER_BY_ID: Record<string, number> = {
  hrrr: 0,
  gfs: 1,
};

function readBasemapModePreference(): BasemapMode {
  if (typeof window === "undefined") {
    return "light";
  }
  try {
    const stored = window.localStorage.getItem(BASEMAP_MODE_STORAGE_KEY);
    return stored === "dark" ? "dark" : "light";
  } catch {
    return "light";
  }
}

function writeBasemapModePreference(mode: BasemapMode): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(BASEMAP_MODE_STORAGE_KEY, mode);
  } catch {
    // Ignore storage errors.
  }
}

function pickPreferred(values: string[], preferred: string): string {
  if (values.includes(preferred)) {
    return preferred;
  }
  return values[0] ?? "";
}

function makeRegionLabel(id: string, preset?: RegionPreset): string {
  return preset?.label ?? id.toUpperCase();
}

function makeVariableLabel(id: string, preferredLabel?: string | null): string {
  if (preferredLabel && preferredLabel.trim()) {
    return preferredLabel.trim();
  }
  return id;
}

function toNumberOrNull(value: unknown): number | null {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function variableDefaultFh(entry?: CapabilityVariable | null): number | null {
  const defaultFh = toNumberOrNull(entry?.default_fh);
  if (defaultFh !== null) {
    return defaultFh;
  }
  const minFh = toNumberOrNull(entry?.constraints?.min_fh);
  if (minFh !== null) {
    return minFh;
  }
  return null;
}

function modelOrderById(id: string): number | null {
  const normalized = id.trim().toLowerCase();
  return Number.isFinite(MODEL_ORDER_BY_ID[normalized]) ? MODEL_ORDER_BY_ID[normalized] : null;
}

function normalizeModelRows(
  capabilities: CapabilitiesResponse | null | undefined,
  modelIds: string[]
): ModelEntry[] {
  if (!capabilities?.model_catalog || modelIds.length === 0) {
    return [];
  }

  const normalized: ModelEntry[] = [];
  for (const id of modelIds) {
    const normalizedId = String(id).trim();
    const capability = capabilities.model_catalog[normalizedId];
    if (!normalizedId || !capability) {
      continue;
    }
    normalized.push({
      id: normalizedId,
      displayName: capability.name?.trim() || undefined,
      order: modelOrderById(normalizedId),
    });
  }

  return normalized.sort((a, b) => {
    const aOrder = Number.isFinite(a.order) ? Number(a.order) : Number.POSITIVE_INFINITY;
    const bOrder = Number.isFinite(b.order) ? Number(b.order) : Number.POSITIVE_INFINITY;
    if (aOrder !== bOrder) {
      return aOrder - bOrder;
    }
    return a.id.localeCompare(b.id);
  });
}

function normalizeCapabilityVarRows(modelCapability: CapabilityModel | null | undefined): VariableEntry[] {
  if (!modelCapability?.variables) {
    return [];
  }
  const normalized: VariableEntry[] = Object.entries(modelCapability.variables)
    .map(([id, entry]) => ({
      id: String(id).trim(),
      displayName: entry.display_name?.trim() || undefined,
      order: toNumberOrNull(entry.order),
      defaultFh: variableDefaultFh(entry),
      buildable: entry.buildable !== false,
      kind: typeof entry.kind === "string" ? entry.kind : null,
    }))
    .filter((entry) => Boolean(entry.id) && entry.buildable);

  return normalized.sort((a, b) => {
    const aOrder = Number.isFinite(a.order) ? Number(a.order) : Number.POSITIVE_INFINITY;
    const bOrder = Number.isFinite(b.order) ? Number(b.order) : Number.POSITIVE_INFINITY;
    if (aOrder !== bOrder) {
      return aOrder - bOrder;
    }
    return a.id.localeCompare(b.id);
  });
}

function capabilityVarsForManifest(
  manifestVars: RunManifestResponse["variables"] | null | undefined,
  capabilityVars: VariableEntry[]
): VariableEntry[] {
  if (!manifestVars) {
    return capabilityVars;
  }
  const manifestKeys = Object.keys(manifestVars);
  if (manifestKeys.length === 0) {
    return [];
  }
  const manifestSet = new Set(manifestKeys);
  const known = capabilityVars.filter((entry) => manifestSet.has(entry.id));
  const knownSet = new Set(known.map((entry) => entry.id));
  const extras = normalizeManifestVarRows(manifestVars).filter((entry) => !knownSet.has(entry.id));
  return [...known, ...extras];
}

function normalizeManifestVarRows(
  variables: RunManifestResponse["variables"] | null | undefined
): VariableEntry[] {
  if (!variables) {
    return [];
  }
  const normalized: VariableEntry[] = [];
  for (const [id, entry] of Object.entries(variables)) {
    const normalizedId = String(id ?? "").trim();
    if (!normalizedId) {
      continue;
    }
    const displayName = entry?.display_name ?? entry?.name ?? entry?.label;
    normalized.push({ id: normalizedId, displayName: displayName?.trim() || undefined });
  }
  return normalized;
}

function makeVariableOptions(entries: VariableEntry[]): Option[] {
  return entries.map((entry) => ({
    value: entry.id,
    label: makeVariableLabel(entry.id, entry.displayName),
  }));
}

function resolveManifestFrames(
  manifest: RunManifestResponse | null | undefined,
  varKey: string
): { rows: FrameRow[]; hasFrameList: boolean } {
  if (!manifest || !varKey) {
    return { rows: [], hasFrameList: false };
  }
  const varEntry = manifest.variables?.[varKey];
  if (!varEntry || !Array.isArray(varEntry.frames)) {
    return { rows: [], hasFrameList: false };
  }

  const rows: FrameRow[] = [];
  for (const frame of varEntry.frames) {
    const fh = Number(frame?.fh);
    if (!Number.isFinite(fh)) {
      continue;
    }
    rows.push({
      fh,
      has_cog: true,
      run: manifest.run,
    });
  }
  rows.sort((a, b) => Number(a.fh) - Number(b.fh));
  return { rows, hasFrameList: true };
}

function mergeManifestRowsWithPrevious(manifestRows: FrameRow[], previousRows: FrameRow[]): FrameRow[] {
  if (manifestRows.length === 0 || previousRows.length === 0) {
    return manifestRows;
  }

  const previousByHour = new Map<number, FrameRow>();
  for (const row of previousRows) {
    const fh = Number(row.fh);
    if (Number.isFinite(fh)) {
      previousByHour.set(fh, row);
    }
  }

  return manifestRows.map((row) => {
    const previous = previousByHour.get(Number(row.fh));
    if (!previous) {
      return row;
    }
    return {
      ...row,
      meta: row.meta ?? previous.meta,
      tile_url_template: row.tile_url_template ?? previous.tile_url_template,
      loop_webp_url: row.loop_webp_url ?? previous.loop_webp_url,
      loop_webp_tier0_url: row.loop_webp_tier0_url ?? previous.loop_webp_tier0_url,
      loop_webp_tier1_url: row.loop_webp_tier1_url ?? previous.loop_webp_tier1_url,
    };
  });
}

function extractLegendMeta(row: FrameRow | null | undefined): LegendMeta | null {
  const rawMeta = row?.meta?.meta ?? null;
  if (!rawMeta) return null;
  const nested = (rawMeta as { meta?: LegendMeta | null }).meta;
  return nested ?? (rawMeta as LegendMeta);
}

function nearestFrame(frames: number[], current: number): number {
  if (frames.length === 0) return 0;
  if (frames.includes(current)) return current;
  return frames.reduce((nearest, value) => {
    const nearestDelta = Math.abs(nearest - current);
    const valueDelta = Math.abs(value - current);
    return valueDelta < nearestDelta ? value : nearest;
  }, frames[0]);
}

function selectableFramesForVariable(frames: number[], preferredFh: number | null | undefined): number[] {
  if (frames.length === 0) {
    return frames;
  }
  if (!Number.isFinite(preferredFh)) {
    return frames;
  }
  const minimumFh = Number(preferredFh);
  const filtered = frames.filter((fh) => fh >= minimumFh);
  return filtered.length > 0 ? filtered : frames;
}

function preferredInitialFrame(frames: number[], preferredFh: number | null | undefined): number {
  if (frames.length === 0) {
    return 0;
  }
  if (!Number.isFinite(preferredFh)) {
    return frames[0];
  }
  return nearestFrame(frames, Number(preferredFh));
}

function resolveForecastHour(frames: number[], current: number, preferredFh: number | null | undefined): number {
  const selectableFrames = selectableFramesForVariable(frames, preferredFh);
  if (selectableFrames.length === 0) {
    return 0;
  }
  if (Number.isFinite(current)) {
    return nearestFrame(selectableFrames, current);
  }
  return preferredInitialFrame(selectableFrames, preferredFh);
}

function getEffectiveZoom(zoom: number): number {
  const dpr = typeof window === "undefined" ? 1 : Math.max(1, window.devicePixelRatio || 1);
  return zoom + Math.log2(dpr);
}

function nextRenderModeByHysteresis(current: RenderModeState, effectiveZoom: number): RenderModeState {
  const { tier0Max, tier1Max, hysteresis } = WEBP_RENDER_MODE_THRESHOLDS;

  if (current === "webp_tier0") {
    if (effectiveZoom > tier0Max + hysteresis) {
      return effectiveZoom > tier1Max + hysteresis ? "tiles" : "webp_tier1";
    }
    return "webp_tier0";
  }

  if (current === "webp_tier1") {
    if (effectiveZoom <= tier0Max - hysteresis) {
      return "webp_tier0";
    }
    if (effectiveZoom > tier1Max + hysteresis) {
      return "tiles";
    }
    return "webp_tier1";
  }

  if (effectiveZoom <= tier1Max - hysteresis) {
    return effectiveZoom <= tier0Max - hysteresis ? "webp_tier0" : "webp_tier1";
  }
  return "tiles";
}

async function preloadLoopFrame(
  url: string,
  signal?: AbortSignal
): Promise<{ ok: boolean; bitmap: ImageBitmap | null; bytes: number; readyMs: number; fetchMs: number; decodeMs: number }> {
  const startedAt = performance.now();
  try {
    const fetchStart = performance.now();
    const response = await fetch(url, {
      credentials: "omit",
      signal,
      cache: "force-cache",
    });
    const fetchEnd = performance.now();
    if (!response.ok) {
      return { ok: false, bitmap: null, bytes: 0, readyMs: 0, fetchMs: 0, decodeMs: 0 };
    }
    const blob = await response.blob();
    if (typeof createImageBitmap !== "function") {
      const readyEnd = performance.now();
      return {
        ok: true,
        bitmap: null,
        bytes: 0,
        readyMs: Math.max(0, Math.round(readyEnd - startedAt)),
        fetchMs: Math.max(0, Math.round(fetchEnd - fetchStart)),
        decodeMs: 0,
      };
    }
    const decodeStart = performance.now();
    const bitmap = await createImageBitmap(blob);
    const decodeEnd = performance.now();
    return {
      ok: true,
      bitmap,
      bytes: bitmap.width * bitmap.height * 4,
      readyMs: Math.max(0, Math.round(decodeEnd - startedAt)),
      fetchMs: Math.max(0, Math.round(fetchEnd - fetchStart)),
      decodeMs: Math.max(0, Math.round(decodeEnd - decodeStart)),
    };
  } catch {
    return { ok: false, bitmap: null, bytes: 0, readyMs: 0, fetchMs: 0, decodeMs: 0 };
  }
}

function runIdToIso(runId: string | null): string | null {
  if (!runId) return null;
  const match = runId.match(/^(\d{4})(\d{2})(\d{2})_(\d{2})z$/i);
  if (!match) return null;
  const [, year, month, day, hour] = match;
  return new Date(Date.UTC(Number(year), Number(month) - 1, Number(day), Number(hour), 0, 0)).toISOString();
}

function isPrecipPtypeLegendMeta(
  meta: LegendMeta & { var_key?: string; spec_key?: string; id?: string; var?: string }
): boolean {
  const kind = String(meta.kind ?? "").toLowerCase();
  const id = String(meta.var_key ?? meta.spec_key ?? meta.id ?? meta.var ?? "").toLowerCase();
  return kind.includes("precip_ptype") || id === "precip_ptype";
}

function withPrecipRateUnits(title: string, units?: string): string {
  const resolvedUnits = (units ?? "").trim();
  if (!resolvedUnits) {
    return title;
  }
  const lowerTitle = title.toLowerCase();
  const lowerUnits = resolvedUnits.toLowerCase();
  if (lowerTitle.includes(`(${lowerUnits})`)) {
    return title;
  }
  return `${title} (${resolvedUnits})`;
}

function normalizeLegendUnits(
  units: string | undefined,
  meta: LegendMeta & { var_key?: string; spec_key?: string; id?: string; var?: string }
): string | undefined {
  const resolved = (units ?? "").trim();
  if (resolved.toLowerCase() !== "index") {
    return units;
  }
  const id = String(meta.var_key ?? meta.spec_key ?? meta.id ?? meta.var ?? "").toLowerCase();
  if (id === "radar_ptype") {
    return "dBZ";
  }
  return units;
}

function buildLegend(meta: LegendMeta | null | undefined, opacity: number): LegendPayload | null {
  if (!meta) {
    return null;
  }
  const metaWithIds = meta as LegendMeta & { var_key?: string; spec_key?: string; id?: string; var?: string };
  const isPrecipPtype = isPrecipPtypeLegendMeta(metaWithIds);
  const baseTitle = meta.legend_title ?? meta.display_name ?? "Legend";
  const title = isPrecipPtype ? withPrecipRateUnits(baseTitle, meta.units) : baseTitle;
  const units = normalizeLegendUnits(meta.units, metaWithIds);
  const legendMetadata = {
    kind: metaWithIds.kind,
    id: metaWithIds.var_key ?? metaWithIds.spec_key ?? metaWithIds.id ?? metaWithIds.var,
    ptype_breaks: metaWithIds.ptype_breaks,
    ptype_order: metaWithIds.ptype_order,
    bins_per_ptype: metaWithIds.bins_per_ptype,
  };

  // V3 sidecar format: meta.legend.stops = [[value, color], ...]
  const resolvedStops = meta.legend_stops ?? meta.legend?.stops;
  if (Array.isArray(resolvedStops) && resolvedStops.length > 0) {
    const entries = resolvedStops
      .map(([value, color]) => ({ value: Number(value), color }))
      .filter((entry) => Number.isFinite(entry.value));
    if (entries.length === 0) {
      return null;
    }
    return {
      title,
      units,
      entries,
      opacity,
      ...legendMetadata,
    };
  }

  const hasPtypeSegments =
    Array.isArray(meta.ptype_order) && Boolean(meta.ptype_breaks) && Boolean(meta.ptype_levels);

  if (
    Array.isArray(meta.colors) &&
    meta.colors.length > 1 &&
    Array.isArray(meta.range) &&
    meta.range.length === 2 &&
    !hasPtypeSegments
  ) {
    const [min, max] = meta.range;
    const entries = meta.colors.map((color, index) => {
      const denom = Math.max(1, meta.colors!.length - 1);
      const value = min + ((max - min) * index) / denom;
      return { value, color };
    });
    return {
      title,
      units,
      entries,
      opacity,
      ...legendMetadata,
    };
  }

  if (Array.isArray(meta.colors) && meta.colors.length > 0) {
    const entries: Array<{ value: number; color: string }> = [];

    if (Array.isArray(meta.ptype_order) && meta.ptype_breaks && meta.ptype_levels) {
      for (const ptype of meta.ptype_order) {
        const ptypeBreak = meta.ptype_breaks[ptype];
        const ptypeLevels = meta.ptype_levels[ptype];
        if (!ptypeBreak || !Array.isArray(ptypeLevels)) {
          continue;
        }
        const offset = Number(ptypeBreak.offset);
        const count = Number(ptypeBreak.count);
        if (!Number.isFinite(offset) || !Number.isFinite(count) || offset < 0 || count <= 0) {
          continue;
        }
        const maxItems = Math.min(count, ptypeLevels.length, meta.colors.length - offset);
        for (let index = 0; index < maxItems; index += 1) {
          const value = Number(ptypeLevels[index]);
          const color = meta.colors[offset + index];
          if (!Number.isFinite(value) || !color) {
            continue;
          }
          entries.push({ value, color });
        }
      }
    }

    if (entries.length === 0 && Array.isArray(meta.levels) && meta.levels.length > 0) {
      const maxItems = Math.min(meta.levels.length, meta.colors.length);
      for (let index = 0; index < maxItems; index += 1) {
        const value = Number(meta.levels[index]);
        const color = meta.colors[index];
        if (!Number.isFinite(value) || !color) {
          continue;
        }
        entries.push({ value, color });
      }
    }

    if (entries.length > 0) {
      return {
        title,
        units,
        entries,
        opacity,
        ...legendMetadata,
      };
    }
  }

  return null;
}

export default function App() {
  const webpDefaultEnabled = isWebpDefaultRenderEnabled();
  const [capabilities, setCapabilities] = useState<CapabilitiesResponse | null>(null);
  const [models, setModels] = useState<Option[]>([]);
  const [regions, setRegions] = useState<Option[]>([]);
  const [runs, setRuns] = useState<string[]>([]);
  const [variables, setVariables] = useState<Option[]>([]);
  const [frameRows, setFrameRows] = useState<FrameRow[]>([]);
  const [runManifest, setRunManifest] = useState<RunManifestResponse | null>(null);
  const [loopManifest, setLoopManifest] = useState<LoopManifestResponse | null>(null);
  const [regionPresets, setRegionPresets] = useState<Record<string, RegionPreset>>({});

  const [model, setModel] = useState("");
  const [region, setRegion] = useState(MAP_VIEW_DEFAULTS.region);
  const [run, setRun] = useState("latest");
  const [variable, setVariable] = useState("");
  const [forecastHour, setForecastHour] = useState(Number.POSITIVE_INFINITY);
  const [targetForecastHour, setTargetForecastHour] = useState(Number.POSITIVE_INFINITY);
  const [, setZoomBucket] = useState(Math.round(MAP_VIEW_DEFAULTS.zoom));
  const [mapZoom, setMapZoom] = useState(MAP_VIEW_DEFAULTS.zoom);
  const [zoomGestureActive, setZoomGestureActive] = useState(false);
  const [isPlaying, setIsPlaying] = useState(false);
  const [renderMode, setRenderMode] = useState<RenderModeState>(webpDefaultEnabled ? "webp_tier0" : "tiles");
  const [visibleRenderMode, setVisibleRenderMode] = useState<RenderModeState>(webpDefaultEnabled ? "webp_tier0" : "tiles");
  const [loopDisplayHour, setLoopDisplayHour] = useState<number | null>(null);
  const [isLoopPreloading, setIsLoopPreloading] = useState(false);
  const [isLoopAutoplayBuffering, setIsLoopAutoplayBuffering] = useState(false);
  const [loopProgress, setLoopProgress] = useState({ total: 0, ready: 0, failed: 0 });
  const [loopBaseForecastHour, setLoopBaseForecastHour] = useState<number | null>(null);
  const [isPreloadingForPlay, setIsPreloadingForPlay] = useState(false);
  const [isScrubbing, setIsScrubbing] = useState(false);
  const [scrubRequestedHour, setScrubRequestedHour] = useState<number | null>(null);
  const [opacity, setOpacity] = useState(OVERLAY_DEFAULT_OPACITY);
  const [basemapMode, setBasemapMode] = useState<BasemapMode>(() => readBasemapModePreference());
  const [isPageVisible, setIsPageVisible] = useState(() =>
    typeof document === "undefined" ? true : !document.hidden
  );
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [settledTileUrl, setSettledTileUrl] = useState<string | null>(null);
  const [mapLoadingTileUrl, setMapLoadingTileUrl] = useState<string | null>(null);
  const [frameStatusMessage, setFrameStatusMessage] = useState<string | null>(null);
  const [showZoomHint, setShowZoomHint] = useState(false);
  const [bufferSnapshot, setBufferSnapshot] = useState<BufferSnapshot>({
    totalFrames: 0,
    bufferedCount: 0,
    bufferedAheadCount: 0,
    terminalCount: 0,
    terminalAheadCount: 0,
    failedCount: 0,
    inFlightCount: 0,
    queueDepth: 0,
    statusText: "Buffered 0/0",
    version: 0,
  });
  const latestTileUrlRef = useRef<string>("");
  const readyTileUrlsRef = useRef<Map<string, number>>(new Map());
  const readyFramesRef = useRef<Set<number>>(new Set());
  const inFlightFramesRef = useRef<Set<number>>(new Set());
  const failedFramesRef = useRef<Set<number>>(new Set());
  const frameRetryCountRef = useRef<Map<number, number>>(new Map());
  const frameCycleStartedAtRef = useRef<Map<number, number>>(new Map());
  const frameNextRetryAtRef = useRef<Map<number, number>>(new Map());
  const inFlightStartedAtRef = useRef<Map<number, number>>(new Map());
  const readyLatencyStatsRef = useRef({ totalMs: 0, count: 0 });
  const bufferVersionRef = useRef(0);
  // Tracks a pending RAF for coalescing bufferSnapshot updates (see markFrameReady).
  const bufferSnapshotRafRef = useRef<number | null>(null);
  // Stores the last committed snapshot stats so unchanged updates are skipped entirely.
  const lastSnapshotStatsRef = useRef({ bufferedCount: -1, failedCount: -1, inFlightCount: -1, queueDepth: -1 });
  const datasetGenerationRef = useRef(0);
  const requestGenerationRef = useRef(0);
  const scrubRafRef = useRef<number | null>(null);
  const pendingScrubHourRef = useRef<number | null>(null);
  const autoplayPrimedRef = useRef(false);
  const frameStatusTimerRef = useRef<number | null>(null);
  const preloadProgressRef = useRef({
    lastBufferedCount: 0,
    lastProgressAt: 0,
  });
  const loopPreloadTokenRef = useRef(0);
  const loopReadyHoursRef = useRef<Set<number>>(new Set());
  const loopFailedHoursRef = useRef<Set<number>>(new Set());
  const mapZoomRef = useRef(MAP_VIEW_DEFAULTS.zoom);
  const renderModeDwellTimerRef = useRef<number | null>(null);
  const transitionTokenRef = useRef(0);
  const lastTileViewportCommitUrlRef = useRef<string | null>(null);
  const loopDisplayDecodeTokenRef = useRef(0);
  const loopDisplayDecodeAbortRef = useRef<AbortController | null>(null);
  const loopDecodedCacheRef = useRef<Map<string, { bitmap: ImageBitmap; bytes: number; lastUsedAt: number }>>(new Map());
  const loopDecodedCacheBytesRef = useRef(0);
  const loopDecodedCacheHighWaterRef = useRef(0);
  const loopDecodeReadySamplesRef = useRef<number[]>([]);
  const loopDecodeFetchSamplesRef = useRef<number[]>([]);
  const loopDecodeOnlySamplesRef = useRef<number[]>([]);
  const tierFailoverCycleRef = useRef<{ key: string; emitted: boolean }>({ key: "", emitted: false });
  const runsLoadedForModelRef = useRef<string>("");
  // Pre-built Set of valid forecast hours, kept in sync with frameHours.
  // updateBufferSnapshot reads from this ref instead of constructing a new Set
  // on every tile event (which fired 20-40×/sec during animation).
  const frameSetRef = useRef<Set<number>>(new Set());

  useEffect(() => {
    writeBasemapModePreference(basemapMode);
  }, [basemapMode]);

  const modelCatalog = capabilities?.model_catalog ?? {};
  const selectedModelCapability: CapabilityModel | null = model ? modelCatalog[model] ?? null : null;
  const selectedCapabilityVars = useMemo(
    () => normalizeCapabilityVarRows(selectedModelCapability),
    [selectedModelCapability]
  );
  const selectedCapabilityVarMap = useMemo(() => {
    const map = new Map<string, VariableEntry>();
    for (const entry of selectedCapabilityVars) {
      map.set(entry.id, entry);
    }
    return map;
  }, [selectedCapabilityVars]);
  const hasRenderableSelection = Boolean(model && variable && selectedCapabilityVarMap.has(variable));
  const selectedVariableDefaultFh = selectedCapabilityVarMap.get(variable)?.defaultFh ?? null;
  const selectedVariableKind = selectedCapabilityVarMap.get(variable)?.kind ?? null;
  const selectedModelConstraints = (selectedModelCapability?.constraints ?? {}) as Record<string, unknown>;
  const zoomHintMinZoom = toNumberOrNull(selectedModelConstraints.zoom_hint_min);
  const overlayFadeOutZoom = useMemo(() => {
    const start = toNumberOrNull(selectedModelConstraints.overlay_fade_out_zoom_start);
    const end = toNumberOrNull(selectedModelConstraints.overlay_fade_out_zoom_end);
    if (start === null || end === null || end <= start) {
      return null;
    }
    return { start, end };
  }, [selectedModelConstraints.overlay_fade_out_zoom_start, selectedModelConstraints.overlay_fade_out_zoom_end]);

  const frameHours = useMemo(() => {
    const hours = frameRows.map((row) => Number(row.fh)).filter(Number.isFinite);
    return Array.from(new Set(hours)).sort((a, b) => a - b);
  }, [frameRows]);

  const selectableFrameHours = useMemo(
    () => selectableFramesForVariable(frameHours, selectedVariableDefaultFh),
    [frameHours, selectedVariableDefaultFh]
  );

  // Keep frameSetRef in sync so updateBufferSnapshot never allocates a one-off Set.
  useEffect(() => {
    frameSetRef.current = new Set(frameHours);
  }, [frameHours]);

  const frameByHour = useMemo(() => {
    return new Map(frameRows.map((row) => [Number(row.fh), row]));
  }, [frameRows]);

  const regionViews = useMemo(() => {
    return Object.fromEntries(
      Object.entries(regionPresets).map(([id, preset]) => [
        id,
        {
          center: [preset.defaultCenter[0], preset.defaultCenter[1]] as [number, number],
          zoom: preset.defaultZoom,
          bbox: preset.bbox,
          minZoom: preset.minZoom,
          maxZoom: preset.maxZoom,
        },
      ])
    );
  }, [regionPresets]);

  const currentFrame = frameByHour.get(forecastHour) ?? frameRows[0] ?? null;
  const latestRunId = useMemo(() => {
    const manifestLatest =
      run === "latest" && runManifest?.model === model ? (runManifest.run ?? null) : null;
    const availabilityLatest =
      model && capabilities?.availability?.[model]
        ? (capabilities.availability[model].latest_run ?? null)
        : null;
    return manifestLatest ?? availabilityLatest ?? runs[0] ?? frameRows[0]?.run ?? null;
  }, [run, runManifest, model, capabilities, runs, frameRows]);
  const resolvedRunForRequests = run === "latest" ? (latestRunId ?? "latest") : run;
  const apiRoot = API_ORIGIN.replace(/\/$/, "");

  const runOptions = useMemo<Option[]>(() => {
    return buildRunOptions(runs, latestRunId);
  }, [runs, latestRunId]);

  const loopFrameTier0FallbackByHour = useMemo(() => {
    const map = new Map<number, string>();
    for (const row of frameRows) {
      const fh = Number(row?.fh);
      const loopUrl = row?.loop_webp_tier0_url ?? row?.loop_webp_url;
      if (!Number.isFinite(fh) || !loopUrl) {
        continue;
      }
      const absolute = /^https?:\/\//i.test(loopUrl)
        ? loopUrl
        : `${apiRoot}${loopUrl.startsWith("/") ? "" : "/"}${loopUrl}`;
      map.set(fh, absolute);
    }
    return map;
  }, [apiRoot, frameRows]);

  const loopTier0UrlByHour = useMemo(() => {
    const map = new Map<number, string>(loopFrameTier0FallbackByHour);
    const tier0 = loopManifest?.loop_tiers.find((entry) => Number(entry?.tier) === 0);
    const frames = Array.isArray(tier0?.frames) ? tier0.frames : [];
    for (const frame of frames) {
      const fh = Number(frame?.fh);
      const loopUrl = frame?.url;
      if (!Number.isFinite(fh) || !loopUrl) {
        continue;
      }
      const absolute = /^https?:\/\//i.test(loopUrl)
        ? loopUrl
        : `${apiRoot}${loopUrl.startsWith("/") ? "" : "/"}${loopUrl}`;
      map.set(fh, absolute);
    }
    return map;
  }, [apiRoot, loopFrameTier0FallbackByHour, loopManifest]);

  const loopTier1UrlByHour = useMemo(() => {
    const map = new Map<number, string>();
    for (const row of frameRows) {
      const fh = Number(row?.fh);
      const loopUrl = row?.loop_webp_tier1_url;
      if (!Number.isFinite(fh) || !loopUrl) {
        continue;
      }
      const absolute = /^https?:\/\//i.test(loopUrl)
        ? loopUrl
        : `${apiRoot}${loopUrl.startsWith("/") ? "" : "/"}${loopUrl}`;
      map.set(fh, absolute);
    }
    const tier1 = loopManifest?.loop_tiers.find((entry) => Number(entry?.tier) === 1);
    const frames = Array.isArray(tier1?.frames) ? tier1.frames : [];
    for (const frame of frames) {
      const fh = Number(frame?.fh);
      const loopUrl = frame?.url;
      if (!Number.isFinite(fh) || !loopUrl) {
        continue;
      }
      const absolute = /^https?:\/\//i.test(loopUrl)
        ? loopUrl
        : `${apiRoot}${loopUrl.startsWith("/") ? "" : "/"}${loopUrl}`;
      map.set(fh, absolute);
    }
    return map;
  }, [apiRoot, frameRows, loopManifest]);

  const loopUrlByHour = useMemo(() => new Map(loopTier0UrlByHour), [loopTier0UrlByHour]);

  const loopFrameHours = useMemo(() => {
    return Array.from(loopTier0UrlByHour.keys()).sort((a, b) => a - b);
  }, [loopTier0UrlByHour]);

  const resolvedLoopForecastHour = useMemo(() => {
    if (loopFrameHours.length === 0) {
      return forecastHour;
    }
    return nearestFrame(loopFrameHours, forecastHour);
  }, [loopFrameHours, forecastHour]);

  const resolveLoopUrlForHour = useCallback(
    (fh: number, preferredMode: RenderModeState): string | null => {
      if (preferredMode === "webp_tier1") {
        return loopTier1UrlByHour.get(fh) ?? loopTier0UrlByHour.get(fh) ?? null;
      }
      return loopTier0UrlByHour.get(fh) ?? loopUrlByHour.get(fh) ?? null;
    },
    [loopTier0UrlByHour, loopTier1UrlByHour, loopUrlByHour]
  );

  const webpDecodeCacheBudgetBytes = useMemo(() => {
    if (typeof navigator === "undefined") {
      return WEBP_DECODE_CACHE_BUDGET_DESKTOP_BYTES;
    }
    const isMobile = /android|iphone|ipad|ipod|mobile/i.test(navigator.userAgent);
    return isMobile ? WEBP_DECODE_CACHE_BUDGET_MOBILE_BYTES : WEBP_DECODE_CACHE_BUDGET_DESKTOP_BYTES;
  }, []);

  const loopCacheKey = useCallback(
    (fh: number, mode: RenderModeState) => {
      return `${model}:${resolvedRunForRequests}:${variable}:${mode}:${fh}`;
    },
    [model, resolvedRunForRequests, variable]
  );

  const upsertLoopDecodedCache = useCallback(
    (key: string, bitmap: ImageBitmap, bytes: number) => {
      const now = Date.now();
      const cache = loopDecodedCacheRef.current;
      const previous = cache.get(key);
      if (previous) {
        loopDecodedCacheBytesRef.current -= previous.bytes;
        previous.bitmap.close();
      }
      cache.set(key, { bitmap, bytes, lastUsedAt: now });
      loopDecodedCacheBytesRef.current += bytes;
      if (loopDecodedCacheBytesRef.current > loopDecodedCacheHighWaterRef.current) {
        loopDecodedCacheHighWaterRef.current = loopDecodedCacheBytesRef.current;
      }

      while (loopDecodedCacheBytesRef.current > webpDecodeCacheBudgetBytes && cache.size > 1) {
        let lruKey: string | null = null;
        let oldest = Number.POSITIVE_INFINITY;
        for (const [candidateKey, candidate] of cache.entries()) {
          if (candidate.lastUsedAt < oldest) {
            oldest = candidate.lastUsedAt;
            lruKey = candidateKey;
          }
        }
        if (!lruKey || lruKey === key) {
          break;
        }
        const evicted = cache.get(lruKey);
        if (!evicted) {
          break;
        }
        evicted.bitmap.close();
        loopDecodedCacheBytesRef.current -= evicted.bytes;
        cache.delete(lruKey);
      }
    },
    [webpDecodeCacheBudgetBytes]
  );

  const ensureLoopFrameDecoded = useCallback(
    async (fh: number, mode: RenderModeState, signal?: AbortSignal): Promise<boolean> => {
      if (mode === "tiles") {
        return false;
      }
      const key = loopCacheKey(fh, mode);
      const cached = loopDecodedCacheRef.current.get(key);
      if (cached) {
        cached.lastUsedAt = Date.now();
        loopReadyHoursRef.current.add(fh);
        return true;
      }

      const url = resolveLoopUrlForHour(fh, mode);
      if (!url) {
        return false;
      }

      const decoded = await preloadLoopFrame(url, signal);
      if (!decoded.ok) {
        return false;
      }
      if (decoded.readyMs > 0) {
        const readySamples = loopDecodeReadySamplesRef.current;
        readySamples.push(decoded.readyMs);
        if (readySamples.length > 256) {
          readySamples.splice(0, readySamples.length - 256);
        }
      }
      if (decoded.fetchMs > 0) {
        const fetchSamples = loopDecodeFetchSamplesRef.current;
        fetchSamples.push(decoded.fetchMs);
        if (fetchSamples.length > 256) {
          fetchSamples.splice(0, fetchSamples.length - 256);
        }
      }
      if (decoded.decodeMs > 0) {
        const decodeSamples = loopDecodeOnlySamplesRef.current;
        decodeSamples.push(decoded.decodeMs);
        if (decodeSamples.length > 256) {
          decodeSamples.splice(0, decodeSamples.length - 256);
        }
      }
      if (decoded.bitmap) {
        upsertLoopDecodedCache(key, decoded.bitmap, decoded.bytes);
      }
      loopReadyHoursRef.current.add(fh);
      return true;
    },
    [loopCacheKey, resolveLoopUrlForHour, upsertLoopDecodedCache]
  );

  const hasDecodedLoopFrame = useCallback(
    (fh: number, mode: RenderModeState): boolean => {
      if (mode === "tiles") {
        return false;
      }
      return loopDecodedCacheRef.current.has(loopCacheKey(fh, mode));
    },
    [loopCacheKey]
  );

  const countAheadReadyLoopFrames = useCallback(
    (currentHour: number, mode: RenderModeState, maxAhead: number): number => {
      if (mode === "tiles" || loopFrameHours.length === 0 || maxAhead <= 0) {
        return 0;
      }
      const currentIndex = loopFrameHours.indexOf(currentHour);
      if (currentIndex < 0) {
        return 0;
      }

      let ready = 0;
      const endIndex = Math.min(loopFrameHours.length - 1, currentIndex + maxAhead);
      for (let index = currentIndex + 1; index <= endIndex; index += 1) {
        const fh = loopFrameHours[index];
        if (hasDecodedLoopFrame(fh, mode)) {
          ready += 1;
        }
      }
      return ready;
    },
    [loopFrameHours, hasDecodedLoopFrame]
  );

  const canUseLoopPlayback = useMemo(() => {
    if (loopFrameHours.length <= 1) {
      return false;
    }
    return loopFrameHours.every((fh) => Boolean(loopTier0UrlByHour.get(fh) ?? loopUrlByHour.get(fh)));
  }, [loopFrameHours, loopTier0UrlByHour, loopUrlByHour]);
  const isHighDetailZoom = useMemo(() => {
    const effectiveZoom = getEffectiveZoom(mapZoom);
    const highDetailCutoff = WEBP_RENDER_MODE_THRESHOLDS.tier1Max + WEBP_RENDER_MODE_THRESHOLDS.hysteresis;
    return effectiveZoom > highDetailCutoff;
  }, [mapZoom]);

  useEffect(() => {
    mapZoomRef.current = mapZoom;
  }, [mapZoom]);

  useEffect(() => {
    if (!webpDefaultEnabled || renderMode !== "webp_tier1") {
      tierFailoverCycleRef.current = { key: "", emitted: false };
      return;
    }

    const cycleKey = `${model}:${resolvedRunForRequests}:${variable}:webp_tier1`;
    if (tierFailoverCycleRef.current.key !== cycleKey) {
      tierFailoverCycleRef.current = { key: cycleKey, emitted: false };
    }

    if (tierFailoverCycleRef.current.emitted) {
      return;
    }

    const hasTier1 = Boolean(loopTier1UrlByHour.get(forecastHour));
    const hasTier0 = Boolean(loopTier0UrlByHour.get(forecastHour) ?? loopUrlByHour.get(forecastHour));
    if (!hasTier1 && hasTier0) {
      tierFailoverCycleRef.current = { key: cycleKey, emitted: true };
    }
  }, [
    webpDefaultEnabled,
    renderMode,
    model,
    resolvedRunForRequests,
    variable,
    forecastHour,
    loopTier1UrlByHour,
    loopTier0UrlByHour,
    loopUrlByHour,
  ]);

  useEffect(() => {
    const clearDwellTimer = () => {
      if (renderModeDwellTimerRef.current !== null) {
        window.clearTimeout(renderModeDwellTimerRef.current);
        renderModeDwellTimerRef.current = null;
      }
    };

    if (!webpDefaultEnabled || !canUseLoopPlayback) {
      clearDwellTimer();
      if (renderMode !== "tiles") {
        setRenderMode("tiles");
      }
      return clearDwellTimer;
    }

    if (zoomGestureActive) {
      clearDwellTimer();
      return clearDwellTimer;
    }

    const effectiveZoom = getEffectiveZoom(mapZoom);
    const candidate = nextRenderModeByHysteresis(renderMode, effectiveZoom);
    if (candidate === renderMode) {
      clearDwellTimer();
      return clearDwellTimer;
    }

    clearDwellTimer();
    renderModeDwellTimerRef.current = window.setTimeout(() => {
      const latestEffectiveZoom = getEffectiveZoom(mapZoomRef.current);
      setRenderMode((current) => nextRenderModeByHysteresis(current, latestEffectiveZoom));
      renderModeDwellTimerRef.current = null;
    }, WEBP_RENDER_MODE_THRESHOLDS.dwellMs);

    return clearDwellTimer;
  }, [mapZoom, zoomGestureActive, renderMode, webpDefaultEnabled, canUseLoopPlayback]);

  useEffect(() => {
    transitionTokenRef.current += 1;

    if (!canUseLoopPlayback) {
      setVisibleRenderMode("tiles");
      setLoopDisplayHour(null);
      return;
    }

    if (renderMode === visibleRenderMode) {
      return;
    }

    if (renderMode === "tiles") {
      setVisibleRenderMode("tiles");
      setLoopDisplayHour(null);
      return;
    }

    if (!resolveLoopUrlForHour(resolvedLoopForecastHour, renderMode)) {
      setVisibleRenderMode("tiles");
      setLoopDisplayHour(null);
      return;
    }

    // No signal passed to ensureLoopFrameDecoded: the decode always runs to
    // completion so its result is stored in the LRU cache for immediate reuse
    // by playback or scrub paths. The token gates whether we actually commit
    // the visible mode change — preventing stale results from being applied.
    const token = transitionTokenRef.current;
    ensureLoopFrameDecoded(resolvedLoopForecastHour, renderMode)
      .then((ready) => {
        if (token !== transitionTokenRef.current) {
          return;
        }
        if (ready) {
          setVisibleRenderMode(renderMode);
          setLoopDisplayHour(resolvedLoopForecastHour);
        }
      })
      .catch(() => {
        // Decode failed; remain in current visible mode.
      });
  }, [
    renderMode,
    visibleRenderMode,
    canUseLoopPlayback,
    resolvedLoopForecastHour,
    resolveLoopUrlForHour,
    ensureLoopFrameDecoded,
  ]);

  const isLoopPlaybackLocked = renderMode !== "tiles" && canUseLoopPlayback && (isPlaying || isLoopPreloading);
  const isLoopDisplayActive = visibleRenderMode !== "tiles" && canUseLoopPlayback;
  const mapForecastHour = isLoopPlaybackLocked && Number.isFinite(loopBaseForecastHour)
    ? (loopBaseForecastHour as number)
    : forecastHour;

  const tileUrlForHour = useCallback(
    (fh: number): string => {
      if (!hasRenderableSelection) {
        return EMPTY_TILE_DATA_URL;
      }
      const fallbackFh = frameHours[0] ?? 0;
      const resolvedFh = Number.isFinite(fh) ? fh : fallbackFh;
      return buildTileUrlFromFrame({
        model,
        run: resolvedRunForRequests,
        varKey: variable,
        fh: resolvedFh,
        frameRow: frameByHour.get(resolvedFh) ?? frameRows[0] ?? null,
      });
    },
    [hasRenderableSelection, model, resolvedRunForRequests, variable, frameHours, frameByHour, frameRows]
  );

  const tileUrl = useMemo(() => {
    return tileUrlForHour(mapForecastHour);
  }, [tileUrlForHour, mapForecastHour]);

  const tileUrlToHour = useMemo(() => {
    const map = new Map<string, number>();
    for (const fh of frameHours) {
      map.set(tileUrlForHour(fh), fh);
    }
    return map;
  }, [frameHours, tileUrlForHour]);

  const playbackPolicy = useMemo(
    () =>
      getPlaybackBufferPolicy({
        totalFrames: frameHours.length,
        autoplayTickMs: AUTOPLAY_TICK_MS,
      }),
    [frameHours.length]
  );

  const updateBufferSnapshot = useCallback(() => {
    const totalFrames = frameHours.length;
    const ready = readyFramesRef.current;
    const inFlight = inFlightFramesRef.current;
    const failed = failedFramesRef.current;
    const now = Date.now();

    if (totalFrames === 0) {
      const version = ++bufferVersionRef.current;
      setBufferSnapshot({
        totalFrames: 0,
        bufferedCount: 0,
        bufferedAheadCount: 0,
        terminalCount: 0,
        terminalAheadCount: 0,
        failedCount: 0,
        inFlightCount: 0,
        queueDepth: 0,
        statusText: "Buffered 0/0",
        version,
      });
      return;
    }

    const frameSet = frameSetRef.current;
    for (const fh of ready) {
      if (!frameSet.has(fh)) {
        ready.delete(fh);
      }
    }
    for (const fh of failed) {
      if (!frameSet.has(fh)) {
        failed.delete(fh);
      }
    }
    for (const fh of inFlight) {
      if (!frameSet.has(fh) || ready.has(fh)) {
        inFlight.delete(fh);
        inFlightStartedAtRef.current.delete(fh);
        continue;
      }
      const startedAt = inFlightStartedAtRef.current.get(fh);
      if (Number.isFinite(startedAt) && now - (startedAt as number) > INFLIGHT_FRAME_TTL_MS) {
        const nextRetry = (frameRetryCountRef.current.get(fh) ?? 0) + 1;
        frameRetryCountRef.current.set(fh, nextRetry);
        const cycleStartedAt = frameCycleStartedAtRef.current.get(fh) ?? now;
        frameCycleStartedAtRef.current.set(fh, cycleStartedAt);
        const ageMs = now - cycleStartedAt;

        inFlight.delete(fh);
        inFlightStartedAtRef.current.delete(fh);
        if (nextRetry >= FRAME_MAX_RETRIES || ageMs >= FRAME_HARD_DEADLINE_MS) {
          failed.add(fh);
          frameNextRetryAtRef.current.delete(fh);
        } else {
          const retryDelayMs = FRAME_RETRY_BASE_MS * 2 ** (nextRetry - 1);
          frameNextRetryAtRef.current.set(fh, now + retryDelayMs);
          void retryDelayMs;
        }
      }
    }

    const currentIndex = frameHours.indexOf(forecastHour);
    let bufferedAheadCount = 0;
    let terminalAheadCount = 0;
    if (currentIndex >= 0) {
      for (let i = currentIndex + 1; i < frameHours.length; i += 1) {
        const hour = frameHours[i];
        if (ready.has(hour)) {
          bufferedAheadCount += 1;
        }
        if (ready.has(hour) || failed.has(hour)) {
          terminalAheadCount += 1;
        }
      }
    }

    const bufferedCount = ready.size;
    const failedCount = failed.size;
    const terminalCount = Math.min(totalFrames, bufferedCount + failedCount);
    const queueDepth = Math.max(0, totalFrames - terminalCount - inFlight.size);

    // Skip the React state update when the counts that drive UI and prefetchHours
    // are identical to the last committed snapshot. Tile events from prefetch sources
    // can fire 20-40×/sec during animation even when nothing meaningful has changed.
    const prev = lastSnapshotStatsRef.current;
    if (
      prev.bufferedCount === bufferedCount &&
      prev.failedCount === failedCount &&
      prev.inFlightCount === inFlight.size &&
      prev.queueDepth === queueDepth
    ) {
      return;
    }
    lastSnapshotStatsRef.current = { bufferedCount, failedCount, inFlightCount: inFlight.size, queueDepth };

    const version = ++bufferVersionRef.current;
    const snapshot = {
      totalFrames,
      bufferedCount,
      bufferedAheadCount,
      terminalCount,
      terminalAheadCount,
      failedCount,
      inFlightCount: inFlight.size,
      queueDepth,
      statusText: `Loaded ${terminalCount}/${totalFrames} (${bufferedCount} ready)`,
      version,
    };
    setBufferSnapshot(snapshot);
  }, [frameHours, forecastHour]);

  const contourGeoJsonUrl = useMemo(() => {
    if (!hasRenderableSelection || variable !== "tmp2m") {
      return null;
    }
    const frameMeta = extractLegendMeta(currentFrame);
    const contourSpec = frameMeta?.contours?.iso32f;
    if (!contourSpec) {
      return null;
    }
    return buildContourUrl({
      model,
      run: resolvedRunForRequests,
      varKey: variable,
      fh: mapForecastHour,
      key: "iso32f",
    });
  }, [currentFrame, hasRenderableSelection, model, resolvedRunForRequests, variable, mapForecastHour]);

  const legend = useMemo(() => {
    const normalizedMeta = extractLegendMeta(currentFrame) ?? extractLegendMeta(frameRows[0] ?? null);
    return buildLegend(normalizedMeta, opacity);
  }, [currentFrame, frameRows, opacity]);

  const prefetchHours = useMemo(() => {
    if (!hasRenderableSelection || isLoopDisplayActive || frameHours.length === 0) {
      return [] as number[];
    }

    const ready = readyFramesRef.current;
    const failed = failedFramesRef.current;
    const inFlight = inFlightFramesRef.current;
    const maxRequests = isPreloadingForPlay ? 8 : 4;
    const targetReady = isPreloadingForPlay
      ? frameHours.length
      : Math.min(frameHours.length, playbackPolicy.bufferTarget);
    const activeInFlight = frameHours.filter((fh) => inFlight.has(fh)).slice(0, maxRequests);
    if (ready.size + inFlight.size >= targetReady) {
      return activeInFlight;
    }

    const requestedPivotHour = isScrubbing && Number.isFinite(scrubRequestedHour)
      ? nearestFrame(frameHours, scrubRequestedHour as number)
      : forecastHour;
    const currentIndex = frameHours.indexOf(requestedPivotHour);
    const pivot = currentIndex >= 0 ? currentIndex : 0;
    const candidates: number[] = [...activeInFlight];
    const seen = new Set<number>(activeInFlight);

    const pushCandidate = (fh: number) => {
      if (seen.has(fh)) return;
      seen.add(fh);
      if (ready.has(fh) || inFlight.has(fh)) return;
      if (failed.has(fh)) {
        if (isScrubbing) {
          return;
        }
        const retryAt = frameNextRetryAtRef.current.get(fh) ?? 0;
        if (Date.now() < retryAt) {
          return;
        }
      }
      candidates.push(fh);
    };

    pushCandidate(frameHours[pivot]);

    for (let i = pivot + 1; i < frameHours.length; i += 1) {
      pushCandidate(frameHours[i]);
      if (candidates.length >= maxRequests) {
        return candidates.slice(0, maxRequests);
      }
    }

    if (isPreloadingForPlay) {
      for (let i = 0; i < frameHours.length; i += 1) {
        pushCandidate(frameHours[i]);
        if (candidates.length >= maxRequests) {
          return candidates.slice(0, maxRequests);
        }
      }
    } else {
      for (let i = pivot - 1; i >= 0; i -= 1) {
        pushCandidate(frameHours[i]);
        if (candidates.length >= maxRequests) {
          return candidates.slice(0, maxRequests);
        }
      }
    }

    return candidates.slice(0, maxRequests);
  }, [
    frameHours,
    forecastHour,
    bufferSnapshot.version,
    playbackPolicy.bufferTarget,
    isPreloadingForPlay,
    isScrubbing,
    scrubRequestedHour,
    isLoopDisplayActive,
    hasRenderableSelection,
  ]);

  const prefetchTileUrls = useMemo(() => {
    return prefetchHours.map((fh) => tileUrlForHour(fh));
  }, [prefetchHours, tileUrlForHour]);

  const effectiveRunId = currentFrame?.run ?? (run !== "latest" ? run : latestRunId);
  const runDateTimeISO = runIdToIso(effectiveRunId);

  // ── Hover-for-data tooltip ──────────────────────────────────────────
  const { tooltip, onHover, onHoverEnd } = useSampleTooltip({
    model,
    run: resolvedRunForRequests,
    varId: variable,
    fh: forecastHour,
  });

  const markTileReady = useCallback((readyUrl: string) => {
    const now = Date.now();
    const ready = readyTileUrlsRef.current;
    ready.set(readyUrl, now);

    // Only pay the eviction cost when the map is actually over budget.
    // The previous code iterated all 160 entries + spread them into an array on
    // every tile event regardless of map size.
    if (ready.size > READY_URL_LIMIT) {
      // First pass: evict TTL-expired entries.
      for (const [url, ts] of ready) {
        if (now - ts > READY_URL_TTL_MS) {
          ready.delete(url);
        }
      }
      // If still over limit, find and remove the single oldest entry per iteration.
      // Excess is typically 1-2 entries, so a linear-scan minimum is cheaper than
      // spreading the whole map into a temporary array and sorting it.
      while (ready.size > READY_URL_LIMIT) {
        let oldestUrl: string | null = null;
        let oldestTs = Number.POSITIVE_INFINITY;
        for (const [url, ts] of ready) {
          if (ts < oldestTs) {
            oldestTs = ts;
            oldestUrl = url;
          }
        }
        if (oldestUrl !== null) {
          ready.delete(oldestUrl);
        } else {
          break;
        }
      }
    }
  }, []);

  const markFrameReady = useCallback((readyUrl: string) => {
    const frameHour = tileUrlToHour.get(readyUrl);
    if (!Number.isFinite(frameHour)) {
      return;
    }
    readyFramesRef.current.add(frameHour as number);
    inFlightFramesRef.current.delete(frameHour as number);
    failedFramesRef.current.delete(frameHour as number);
    frameRetryCountRef.current.delete(frameHour as number);
    frameCycleStartedAtRef.current.delete(frameHour as number);
    frameNextRetryAtRef.current.delete(frameHour as number);

    const startedAt = inFlightStartedAtRef.current.get(frameHour as number);
    if (Number.isFinite(startedAt)) {
      const deltaMs = Date.now() - (startedAt as number);
      if (deltaMs >= 0) {
        readyLatencyStatsRef.current.totalMs += deltaMs;
        readyLatencyStatsRef.current.count += 1;
      }
      inFlightStartedAtRef.current.delete(frameHour as number);
    }
    // Coalesce snapshot updates to at most once per animation frame. Tile events
    // from 8 prefetch sources flood this path during animation — scheduling via
    // RAF prevents each tile from triggering a full React re-render cascade.
    if (bufferSnapshotRafRef.current === null) {
      bufferSnapshotRafRef.current = window.requestAnimationFrame(() => {
        bufferSnapshotRafRef.current = null;
        updateBufferSnapshot();
      });
    }
  }, [tileUrlToHour, updateBufferSnapshot]);

  const isTileReady = useCallback((url: string): boolean => {
    const ts = readyTileUrlsRef.current.get(url);
    if (!ts) return false;
    if (Date.now() - ts > READY_URL_TTL_MS) {
      readyTileUrlsRef.current.delete(url);
      return false;
    }
    return true;
  }, []);

  useEffect(() => {
    latestTileUrlRef.current = tileUrl;
    setSettledTileUrl(isTileReady(tileUrl) ? tileUrl : null);
  }, [tileUrl, isTileReady]);

  const isScrubLoading = useMemo(() => {
    if (isPlaying || isScrubbing) {
      return false;
    }
    return Boolean(mapLoadingTileUrl && mapLoadingTileUrl === tileUrl && settledTileUrl !== tileUrl);
  }, [isPlaying, isScrubbing, mapLoadingTileUrl, tileUrl, settledTileUrl]);

  const findNearestReadyTileScrubHour = useCallback(
    (requestedHour: number): number | null => {
      if (frameHours.length === 0) {
        return null;
      }
      const snappedHour = nearestFrame(frameHours, requestedHour);
      if (isTileReady(tileUrlForHour(snappedHour))) {
        return snappedHour;
      }

      const requestedIndex = frameHours.indexOf(snappedHour);
      if (requestedIndex < 0) {
        return null;
      }

      const movingForward = snappedHour >= forecastHour;
      const checkIndex = (index: number): number | null => {
        if (index < 0 || index >= frameHours.length) {
          return null;
        }
        const candidateHour = frameHours[index];
        if (!isTileReady(tileUrlForHour(candidateHour))) {
          return null;
        }
        return candidateHour;
      };

      for (let step = 1; step <= AUTOPLAY_SKIP_WINDOW; step += 1) {
        const primaryIndex = movingForward ? requestedIndex + step : requestedIndex - step;
        const primaryCandidate = checkIndex(primaryIndex);
        if (Number.isFinite(primaryCandidate)) {
          return primaryCandidate as number;
        }

        const secondaryIndex = movingForward ? requestedIndex - step : requestedIndex + step;
        const secondaryCandidate = checkIndex(secondaryIndex);
        if (Number.isFinite(secondaryCandidate)) {
          return secondaryCandidate as number;
        }
      }

      const currentCandidate = checkIndex(frameHours.indexOf(forecastHour));
      if (Number.isFinite(currentCandidate)) {
        return currentCandidate as number;
      }

      return null;
    },
    [frameHours, forecastHour, isTileReady, tileUrlForHour]
  );

  const findNearestDecodedLoopScrubHour = useCallback(
    (requestedHour: number, mode: RenderModeState): number | null => {
      if (mode === "tiles" || loopFrameHours.length === 0) {
        return null;
      }
      const snappedHour = nearestFrame(loopFrameHours, requestedHour);
      if (hasDecodedLoopFrame(snappedHour, mode)) {
        return snappedHour;
      }

      const pivotIndex = loopFrameHours.indexOf(snappedHour);
      if (pivotIndex < 0) {
        return null;
      }

      const movingForward = snappedHour >= forecastHour;
      for (let step = 1; step < loopFrameHours.length; step += 1) {
        const primaryIndex = movingForward ? pivotIndex + step : pivotIndex - step;
        if (primaryIndex >= 0 && primaryIndex < loopFrameHours.length) {
          const primaryHour = loopFrameHours[primaryIndex];
          if (hasDecodedLoopFrame(primaryHour, mode)) {
            return primaryHour;
          }
        }

        const secondaryIndex = movingForward ? pivotIndex - step : pivotIndex + step;
        if (secondaryIndex >= 0 && secondaryIndex < loopFrameHours.length) {
          const secondaryHour = loopFrameHours[secondaryIndex];
          if (hasDecodedLoopFrame(secondaryHour, mode)) {
            return secondaryHour;
          }
        }
      }

      return null;
    },
    [loopFrameHours, hasDecodedLoopFrame, forecastHour]
  );

  const handleFrameSettled = useCallback((loadedTileUrl: string) => {
    markTileReady(loadedTileUrl);
    markFrameReady(loadedTileUrl);
    if (loadedTileUrl === latestTileUrlRef.current) {
      setSettledTileUrl(loadedTileUrl);
    }
  }, [markTileReady, markFrameReady]);

  const handleTileReady = useCallback((loadedTileUrl: string) => {
    markTileReady(loadedTileUrl);
    markFrameReady(loadedTileUrl);
    if (loadedTileUrl === latestTileUrlRef.current) {
      setSettledTileUrl(loadedTileUrl);
    }
  }, [markTileReady, markFrameReady]);

  const handleFrameLoadingChange = useCallback((loadingTileUrl: string, isLoadingValue: boolean) => {
    if (isLoadingValue) {
      setMapLoadingTileUrl(loadingTileUrl);
      return;
    }
    setMapLoadingTileUrl((current) => (current === loadingTileUrl ? null : current));
  }, []);

  const clearFrameStatusTimer = useCallback(() => {
    if (frameStatusTimerRef.current !== null) {
      window.clearTimeout(frameStatusTimerRef.current);
      frameStatusTimerRef.current = null;
    }
    setFrameStatusMessage(null);
  }, []);

  const showTransientFrameStatus = useCallback((message: string) => {
    setFrameStatusMessage(message);
    if (frameStatusTimerRef.current !== null) {
      window.clearTimeout(frameStatusTimerRef.current);
    }
    frameStatusTimerRef.current = window.setTimeout(() => {
      frameStatusTimerRef.current = null;
      setFrameStatusMessage(null);
    }, FRAME_STATUS_BADGE_MS);
  }, []);

  useEffect(() => {
    requestGenerationRef.current += 1;
  }, [model, run, variable]);

  useEffect(() => {
    datasetGenerationRef.current += 1;
    readyFramesRef.current.clear();
    inFlightFramesRef.current.clear();
    failedFramesRef.current.clear();
    frameRetryCountRef.current.clear();
    frameCycleStartedAtRef.current.clear();
    frameNextRetryAtRef.current.clear();
    inFlightStartedAtRef.current.clear();
    readyLatencyStatsRef.current = { totalMs: 0, count: 0 };
    autoplayPrimedRef.current = false;
    // Cancel any pending coalesced snapshot RAF and reset the equality baseline so
    // the first update after reset is never incorrectly skipped.
    if (bufferSnapshotRafRef.current !== null) {
      window.cancelAnimationFrame(bufferSnapshotRafRef.current);
      bufferSnapshotRafRef.current = null;
    }
    lastSnapshotStatsRef.current = { bufferedCount: -1, failedCount: -1, inFlightCount: -1, queueDepth: -1 };
    setIsLoopPreloading(false);
    setIsLoopAutoplayBuffering(false);
    setLoopProgress({ total: loopFrameHours.length, ready: 0, failed: 0 });
    setLoopBaseForecastHour(null);
    setLoopDisplayHour(null);
    loopPreloadTokenRef.current += 1;
    loopReadyHoursRef.current.clear();
    loopFailedHoursRef.current.clear();
    for (const cached of loopDecodedCacheRef.current.values()) {
      cached.bitmap.close();
    }
    loopDecodedCacheRef.current.clear();
    loopDecodedCacheBytesRef.current = 0;
    setIsPreloadingForPlay(false);
    lastTileViewportCommitUrlRef.current = null;
    preloadProgressRef.current = {
      lastBufferedCount: 0,
      lastProgressAt: Date.now(),
    };
    setScrubRequestedHour(null);
    const version = ++bufferVersionRef.current;
    setBufferSnapshot({
      totalFrames: frameHours.length,
      bufferedCount: 0,
      bufferedAheadCount: 0,
      terminalCount: 0,
      terminalAheadCount: 0,
      failedCount: 0,
      inFlightCount: 0,
      queueDepth: frameHours.length,
      statusText: `Buffered 0/${frameHours.length}`,
      version,
    });
  }, [
    // Only the three selector values that uniquely identify a dataset change.
    // frameHours.length and loopFrameHours.length are derived state — including
    // them caused a second reset firing when frames were cleared then re-populated,
    // which wiped newly-decoded bitmaps and reset the whole buffer mid-load.
    model,
    resolvedRunForRequests,
    variable,
  ]);

  useEffect(() => {
    if (!isLoopPreloading) {
      return;
    }
    if (!canUseLoopPlayback || loopFrameHours.length === 0) {
      setIsLoopPreloading(false);
      setRenderMode("tiles");
      return;
    }

    const token = ++loopPreloadTokenRef.current;
    const readySet = new Set<number>();
    const failedSet = new Set<number>();
    loopReadyHoursRef.current = readySet;
    loopFailedHoursRef.current = failedSet;
    setLoopProgress({ total: loopFrameHours.length, ready: 0, failed: 0 });

    // RAF-coalesced progress updates: with PRELOAD_CONCURRENCY=4, multiple decodes
    // can complete within the same 16ms frame. Batching them into a single setState
    // call eliminates N intermediate re-renders while frames are loading.
    let progressRafId: number | null = null;
    const flushProgress = () => {
      if (token !== loopPreloadTokenRef.current) return;
      setLoopProgress({ total: loopFrameHours.length, ready: readySet.size, failed: failedSet.size });
    };
    const scheduleProgress = () => {
      if (progressRafId !== null) return;
      progressRafId = window.requestAnimationFrame(() => {
        progressRafId = null;
        flushProgress();
      });
    };

    const mark = (fh: number, ok: boolean) => {
      if (token !== loopPreloadTokenRef.current) {
        return;
      }
      if (ok) {
        readySet.add(fh);
      } else {
        failedSet.add(fh);
      }

      if (readySet.size + failedSet.size < loopFrameHours.length) {
        // Not done yet — schedule a batched progress update.
        scheduleProgress();
        return;
      }

      // All frames accounted for — flush progress synchronously then transition.
      if (progressRafId !== null) {
        window.cancelAnimationFrame(progressRafId);
        progressRafId = null;
      }
      flushProgress();
      setIsLoopPreloading(false);
      const minReady = Math.min(LOOP_PRELOAD_MIN_READY, loopFrameHours.length);
      if (readySet.size >= minReady) {
        if (renderMode !== "tiles") {
          setVisibleRenderMode(renderMode);
        }
        setLoopDisplayHour(forecastHour);
        setIsPlaying(true);
        return;
      }
      setRenderMode("tiles");
      setIsPlaying(false);
      showTransientFrameStatus("Loop preload failed");
    };

    // Process frames in playback order with bounded concurrency to stay within
    // the browser's HTTP/2 stream budget (~6 per host) and keep early frames
    // ready before later ones.
    const PRELOAD_CONCURRENCY = 4;
    let inFlight = 0;
    let nextIndex = 0;

    const processNext = () => {
      while (inFlight < PRELOAD_CONCURRENCY && nextIndex < loopFrameHours.length) {
        const fh = loopFrameHours[nextIndex];
        nextIndex += 1;
        if (!resolveLoopUrlForHour(fh, renderMode)) {
          mark(fh, false);
          continue;
        }
        inFlight += 1;
        // No signal: decode always runs to completion for LRU cache warming;
        // the token inside mark() gates whether the result is actually applied.
        ensureLoopFrameDecoded(fh, renderMode)
          .then((ready) => mark(fh, ready))
          .catch(() => mark(fh, false))
          .finally(() => {
            inFlight -= 1;
            processNext();
          });
      }
    };
    processNext();

    return () => {
      loopPreloadTokenRef.current += 1;
      if (progressRafId !== null) {
        window.cancelAnimationFrame(progressRafId);
        progressRafId = null;
      }
    };
  }, [
    isLoopPreloading,
    canUseLoopPlayback,
    loopFrameHours,
    resolveLoopUrlForHour,
    showTransientFrameStatus,
    renderMode,
    forecastHour,
    ensureLoopFrameDecoded,
  ]);

  useEffect(() => {
    if (!isLoopDisplayActive || loopFrameHours.length === 0) {
      return;
    }

    let cancelled = false;
    const inFlight = new Set<number>();
    const controllers = new Map<number, AbortController>();

    const launchDecode = (fh: number) => {
      if (cancelled || inFlight.has(fh)) {
        return;
      }
      const controller = new AbortController();
      inFlight.add(fh);
      controllers.set(fh, controller);
      ensureLoopFrameDecoded(fh, visibleRenderMode, controller.signal)
        .catch(() => {
          // best-effort prefetch; decode failures are handled by fallback path.
        })
        .finally(() => {
          inFlight.delete(fh);
          controllers.delete(fh);
        });
    };

    const schedulePrefetch = () => {
      if (cancelled) {
        return;
      }
      const currentIndex = loopFrameHours.indexOf(forecastHour);
      if (currentIndex < 0) {
        return;
      }

      const remainingAhead = Math.max(0, loopFrameHours.length - 1 - currentIndex);
      const targetAhead = Math.min(LOOP_AHEAD_READY_TARGET, remainingAhead);
      if (targetAhead <= 0) {
        return;
      }

      const candidates: number[] = [];
      for (let index = currentIndex + 1; index < loopFrameHours.length && candidates.length < targetAhead * 2; index += 1) {
        const fh = loopFrameHours[index];
        if (hasDecodedLoopFrame(fh, visibleRenderMode)) {
          continue;
        }
        if (inFlight.has(fh)) {
          continue;
        }
        candidates.push(fh);
      }

      const availableSlots = Math.max(0, MAX_CONCURRENT_DECODES - inFlight.size);
      for (const fh of candidates.slice(0, availableSlots)) {
        launchDecode(fh);
      }
    };

    schedulePrefetch();
    const interval = window.setInterval(schedulePrefetch, 350);

    return () => {
      cancelled = true;
      window.clearInterval(interval);
      for (const controller of controllers.values()) {
        controller.abort();
      }
      controllers.clear();
      inFlight.clear();
    };
  }, [
    isLoopDisplayActive,
    visibleRenderMode,
    loopFrameHours,
    forecastHour,
    ensureLoopFrameDecoded,
    hasDecodedLoopFrame,
  ]);

  useEffect(() => {
    if (!isPlaying || renderMode === "tiles" || loopFrameHours.length === 0) {
      return;
    }

    const interval = window.setInterval(() => {
      const currentIndex = loopFrameHours.indexOf(forecastHour);
      if (currentIndex < 0) {
        return;
      }

      const remainingAheadFrames = Math.max(0, loopFrameHours.length - currentIndex - 1);
      const minAheadRequired = Math.min(LOOP_MIN_PLAYABLE_AHEAD, remainingAheadFrames);
      const aheadReady = countAheadReadyLoopFrames(forecastHour, visibleRenderMode, LOOP_AHEAD_READY_TARGET);
      if (aheadReady < minAheadRequired) {
        setIsPlaying(false);
        setIsLoopAutoplayBuffering(true);
        showTransientFrameStatus("Buffering");
        return;
      }

      const nextIndex = currentIndex + 1;
      if (nextIndex >= loopFrameHours.length) {
        setIsPlaying(false);
        setIsLoopAutoplayBuffering(false);
        return;
      }

      for (let idx = nextIndex; idx < loopFrameHours.length; idx += 1) {
        const candidate = loopFrameHours[idx];
        if (hasDecodedLoopFrame(candidate, visibleRenderMode)) {
          setTargetForecastHour(candidate);
          return;
        }
      }

      setIsPlaying(false);
      setIsLoopAutoplayBuffering(true);
      showTransientFrameStatus("Buffering");
    }, AUTOPLAY_TICK_MS);

    return () => window.clearInterval(interval);
  }, [
    isPlaying,
    renderMode,
    loopFrameHours,
    forecastHour,
    visibleRenderMode,
    hasDecodedLoopFrame,
    countAheadReadyLoopFrames,
    showTransientFrameStatus,
  ]);

  useEffect(() => {
    if (!isLoopAutoplayBuffering || isPlaying || renderMode === "tiles" || loopFrameHours.length === 0) {
      return;
    }
    const currentIndex = loopFrameHours.indexOf(forecastHour);
    if (currentIndex < 0) {
      return;
    }

    const remainingAheadFrames = Math.max(0, loopFrameHours.length - currentIndex - 1);
    const minAheadRequired = Math.min(LOOP_MIN_PLAYABLE_AHEAD, remainingAheadFrames);
    const aheadReady = countAheadReadyLoopFrames(forecastHour, visibleRenderMode, LOOP_AHEAD_READY_TARGET);
    if (aheadReady < minAheadRequired) {
      return;
    }

    setIsLoopAutoplayBuffering(false);
    setIsPlaying(true);
  }, [
    isLoopAutoplayBuffering,
    isPlaying,
    renderMode,
    loopFrameHours,
    forecastHour,
    visibleRenderMode,
    countAheadReadyLoopFrames,
  ]);

  useEffect(() => {
    updateBufferSnapshot();
  }, [updateBufferSnapshot]);

  useEffect(() => {
    const interval = window.setInterval(() => {
      updateBufferSnapshot();
    }, 1000);
    return () => {
      window.clearInterval(interval);
    };
  }, [updateBufferSnapshot]);

  useEffect(() => {
    const inFlight = inFlightFramesRef.current;
    const ready = readyFramesRef.current;
    const failed = failedFramesRef.current;
    const requested = new Set(prefetchHours);
    let changed = false;

    for (const fh of inFlight) {
      if (!requested.has(fh) || ready.has(fh)) {
        inFlight.delete(fh);
        inFlightStartedAtRef.current.delete(fh);
        changed = true;
      }
    }

    for (const fh of prefetchHours) {
      if (!ready.has(fh) && !inFlight.has(fh)) {
        if (failed.has(fh)) {
          failed.delete(fh);
        }
        inFlight.add(fh);
        if (!frameCycleStartedAtRef.current.has(fh)) {
          frameCycleStartedAtRef.current.set(fh, Date.now());
        }
        inFlightStartedAtRef.current.set(fh, Date.now());
        changed = true;
      }
    }
    if (changed) {
      updateBufferSnapshot();
    }
  }, [prefetchHours, updateBufferSnapshot]);

  const requestForecastHour = useCallback(
    (requestedHour: number) => {
      if (!isScrubbing) {
        setScrubRequestedHour(null);
        setTargetForecastHour(requestedHour);
        return;
      }

      setScrubRequestedHour(requestedHour);
      pendingScrubHourRef.current = requestedHour;
      if (scrubRafRef.current !== null) {
        return;
      }

      scrubRafRef.current = window.requestAnimationFrame(() => {
        scrubRafRef.current = null;
        const latestRequestedHour = pendingScrubHourRef.current;
        if (!Number.isFinite(latestRequestedHour)) {
          return;
        }
        const requested = latestRequestedHour as number;
        const useExactScrubSelection = model === "gfs";
        if (!isLoopDisplayActive) {
          if (frameHours.length === 0) {
            return;
          }
          const snappedTileHour = nearestFrame(frameHours, requested);
          if (useExactScrubSelection) {
            setTargetForecastHour(snappedTileHour);
            return;
          }
          const readyTileHour = findNearestReadyTileScrubHour(snappedTileHour);
          if (Number.isFinite(readyTileHour)) {
            setTargetForecastHour(readyTileHour as number);
          }
          return;
        }

        const nextHour = loopFrameHours.length > 0
          ? nearestFrame(loopFrameHours, requested)
          : requested;
        if (useExactScrubSelection) {
          setTargetForecastHour(nextHour);
          setLoopDisplayHour(nextHour);
        } else {
          const readyLoopHour = findNearestDecodedLoopScrubHour(nextHour, visibleRenderMode);
          if (Number.isFinite(readyLoopHour)) {
            const resolvedReadyHour = readyLoopHour as number;
            setTargetForecastHour(resolvedReadyHour);
            setLoopDisplayHour(resolvedReadyHour);
          }
        }

        loopDisplayDecodeTokenRef.current += 1;
        const decodeToken = loopDisplayDecodeTokenRef.current;
        // No signal: decode completes and caches regardless of subsequent scrub
        // positions. The token check below ensures only the last scrub target
        // actually updates the display — earlier frames stay warm in the LRU cache.
        ensureLoopFrameDecoded(nextHour, visibleRenderMode)
          .then((ready) => {
            if (!ready) {
              return;
            }
            if (decodeToken !== loopDisplayDecodeTokenRef.current) {
              return;
            }
            setLoopDisplayHour(nextHour);
            setTargetForecastHour(nextHour);
          })
          .catch(() => {
            // best-effort decode path for scrub; keep previous visible frame on failure.
          });
      });
    },
    [
      isScrubbing,
      isLoopDisplayActive,
      loopFrameHours,
      frameHours,
      model,
      ensureLoopFrameDecoded,
      visibleRenderMode,
      findNearestReadyTileScrubHour,
      findNearestDecodedLoopScrubHour,
    ]
  );

  useEffect(() => {
    if (!isLoopDisplayActive) {
      setLoopDisplayHour(null);
      return;
    }

    loopDisplayDecodeTokenRef.current += 1;
    const decodeToken = loopDisplayDecodeTokenRef.current;

    // No signal: the decode always completes and its result is stored in the LRU
    // cache. The token guards the commit; scrubbing to a new frame only invalidates
    // the commit, not the inflight fetch — keeping every touched frame warm.
    ensureLoopFrameDecoded(resolvedLoopForecastHour, visibleRenderMode)
      .then((ready) => {
        if (!ready) {
          return;
        }
        if (decodeToken !== loopDisplayDecodeTokenRef.current) {
          return;
        }
        setLoopDisplayHour(resolvedLoopForecastHour);
      })
      .catch(() => {
        // keep previous display hour when decode fails.
      });
  }, [isLoopDisplayActive, resolvedLoopForecastHour, visibleRenderMode, ensureLoopFrameDecoded]);

  useEffect(() => {
    const controller = new AbortController();
    const generation = requestGenerationRef.current;

    async function bootstrap() {
      setLoading(true);
      setError(null);
      try {
        const [capabilitiesData, regionPresetData] = await Promise.all([
          fetchCapabilities({ signal: controller.signal }),
          fetchRegionPresets({ signal: controller.signal }),
        ]);
        if (controller.signal.aborted || generation !== requestGenerationRef.current) {
          return;
        }

        setCapabilities(capabilitiesData);

        const supportedModelIds = capabilitiesData.supported_models.filter(
          (modelId) => Boolean(capabilitiesData.model_catalog?.[modelId])
        );
        const readyModelIds = supportedModelIds.filter((modelId) => {
          const availability = capabilitiesData.availability?.[modelId];
          return availability?.latest_run_ready === true;
        });
        const visibleModelIds = readyModelIds.length > 0 ? readyModelIds : supportedModelIds;
        const modelRows = normalizeModelRows(capabilitiesData, visibleModelIds);
        const orderedVisibleModelIds = modelRows.map((entry) => entry.id);
        const preferredDefaultModel = orderedVisibleModelIds.includes("hrrr") ? "hrrr" : "";
        const availableModelId = orderedVisibleModelIds.find((modelId) => {
          const availability = capabilitiesData.availability?.[modelId];
          return Boolean(availability?.latest_run);
        });
        const nextModel = preferredDefaultModel || availableModelId || orderedVisibleModelIds[0] || "";
        const modelOptions = modelRows.map((entry) => ({
          value: entry.id,
          label: entry.displayName || entry.id,
        }));
        setModels(modelOptions);
        setModel(nextModel);

        const modelCapability = nextModel ? capabilitiesData.model_catalog[nextModel] : null;
        const capabilityVars = normalizeCapabilityVarRows(modelCapability);
        const variableOptions = makeVariableOptions(capabilityVars);
        const variableIds = variableOptions.map((opt) => opt.value);
        const defaultVarKey = String(modelCapability?.defaults?.default_var_key ?? "").trim();
        const nextVariable = variableIds.includes(defaultVarKey)
          ? defaultVarKey
          : (variableIds[0] ?? "");
        setVariables(variableOptions);
        setVariable(nextVariable);

        setRegionPresets(regionPresetData);
        const regionIds = Object.keys(regionPresetData);
        const regionOptions = regionIds.map((id) => ({
          value: id,
          label: makeRegionLabel(id, regionPresetData[id]),
        }));
        setRegions(regionOptions);
        const canonicalRegion = String(
          modelCapability?.constraints?.canonical_region
          ?? modelCapability?.canonical_region
          ?? MAP_VIEW_DEFAULTS.region
        ).trim();
        const nextRegion = pickPreferred(regionIds, canonicalRegion || MAP_VIEW_DEFAULTS.region);
        setRegion(nextRegion);

        setRun("latest");
        setRuns([]);
        setRunManifest(null);
        setFrameRows([]);
        setLoopManifest(null);
      } catch (err) {
        if (controller.signal.aborted || generation !== requestGenerationRef.current) {
          return;
        }
        setError(err instanceof Error ? err.message : "Failed to load capabilities");
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      }
    }

    bootstrap();
    return () => {
      controller.abort();
    };
  }, []);

  useEffect(() => {
    if (!model) return;
    const controller = new AbortController();
    const generation = requestGenerationRef.current;

    async function loadRunsAndVars() {
      setError(null);
      try {
        const shouldFetchRuns = runsLoadedForModelRef.current !== model;
        const runDataPromise = shouldFetchRuns
          ? fetchRuns(model, { signal: controller.signal })
          : Promise.resolve(runs);
        const [runData, requestedManifest] = await Promise.all([
          runDataPromise,
          fetchManifest(model, run, { signal: controller.signal }).catch(() => null),
        ]);
        if (controller.signal.aborted || generation !== requestGenerationRef.current) {
          return;
        }

        const nextRun = run !== "latest" && runData.includes(run) ? run : "latest";
        let manifestData = requestedManifest;
        if (!manifestData && nextRun !== run) {
          manifestData = await fetchManifest(model, nextRun, { signal: controller.signal }).catch(() => null);
          if (controller.signal.aborted || generation !== requestGenerationRef.current) {
            return;
          }
        }

        if (shouldFetchRuns) {
          runsLoadedForModelRef.current = model;
          setRuns(runData);
        }
        setRun(nextRun);

        setRunManifest(manifestData);
        const baseCapabilityVars = selectedCapabilityVars;
        const resolvedVars = manifestData
          ? capabilityVarsForManifest(manifestData.variables, baseCapabilityVars)
          : baseCapabilityVars;
        const variableOptions = makeVariableOptions(resolvedVars);
        const variableIds = variableOptions.map((opt) => opt.value);
        const defaultVarKey = String(selectedModelCapability?.defaults?.default_var_key ?? "").trim();
        const nextVar = variableIds.includes(defaultVarKey)
          ? defaultVarKey
          : (variableIds[0] ?? "");
        setVariables(variableOptions);
        setVariable((prev) => (variableIds.includes(prev) ? prev : nextVar));
      } catch (err) {
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;
        setRunManifest(null);
        setError(err instanceof Error ? err.message : "Failed to load run manifest");
      }
    }

    loadRunsAndVars();
    return () => {
      controller.abort();
    };
  }, [model, run, runs, selectedCapabilityVars, selectedModelCapability]);

  useEffect(() => {
    setFrameRows([]);
    setLoopManifest(null);
    setForecastHour(Number.POSITIVE_INFINITY);
    setTargetForecastHour(Number.POSITIVE_INFINITY);
    setLoopDisplayHour(null);
  }, [model, run, variable]);

  useEffect(() => {
    if (!model || !variable || !hasRenderableSelection) {
      setLoopManifest(null);
      return;
    }
    const controller = new AbortController();
    const generation = requestGenerationRef.current;

    async function loadLoopManifest() {
      const manifest = await fetchLoopManifest(model, resolvedRunForRequests, variable, { signal: controller.signal });
      if (controller.signal.aborted || generation !== requestGenerationRef.current) {
        return;
      }
      setLoopManifest(manifest);
    }

    loadLoopManifest().catch(() => {
      if (controller.signal.aborted || generation !== requestGenerationRef.current) {
        return;
      }
      setLoopManifest(null);
    });

    return () => {
      controller.abort();
    };
  }, [model, variable, resolvedRunForRequests, hasRenderableSelection]);

  useEffect(() => {
    if (!model || !variable || !hasRenderableSelection) return;
    const controller = new AbortController();
    const generation = requestGenerationRef.current;

    async function loadFrames() {
      setError(null);
      let hydratedFromManifest = false;
      const manifestMatchesSelection =
        Boolean(runManifest) &&
        runManifest?.model === model &&
        (run === "latest" || runManifest?.run === run || runManifest?.run === resolvedRunForRequests);
      if (manifestMatchesSelection) {
        const { rows, hasFrameList } = resolveManifestFrames(runManifest, variable);
        if (hasFrameList) {
          setFrameRows((prevRows) => mergeManifestRowsWithPrevious(rows, prevRows));
          const frames = rows.map((row) => Number(row.fh)).filter(Number.isFinite);
          setForecastHour((prev) => resolveForecastHour(frames, prev, selectedVariableDefaultFh));
          setTargetForecastHour((prev) => resolveForecastHour(frames, prev, selectedVariableDefaultFh));
          hydratedFromManifest = true;
        }
      }

      try {
        const framesRunKey = run === "latest" ? "latest" : resolvedRunForRequests;
        const rows = await fetchFrames(model, framesRunKey, variable, { signal: controller.signal });
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;
        setFrameRows(rows);
        const frames = rows.map((row) => Number(row.fh)).filter(Number.isFinite);
        setForecastHour((prev) => resolveForecastHour(frames, prev, selectedVariableDefaultFh));
        setTargetForecastHour((prev) => resolveForecastHour(frames, prev, selectedVariableDefaultFh));
      } catch (err) {
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;
        if (!hydratedFromManifest) {
          setError(err instanceof Error ? err.message : "Failed to load frames");
          setFrameRows([]);
        }
      }
    }

    loadFrames();
    return () => {
      controller.abort();
    };
  }, [model, run, variable, resolvedRunForRequests, runManifest, selectedVariableDefaultFh, hasRenderableSelection]);

  useEffect(() => {
    const handleVisibilityChange = () => {
      setIsPageVisible(!document.hidden);
    };

    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => {
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, []);

  useEffect(() => {
    if (!model || !variable || !hasRenderableSelection || run !== "latest" || !isPageVisible) {
      return;
    }

    let cancelled = false;
    let tickController: AbortController | null = null;

    const interval = window.setInterval(() => {
      tickController?.abort();
      tickController = new AbortController();
      const manifestMatchesSelection =
        Boolean(runManifest) &&
        runManifest?.model === model &&
        (run === "latest" || runManifest?.run === run || runManifest?.run === resolvedRunForRequests);

      if (manifestMatchesSelection) {
        fetchManifest(model, run, { signal: tickController.signal })
          .then((manifestData) => {
            if (cancelled || tickController?.signal.aborted) {
              return;
            }
            setRunManifest(manifestData);
            const capabilityVars = capabilityVarsForManifest(manifestData.variables, selectedCapabilityVars);
            if (capabilityVars.length > 0) {
              const variableOptions = makeVariableOptions(capabilityVars);
              const variableIds = variableOptions.map((opt) => opt.value);
              const defaultVarKey = String(selectedModelCapability?.defaults?.default_var_key ?? "").trim();
              const nextVar = variableIds.includes(defaultVarKey)
                ? defaultVarKey
                : (variableIds[0] ?? "");
              setVariables(variableOptions);
              setVariable((prev) => (variableIds.includes(prev) ? prev : nextVar));
            }
            const { rows, hasFrameList } = resolveManifestFrames(manifestData, variable);
            if (hasFrameList) {
              setFrameRows((prevRows) => mergeManifestRowsWithPrevious(rows, prevRows));
              const frames = rows.map((row) => Number(row.fh)).filter(Number.isFinite);
              setForecastHour((prev) => resolveForecastHour(frames, prev, selectedVariableDefaultFh));
              setTargetForecastHour((prev) => resolveForecastHour(frames, prev, selectedVariableDefaultFh));
            }
          })
          .catch((err) => {
            if (err instanceof DOMException && err.name === "AbortError") {
              return;
            }
            // Background refresh should not interrupt active UI.
          });
        return;
      }

      // Use `run` ("latest" when in live mode) rather than the resolved run ID so
      // the request hits the short-TTL ETag path and bypasses any stale immutable
      // browser-cache entries for the resolved run URL.
      fetchFrames(model, run, variable, { signal: tickController.signal })
        .then((rows) => {
          if (cancelled || tickController?.signal.aborted) {
            return;
          }
          setFrameRows(rows);
          const frames = rows.map((row) => Number(row.fh)).filter(Number.isFinite);
          setForecastHour((prev) => resolveForecastHour(frames, prev, selectedVariableDefaultFh));
          setTargetForecastHour((prev) => resolveForecastHour(frames, prev, selectedVariableDefaultFh));
        })
        .catch((err) => {
          if (err instanceof DOMException && err.name === "AbortError") {
            return;
          }
          // Background refresh should not interrupt active UI.
        });
    }, 30000);

    return () => {
      cancelled = true;
      tickController?.abort();
      window.clearInterval(interval);
    };
  }, [model, run, variable, resolvedRunForRequests, runManifest, isPageVisible, selectedCapabilityVars, selectedModelCapability, selectedVariableDefaultFh, hasRenderableSelection]);

  useEffect(() => {
    if (!isPlaying || renderMode !== "tiles" || frameHours.length === 0) return;

    const interval = window.setInterval(() => {
      const currentIndex = frameHours.indexOf(forecastHour);
      if (currentIndex < 0) return;

      const remainingAheadFrames = Math.max(0, frameHours.length - currentIndex - 1);
      const minAheadRequired = Math.min(playbackPolicy.minAheadWhilePlaying, remainingAheadFrames);
      if (bufferSnapshot.bufferedAheadCount < minAheadRequired) {
        setIsPlaying(false);
        showTransientFrameStatus("Buffering frames");
        autoplayPrimedRef.current = false;
        return;
      }

      const nextIndex = currentIndex + 1;
      if (nextIndex >= frameHours.length) {
        setIsPlaying(false);
        return;
      }

      if (!autoplayPrimedRef.current) {
        let primed = true;
        const readyAheadEnd = Math.min(frameHours.length - 1, currentIndex + AUTOPLAY_READY_AHEAD);
        for (let idx = currentIndex + 1; idx <= readyAheadEnd; idx += 1) {
          const aheadHour = frameHours[idx];
          if (!isTileReady(tileUrlForHour(aheadHour))) {
            primed = false;
            break;
          }
        }
        if (!primed) {
          return;
        }
        autoplayPrimedRef.current = true;
      }

      let chosenHour: number | null = null;
      let chosenStep = 0;
      const maxStep = Math.min(AUTOPLAY_SKIP_WINDOW, frameHours.length - 1 - currentIndex);
      for (let step = 1; step <= maxStep; step += 1) {
        const candidateHour = frameHours[currentIndex + step];
        const candidateUrl = tileUrlForHour(candidateHour);
        if (isTileReady(candidateUrl)) {
          chosenHour = candidateHour;
          chosenStep = step;
          break;
        }
      }

      if (chosenHour !== null) {
        if (chosenStep > 1) {
          const skippedHour = frameHours[nextIndex];
          showTransientFrameStatus(`Frame unavailable (FH ${skippedHour})`);
        }
        setTargetForecastHour(chosenHour);
        return;
      }

      autoplayPrimedRef.current = false;
    }, AUTOPLAY_TICK_MS);

    return () => window.clearInterval(interval);
  }, [
    isPlaying,
    frameHours,
    forecastHour,
    isTileReady,
    tileUrlForHour,
    showTransientFrameStatus,
    bufferSnapshot.bufferedAheadCount,
    playbackPolicy.minAheadWhilePlaying,
    renderMode,
  ]);

  useEffect(() => {
    if (!isPreloadingForPlay) {
      return;
    }
    if (frameHours.length === 0) {
      setIsPreloadingForPlay(false);
      return;
    }

    const bufferedCount = Math.max(0, Math.min(bufferSnapshot.bufferedCount, frameHours.length));
    const progress = preloadProgressRef.current;
    const now = Date.now();

    if (progress.lastProgressAt <= 0) {
      progress.lastProgressAt = now;
    }
    if (bufferedCount > progress.lastBufferedCount) {
      progress.lastBufferedCount = bufferedCount;
      progress.lastProgressAt = now;
    }

    const remainingAheadFrames = Math.max(0, frameHours.length - forecastHour - 1);
    const minAheadReady = Math.min(playbackPolicy.minAheadWhilePlaying, remainingAheadFrames);
    const canStartByAheadReady = bufferSnapshot.bufferedAheadCount >= minAheadReady;
    const preloadStartThreshold = Math.min(
      frameHours.length,
      Math.max(playbackPolicy.minStartBuffer, Math.ceil(frameHours.length * PRELOAD_START_RATIO))
    );
    const stalledMs = now - progress.lastProgressAt;
    const canStartByThreshold = bufferedCount >= preloadStartThreshold && canStartByAheadReady;
    const canStartByStall =
      bufferedCount >= playbackPolicy.minStartBuffer &&
      canStartByAheadReady &&
      stalledMs >= PRELOAD_STALL_MS;

    if (!canStartByThreshold && !canStartByStall) {
      return;
    }

    setIsPreloadingForPlay(false);
    autoplayPrimedRef.current = false;
    if (canStartByStall && !canStartByThreshold) {
      showTransientFrameStatus("Starting with partial buffer");
    }
    setIsPlaying(true);
  }, [
    isPreloadingForPlay,
    bufferSnapshot.bufferedCount,
    bufferSnapshot.bufferedAheadCount,
    frameHours.length,
    forecastHour,
    playbackPolicy.minAheadWhilePlaying,
    playbackPolicy.minStartBuffer,
    showTransientFrameStatus,
  ]);

  useEffect(() => {
    if (frameHours.length === 0 && isPlaying) {
      setIsPlaying(false);
    }
  }, [frameHours, isPlaying]);

  useEffect(() => {
    if (!isPlaying) {
      autoplayPrimedRef.current = false;
      clearFrameStatusTimer();
    }
  }, [isPlaying, clearFrameStatusTimer]);

  const handleSetIsPlaying = useCallback((value: boolean) => {
    if (!value) {
      setIsPlaying(false);
      setIsLoopAutoplayBuffering(false);
      setIsLoopPreloading(false);
      setIsPreloadingForPlay(false);
      return;
    }
    if (loading || frameHours.length === 0) {
      return;
    }

    if (renderMode === "tiles") {
      if (canUseLoopPlayback && isHighDetailZoom) {
        setIsPlaying(false);
        setIsLoopAutoplayBuffering(false);
        setIsLoopPreloading(false);
        setIsPreloadingForPlay(false);
        showTransientFrameStatus("High detail mode — zoom out for smooth loop");
        return;
      }
      if (!canUseLoopPlayback) {
        setIsPlaying(false);
        setIsLoopAutoplayBuffering(false);
        setIsLoopPreloading(false);
        setIsPreloadingForPlay(false);
        showTransientFrameStatus("Loop unavailable for this variable/run — showing tiles");
        return;
      }
    }

    if (canUseLoopPlayback && webpDefaultEnabled) {
      setLoopBaseForecastHour(forecastHour);
      setIsPlaying(false);
      setIsPreloadingForPlay(false);
      setIsLoopPreloading(true);
      showTransientFrameStatus("Loading loop frames");
      return;
    }

    setRenderMode("tiles");
    setIsLoopAutoplayBuffering(false);
    const remainingAheadFrames = Math.max(0, frameHours.length - forecastHour - 1);
    const minAheadReady = Math.min(playbackPolicy.minAheadWhilePlaying, remainingAheadFrames);
    const canStartImmediately =
      bufferSnapshot.bufferedCount >= playbackPolicy.minStartBuffer &&
      bufferSnapshot.bufferedAheadCount >= minAheadReady;
    if (canStartImmediately) {
      setIsPreloadingForPlay(false);
      setIsPlaying(true);
      return;
    }
    setIsPlaying(false);
    preloadProgressRef.current = {
      lastBufferedCount: Math.max(0, Math.min(bufferSnapshot.bufferedCount, frameHours.length)),
      lastProgressAt: Date.now(),
    };
    setIsPreloadingForPlay(true);
    showTransientFrameStatus("Loading frames");
  }, [
    loading,
    frameHours.length,
    forecastHour,
    bufferSnapshot.bufferedCount,
    bufferSnapshot.bufferedAheadCount,
    playbackPolicy.minAheadWhilePlaying,
    playbackPolicy.minStartBuffer,
    canUseLoopPlayback,
    isHighDetailZoom,
    webpDefaultEnabled,
    renderMode,
    showTransientFrameStatus,
  ]);

  const handleZoomRoutingSignal = useCallback((payload: { zoom: number; gestureActive: boolean }) => {
    setMapZoom(payload.zoom);
    setZoomGestureActive(payload.gestureActive);
  }, []);

  const handleTileViewportReady = useCallback((readyTileUrl: string) => {
    if (renderMode !== "tiles") {
      return;
    }
    if (readyTileUrl !== tileUrl) {
      return;
    }
    if (visibleRenderMode === "tiles" && lastTileViewportCommitUrlRef.current === readyTileUrl) {
      return;
    }
    lastTileViewportCommitUrlRef.current = readyTileUrl;
    setVisibleRenderMode("tiles");
  }, [renderMode, tileUrl, visibleRenderMode]);

  useEffect(() => {
    if (isPlaying && isScrubbing) {
      setIsScrubbing(false);
    }
  }, [isPlaying, isScrubbing]);

  // When the user starts scrubbing, cancel any pending buffering-recovery auto-restart
  // so it cannot preempt the in-progress scrub and re-lock the slider.
  useEffect(() => {
    if (isScrubbing) {
      setIsLoopAutoplayBuffering(false);
      setIsLoopPreloading(false);
      return;
    }
    setScrubRequestedHour(null);
  }, [isScrubbing]);

  useEffect(() => {
    return () => {
      clearFrameStatusTimer();
      if (scrubRafRef.current !== null) {
        window.cancelAnimationFrame(scrubRafRef.current);
      }
      if (bufferSnapshotRafRef.current !== null) {
        window.cancelAnimationFrame(bufferSnapshotRafRef.current);
      }
      loopDisplayDecodeAbortRef.current?.abort();
      for (const cached of loopDecodedCacheRef.current.values()) {
        cached.bitmap.close();
      }
      loopDecodedCacheRef.current.clear();
      loopDecodedCacheBytesRef.current = 0;
    };
  }, [clearFrameStatusTimer]);

  useEffect(() => {
    if (selectableFrameHours.length === 0) {
      return;
    }

    const nextTarget = nearestFrame(selectableFrameHours, targetForecastHour);
    if (nextTarget === forecastHour) {
      return;
    }
    setForecastHour(nextTarget);
  }, [targetForecastHour, forecastHour, selectableFrameHours]);

  const controlsIsPlaying = isPlaying || isPreloadingForPlay || isLoopPreloading;
  const preloadBufferedCount = isLoopPreloading
    ? Math.max(0, Math.min(loopProgress.ready + loopProgress.failed, loopProgress.total))
    : Math.max(0, Math.min(bufferSnapshot.terminalCount, bufferSnapshot.totalFrames));
  const preloadTotal = isLoopPreloading ? loopProgress.total : bufferSnapshot.totalFrames;
  const preloadPercent = preloadTotal > 0
    ? Math.round((preloadBufferedCount / preloadTotal) * 100)
    : 0;
  const showBufferStatus =
    isScrubLoading
    || (isPreloadingForPlay && bufferSnapshot.totalFrames > 0)
    || (isLoopPreloading && loopProgress.total > 0);
  const bufferStatusText = isScrubLoading
    ? "Loading frame"
    : `Loading frames ${preloadBufferedCount}/${preloadTotal}`;
  const activeLoopHour = loopDisplayHour ?? forecastHour;
  const activeLoopUrl = isLoopDisplayActive ? resolveLoopUrlForHour(activeLoopHour, visibleRenderMode) : null;

  return (
    <div className="flex h-full flex-col">
      <WeatherToolbar
        region={region}
        onRegionChange={setRegion}
        model={model}
        onModelChange={setModel}
        run={run}
        onRunChange={setRun}
        variable={variable}
        onVariableChange={setVariable}
        regions={regions}
        models={models}
        runs={runOptions}
        variables={variables}
        basemapMode={basemapMode}
        onBasemapModeChange={setBasemapMode}
        disabled={loading || models.length === 0}
      />

      <div className="relative flex-1 overflow-hidden">
        <MapCanvas
          tileUrl={tileUrl}
          contourGeoJsonUrl={contourGeoJsonUrl}
          region={region}
          regionViews={regionViews}
          opacity={opacity}
          mode={isLoopDisplayActive ? "scrub" : (isPlaying ? "autoplay" : "scrub")}
          variable={variable}
          variableKind={selectedVariableKind}
          overlayFadeOutZoom={overlayFadeOutZoom}
          zoomHintMinZoom={zoomHintMinZoom}
          basemapMode={basemapMode}
          prefetchTileUrls={prefetchTileUrls}
          crossfade={false}
          loopImageUrl={activeLoopUrl}
          loopActive={isLoopDisplayActive}
          onFrameSettled={handleFrameSettled}
          onTileReady={handleTileReady}
          onFrameLoadingChange={handleFrameLoadingChange}
          onTileViewportReady={handleTileViewportReady}
          onZoomHint={setShowZoomHint}
          onZoomBucketChange={setZoomBucket}
          onZoomRoutingSignal={handleZoomRoutingSignal}
          onMapHover={onHover}
          onMapHoverEnd={onHoverEnd}
        />

        {showBufferStatus && (
          <div className="absolute left-1/2 top-4 z-40 flex w-[min(92vw,420px)] -translate-x-1/2 flex-col gap-1.5 rounded-md border border-border/50 bg-[hsl(var(--toolbar))]/95 px-3 py-2 text-xs shadow-xl backdrop-blur-md">
            <div className="flex items-center justify-between">
              <span className="flex items-center gap-1.5 font-medium">
                <AlertCircle className="h-3.5 w-3.5" />
                {bufferStatusText}
              </span>
              {!isScrubLoading ? <span className="font-mono tabular-nums">{preloadPercent}%</span> : null}
            </div>
            {!isScrubLoading ? (
              <div className="h-1.5 overflow-hidden rounded-full bg-muted/70">
                <div
                  className="h-full rounded-full bg-primary transition-[width] duration-200 ease-out"
                  style={{ width: `${preloadPercent}%` }}
                />
              </div>
            ) : null}
          </div>
        )}

        {tooltip && (
          <div
            className="pointer-events-none absolute z-50 rounded-md border border-border/60 bg-[hsl(var(--toolbar))]/95 px-2.5 py-1.5 text-xs font-medium text-foreground shadow-xl backdrop-blur-md"
            style={{
              left: tooltip.x + 14,
              top: tooltip.y - 32,
            }}
          >
            {tooltip.value.toFixed(1)} {tooltip.units}
          </div>
        )}

        {error && (
          <div className="absolute left-4 top-4 z-40 flex items-center gap-2 rounded-md border border-destructive/50 bg-destructive/10 px-3 py-2 text-xs text-destructive shadow-lg backdrop-blur-md">
            <AlertCircle className="h-3.5 w-3.5" />
            {error}
          </div>
        )}

        {showZoomHint && (
          <div className="absolute left-1/2 top-4 z-40 flex -translate-x-1/2 items-center gap-2 rounded-md border border-border/50 bg-[hsl(var(--toolbar))]/95 px-3 py-2 text-xs shadow-xl backdrop-blur-md">
            <AlertCircle className="h-3.5 w-3.5" />
            GFS is low-resolution at this zoom. Switch to HRRR for sharper detail.
          </div>
        )}

        {renderMode === "tiles" && canUseLoopPlayback && isHighDetailZoom && (
          <div className="absolute left-1/2 top-14 z-40 flex -translate-x-1/2 items-center gap-2 rounded-md border border-border/50 bg-[hsl(var(--toolbar))]/95 px-3 py-2 text-xs shadow-xl backdrop-blur-md">
            <AlertCircle className="h-3.5 w-3.5" />
            High detail mode — zoom out for smooth loop
          </div>
        )}

        <MapLegend legend={legend} onOpacityChange={setOpacity} />

        <BottomForecastControls
          forecastHour={forecastHour}
          availableFrames={selectableFrameHours}
          onForecastHourChange={requestForecastHour}
          onScrubStateChange={setIsScrubbing}
          isPlaying={controlsIsPlaying}
          setIsPlaying={handleSetIsPlaying}
          runDateTimeISO={runDateTimeISO}
          disabled={loading}
          playDisabled={loading || selectableFrameHours.length === 0}
          transientStatus={frameStatusMessage}
        />
      </div>
    </div>
  );
}

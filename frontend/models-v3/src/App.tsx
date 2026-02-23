import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AlertCircle } from "lucide-react";

import { BottomForecastControls } from "@/components/bottom-forecast-controls";
import { MapCanvas } from "@/components/map-canvas";
import { type LegendPayload, MapLegend } from "@/components/map-legend";
import { WeatherToolbar } from "@/components/weather-toolbar";
import {
  buildContourUrl,
  type FrameRow,
  type LegendMeta,
  type RegionPreset,
  type VarRow,
  fetchFrames,
  fetchModels,
  fetchRegionPresets,
  fetchRuns,
  fetchVars,
} from "@/lib/api";
import {
  API_BASE,
  DEFAULTS,
  getPlaybackBufferPolicy,
  isAnimationDebugEnabled,
  isWebpDefaultRenderEnabled,
  VARIABLE_LABELS,
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
  if (id === "precip_ptype") {
    return VARIABLE_LABELS.precip_ptype;
  }
  if (preferredLabel && preferredLabel.trim()) {
    return preferredLabel.trim();
  }
  return VARIABLE_LABELS[id] ?? id;
}

function normalizeVarRows(rows: VarRow[]): Array<{ id: string; displayName?: string }> {
  const normalized: Array<{ id: string; displayName?: string }> = [];
  for (const row of rows) {
    if (typeof row === "string") {
      const id = row.trim();
      if (!id) continue;
      normalized.push({ id });
      continue;
    }
    const id = String(row.id ?? "").trim();
    if (!id) continue;
    const displayName = row.display_name ?? row.name ?? row.label;
    normalized.push({ id, displayName: displayName?.trim() || undefined });
  }
  return normalized;
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
  const [models, setModels] = useState<Option[]>([]);
  const [regions, setRegions] = useState<Option[]>([]);
  const [runs, setRuns] = useState<string[]>([]);
  const [variables, setVariables] = useState<Option[]>([]);
  const [frameRows, setFrameRows] = useState<FrameRow[]>([]);
  const [regionPresets, setRegionPresets] = useState<Record<string, RegionPreset>>({});

  const [model, setModel] = useState(DEFAULTS.model);
  const [region, setRegion] = useState(DEFAULTS.region);
  const [run, setRun] = useState(DEFAULTS.run);
  const [variable, setVariable] = useState(DEFAULTS.variable);
  const [forecastHour, setForecastHour] = useState(0);
  const [targetForecastHour, setTargetForecastHour] = useState(0);
  const [, setZoomBucket] = useState(Math.round(DEFAULTS.zoom));
  const [mapZoom, setMapZoom] = useState(DEFAULTS.zoom);
  const [zoomGestureActive, setZoomGestureActive] = useState(false);
  const [isPlaying, setIsPlaying] = useState(false);
  const [renderMode, setRenderMode] = useState<RenderModeState>(webpDefaultEnabled ? "webp_tier0" : "tiles");
  const [isLoopPreloading, setIsLoopPreloading] = useState(false);
  const [loopProgress, setLoopProgress] = useState({ total: 0, ready: 0, failed: 0 });
  const [loopBaseForecastHour, setLoopBaseForecastHour] = useState<number | null>(null);
  const [isPreloadingForPlay, setIsPreloadingForPlay] = useState(false);
  const [isScrubbing, setIsScrubbing] = useState(false);
  const [opacity, setOpacity] = useState(DEFAULTS.overlayOpacity);
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
  const datasetGenerationRef = useRef(0);
  const requestGenerationRef = useRef(0);
  const scrubRafRef = useRef<number | null>(null);
  const pendingScrubHourRef = useRef<number | null>(null);
  const animationDebugRef = useRef(isAnimationDebugEnabled());
  const autoplayPrimedRef = useRef(false);
  const frameStatusTimerRef = useRef<number | null>(null);
  const preloadProgressRef = useRef({
    lastBufferedCount: 0,
    lastProgressAt: 0,
  });
  const loopPreloadTokenRef = useRef(0);
  const loopReadyHoursRef = useRef<Set<number>>(new Set());
  const loopFailedHoursRef = useRef<Set<number>>(new Set());
  const mapZoomRef = useRef(DEFAULTS.zoom);
  const renderModeDwellTimerRef = useRef<number | null>(null);
  // Tracks current selector values so the async fast-path callback can guard against
  // stale-closure issues (updated every render, not inside an effect).
  const activeSelectorRef = useRef({ model, region, variable, run });
  activeSelectorRef.current = { model, region, variable, run };
  // Set to true when fast-path successfully bootstraps all state. Waterfall effects
  // check this and bail out early to avoid duplicate fetches.
  const bootstrappedRef = useRef(false);

  const frameHours = useMemo(() => {
    const hours = frameRows.map((row) => Number(row.fh)).filter(Number.isFinite);
    return Array.from(new Set(hours)).sort((a, b) => a - b);
  }, [frameRows]);

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
  const latestRunId = runs[0] ?? frameRows[0]?.run ?? null;
  const resolvedRunForRequests = run === "latest" ? (latestRunId ?? "latest") : run;

  const runOptions = useMemo<Option[]>(() => {
    return buildRunOptions(runs, latestRunId);
  }, [runs, latestRunId]);

  const loopUrlByHour = useMemo(() => {
    const apiRoot = API_BASE.replace(/\/api\/v3$/i, "").replace(/\/$/, "");
    const map = new Map<number, string>();
    for (const [fh, row] of frameByHour.entries()) {
      const loopUrl = row?.loop_webp_url;
      if (!loopUrl) {
        continue;
      }
      const absolute = /^https?:\/\//i.test(loopUrl)
        ? loopUrl
        : `${apiRoot}${loopUrl.startsWith("/") ? "" : "/"}${loopUrl}`;
      map.set(fh, absolute);
    }
    return map;
  }, [frameByHour]);

  const canUseLoopPlayback = useMemo(() => {
    if (frameHours.length <= 1) {
      return false;
    }
    return frameHours.every((fh) => Boolean(loopUrlByHour.get(fh)));
  }, [frameHours, loopUrlByHour]);

  useEffect(() => {
    mapZoomRef.current = mapZoom;
  }, [mapZoom]);

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

  const isLoopPlaybackLocked = renderMode !== "tiles" && canUseLoopPlayback && (isPlaying || isLoopPreloading);
  const isLoopDisplayActive = renderMode !== "tiles" && canUseLoopPlayback && (isPlaying || isLoopPreloading || isScrubbing);
  const mapForecastHour = isLoopPlaybackLocked && Number.isFinite(loopBaseForecastHour)
    ? (loopBaseForecastHour as number)
    : forecastHour;

  const tileUrlForHour = useCallback(
    (fh: number): string => {
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
    [model, resolvedRunForRequests, variable, frameHours, frameByHour, frameRows]
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

  const debugLog = useCallback((message: string, payload?: Record<string, unknown>) => {
    if (!animationDebugRef.current) {
      return;
    }
    if (payload) {
      console.debug(`[animation] ${message}`, payload);
      return;
    }
    console.debug(`[animation] ${message}`);
  }, []);

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

    const frameSet = new Set(frameHours);
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
          debugLog("frame failed", {
            fh,
            retries: nextRetry,
            ageMs,
            hardDeadlineMs: FRAME_HARD_DEADLINE_MS,
          });
        } else {
          const retryDelayMs = FRAME_RETRY_BASE_MS * 2 ** (nextRetry - 1);
          frameNextRetryAtRef.current.set(fh, now + retryDelayMs);
          debugLog("inflight frame expired", {
            fh,
            ttlMs: INFLIGHT_FRAME_TTL_MS,
            retries: nextRetry,
            retryDelayMs,
          });
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
    debugLog("buffer snapshot", {
      bufferedCount,
      bufferedAheadCount,
      terminalCount,
      terminalAheadCount,
      failedCount,
      inFlightCount: inFlight.size,
      queueDepth,
      totalFrames,
    });
  }, [frameHours, forecastHour, debugLog]);

  const contourGeoJsonUrl = useMemo(() => {
    if (variable !== "tmp2m") {
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
  }, [currentFrame, model, resolvedRunForRequests, variable, mapForecastHour]);

  const legend = useMemo(() => {
    const normalizedMeta = extractLegendMeta(currentFrame) ?? extractLegendMeta(frameRows[0] ?? null);
    return buildLegend(normalizedMeta, opacity);
  }, [currentFrame, frameRows, opacity]);

  const prefetchHours = useMemo(() => {
    if (isLoopDisplayActive || frameHours.length === 0) {
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

    const currentIndex = frameHours.indexOf(forecastHour);
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
    isLoopDisplayActive,
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

    for (const [url, ts] of ready) {
      if (now - ts > READY_URL_TTL_MS) {
        ready.delete(url);
      }
    }

    if (ready.size > READY_URL_LIMIT) {
      const entries = [...ready.entries()].sort((a, b) => a[1] - b[1]);
      for (const [url] of entries.slice(0, ready.size - READY_URL_LIMIT)) {
        ready.delete(url);
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
    const stats = readyLatencyStatsRef.current;
    const avgReadyMs = stats.count > 0 ? Math.round(stats.totalMs / stats.count) : null;
    debugLog("frame ready", {
      fh: frameHour,
      avgReadyMs,
      samples: stats.count,
    });
    updateBufferSnapshot();
  }, [tileUrlToHour, updateBufferSnapshot, debugLog]);

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
    if (isPlaying) {
      return false;
    }
    return Boolean(mapLoadingTileUrl && mapLoadingTileUrl === tileUrl && settledTileUrl !== tileUrl);
  }, [isPlaying, mapLoadingTileUrl, tileUrl, settledTileUrl]);

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
    setIsLoopPreloading(false);
    setLoopProgress({ total: frameHours.length, ready: 0, failed: 0 });
    setLoopBaseForecastHour(null);
    loopPreloadTokenRef.current += 1;
    loopReadyHoursRef.current.clear();
    loopFailedHoursRef.current.clear();
    setIsPreloadingForPlay(false);
    setRenderMode(webpDefaultEnabled ? "webp_tier0" : "tiles");
    preloadProgressRef.current = {
      lastBufferedCount: 0,
      lastProgressAt: Date.now(),
    };
    debugLog("dataset generation changed", {
      generation: datasetGenerationRef.current,
      model,
      run: resolvedRunForRequests,
      variable,
    });
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
    model,
    resolvedRunForRequests,
    variable,
    frameHours.length,
    debugLog,
    webpDefaultEnabled,
  ]);

  useEffect(() => {
    if (!isLoopPreloading) {
      return;
    }
    if (!canUseLoopPlayback || frameHours.length === 0) {
      setIsLoopPreloading(false);
      setRenderMode("tiles");
      return;
    }

    const token = ++loopPreloadTokenRef.current;
    const readySet = new Set<number>();
    const failedSet = new Set<number>();
    loopReadyHoursRef.current = readySet;
    loopFailedHoursRef.current = failedSet;
    setLoopProgress({ total: frameHours.length, ready: 0, failed: 0 });

    const mark = (fh: number, ok: boolean) => {
      if (token !== loopPreloadTokenRef.current) {
        return;
      }
      if (ok) {
        readySet.add(fh);
      } else {
        failedSet.add(fh);
      }
      setLoopProgress({
        total: frameHours.length,
        ready: readySet.size,
        failed: failedSet.size,
      });

      if (readySet.size + failedSet.size < frameHours.length) {
        return;
      }

      setIsLoopPreloading(false);
      const minReady = Math.min(LOOP_PRELOAD_MIN_READY, frameHours.length);
      if (readySet.size >= minReady) {
        setIsPlaying(true);
        return;
      }
      setRenderMode("tiles");
      setIsPlaying(false);
      showTransientFrameStatus("Loop preload failed");
    };

    frameHours.forEach((fh) => {
      const url = loopUrlByHour.get(fh);
      if (!url) {
        mark(fh, false);
        return;
      }
      const img = new Image();
      img.decoding = "async";
      img.onload = () => mark(fh, true);
      img.onerror = () => mark(fh, false);
      img.src = url;
    });

    return () => {
      loopPreloadTokenRef.current += 1;
    };
  }, [isLoopPreloading, canUseLoopPlayback, frameHours, loopUrlByHour, showTransientFrameStatus]);

  useEffect(() => {
    if (!isPlaying || renderMode === "tiles" || frameHours.length === 0) {
      return;
    }

    const interval = window.setInterval(() => {
      const ready = loopReadyHoursRef.current;
      const currentIndex = frameHours.indexOf(forecastHour);
      if (currentIndex < 0) {
        return;
      }

      const nextIndex = currentIndex + 1;
      if (nextIndex >= frameHours.length) {
        setIsPlaying(false);
        return;
      }

      for (let idx = nextIndex; idx < frameHours.length; idx += 1) {
        const candidate = frameHours[idx];
        if (ready.has(candidate)) {
          setTargetForecastHour(candidate);
          return;
        }
      }

      setIsPlaying(false);
    }, AUTOPLAY_TICK_MS);

    return () => window.clearInterval(interval);
  }, [isPlaying, renderMode, frameHours, forecastHour]);

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
        setTargetForecastHour(requestedHour);
        return;
      }

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
        setTargetForecastHour(latestRequestedHour as number);
      });
    },
    [isScrubbing]
  );

  // ── Fast-path: fire all discovery fetches in parallel on mount ─────────────────────────────
  // On a 50 ms RTT connection the standard sequential waterfall (models → regions →
  // runs + vars → frames) costs 200-400 ms before MapLibre can start loading tiles.
  // Here we fire all five fetches simultaneously. If they all succeed and the user has
  // not yet touched any selector, we apply the results in one batched update and skip
  // the waterfall entirely. If any fetch fails we bail out silently and let the
  // sequential waterfall effects (which run in parallel on mount) finish normally.
  useEffect(() => {
    const controller = new AbortController();
    const generation = requestGenerationRef.current;

    Promise.all([
      fetchModels({ signal: controller.signal }),
      fetchRegionPresets({ signal: controller.signal }),
      fetchRuns(DEFAULTS.model, { signal: controller.signal }),
      fetchVars(DEFAULTS.model, DEFAULTS.run, { signal: controller.signal }),
      fetchFrames(DEFAULTS.model, DEFAULTS.run, DEFAULTS.variable, { signal: controller.signal }),
    ])
      .then(([modelsData, regionPresetData, runsData, varsData, framesData]) => {
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;

        // If the user changed a selector while requests were in-flight, bail out and
        // let the sequential waterfall (still running concurrently) handle things.
        const s = activeSelectorRef.current;
        if (
          s.model !== DEFAULTS.model ||
          s.region !== DEFAULTS.region ||
          s.variable !== DEFAULTS.variable ||
          s.run !== DEFAULTS.run
        ) {
          return;
        }

        // Batch-apply all discovery state in one synchronous pass.
        const modelOptions = modelsData.map((item) => ({
          value: item.id,
          label: item.name || item.id,
        }));
        const modelIds = modelOptions.map((opt) => opt.value);
        setModels(modelOptions);
        setModel(pickPreferred(modelIds, DEFAULTS.model));

        setRegionPresets(regionPresetData);
        const regionIds = Object.keys(regionPresetData);
        const regionOptions = regionIds.map((id) => ({
          value: id,
          label: makeRegionLabel(id, regionPresetData[id]),
        }));
        setRegions(regionOptions);
        setRegion((prev) => (regionIds.includes(prev) ? prev : pickPreferred(regionIds, DEFAULTS.region)));

        setRuns(runsData);
        setRun(DEFAULTS.run);

        const normalizedVars = normalizeVarRows(varsData);
        const variableOptions = normalizedVars.map((entry) => ({
          value: entry.id,
          label: makeVariableLabel(entry.id, entry.displayName),
        }));
        const variableIds = variableOptions.map((opt) => opt.value);
        setVariables(variableOptions);
        setVariable((prev) => (variableIds.includes(prev) ? prev : pickPreferred(variableIds, DEFAULTS.variable)));

        setFrameRows(framesData);
        const frames = framesData.map((row) => Number(row.fh)).filter(Number.isFinite);
        setForecastHour((prev) => nearestFrame(frames, prev));
        setTargetForecastHour((prev) => nearestFrame(frames, prev));

        // Signal that all discovery state has been committed — waterfall effects will
        // see this flag and skip their own fetches.
        bootstrappedRef.current = true;
        setLoading(false);
      })
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") {
          return;
        }
        // Fast-path failed silently. The sequential waterfall handles recovery.
      });

    return () => {
      controller.abort();
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (bootstrappedRef.current) return;
    const controller = new AbortController();
    const generation = requestGenerationRef.current;

    async function loadModels() {
      setLoading(true);
      setError(null);
      try {
        const data = await fetchModels({ signal: controller.signal });
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;
        const options = data.map((item) => ({ value: item.id, label: item.name || item.id }));
        setModels(options);
        const modelIds = options.map((opt) => opt.value);
        const nextModel = pickPreferred(modelIds, DEFAULTS.model);
        setModel(nextModel);
      } catch (err) {
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;
        setError(err instanceof Error ? err.message : "Failed to load models");
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false);
        }
      }
    }

    loadModels();
    return () => {
      controller.abort();
    };
  }, []);

  useEffect(() => {
    if (bootstrappedRef.current) return;
    if (!model) return;
    const controller = new AbortController();
    const generation = requestGenerationRef.current;

    async function loadRegions() {
      setError(null);
      try {
        void model;
        const presets = await fetchRegionPresets({ signal: controller.signal });
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;
        setRegionPresets(presets);
        const ids = Object.keys(presets);
        const options = ids.map((id) => ({ value: id, label: makeRegionLabel(id, presets[id]) }));
        setRegions(options);
        const regionIds = options.map((opt) => opt.value);
        const nextRegion = pickPreferred(regionIds, DEFAULTS.region);
        setRegion((prev) => (regionIds.includes(prev) ? prev : nextRegion));
      } catch (err) {
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;
        setError(err instanceof Error ? err.message : "Failed to load regions");
      }
    }

    loadRegions();
    return () => {
      controller.abort();
    };
  }, [model]);

  useEffect(() => {
    if (bootstrappedRef.current) return;
    if (!model) return;
    const controller = new AbortController();
    const generation = requestGenerationRef.current;

    async function loadRunsAndVars() {
      setError(null);
      try {
        const runData = await fetchRuns(model, { signal: controller.signal });
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;

        const nextRun = run !== "latest" && runData.includes(run) ? run : "latest";
        const varData = await fetchVars(model, nextRun, { signal: controller.signal });
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;

        setRuns(runData);

        const normalizedVars = normalizeVarRows(varData);
        const variableOptions = normalizedVars.map((entry) => ({
          value: entry.id,
          label: makeVariableLabel(entry.id, entry.displayName),
        }));
        setVariables(variableOptions);

        setRun(nextRun);

        const variableIds = variableOptions.map((opt) => opt.value);
        const nextVar = pickPreferred(variableIds, DEFAULTS.variable);
        setVariable((prev) => (variableIds.includes(prev) ? prev : nextVar));
      } catch (err) {
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;
        setError(err instanceof Error ? err.message : "Failed to load runs/variables");
      }
    }

    loadRunsAndVars();
    return () => {
      controller.abort();
    };
  }, [model, run]);

  useEffect(() => {
    setFrameRows([]);
    setForecastHour(0);
    setTargetForecastHour(0);
  }, [model, run, variable]);

  useEffect(() => {
    if (!model || !variable) return;
    const controller = new AbortController();
    const generation = requestGenerationRef.current;

    async function loadFrames() {
      setError(null);
      try {
        const rows = await fetchFrames(model, resolvedRunForRequests, variable, { signal: controller.signal });
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;
        setFrameRows(rows);
        const frameMeta = extractLegendMeta(rows[0] ?? null);
        const variableDisplayName = frameMeta?.display_name?.trim();
        if (variableDisplayName && variable !== "precip_ptype") {
          setVariables((prev) =>
            prev.map((option) =>
              option.value === variable ? { ...option, label: makeVariableLabel(option.value, variableDisplayName) } : option
            )
          );
        }
        const frames = rows.map((row) => Number(row.fh)).filter(Number.isFinite);
        setForecastHour((prev) => nearestFrame(frames, prev));
        setTargetForecastHour((prev) => nearestFrame(frames, prev));
      } catch (err) {
        if (controller.signal.aborted || generation !== requestGenerationRef.current) return;
        setError(err instanceof Error ? err.message : "Failed to load frames");
        setFrameRows([]);
      }
    }

    loadFrames();
    return () => {
      controller.abort();
    };
  }, [model, run, variable, resolvedRunForRequests]);

  useEffect(() => {
    let cancelled = false;
    let tickController: AbortController | null = null;

    const interval = window.setInterval(() => {
      if (document.hidden || !model || !variable) {
        return;
      }
      tickController?.abort();
      tickController = new AbortController();
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
          setForecastHour((prev) => nearestFrame(frames, prev));
          setTargetForecastHour((prev) => nearestFrame(frames, prev));
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
  }, [model, run, variable, resolvedRunForRequests]);

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
        debugLog("autopause low ahead buffer", {
          ahead: bufferSnapshot.bufferedAheadCount,
          minAheadRequired,
          totalFrames: frameHours.length,
          fh: forecastHour,
        });
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
    debugLog,
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
      setIsLoopPreloading(false);
      setIsPreloadingForPlay(false);
      return;
    }
    if (loading || frameHours.length === 0) {
      return;
    }

    if (canUseLoopPlayback && webpDefaultEnabled && renderMode !== "tiles") {
      setLoopBaseForecastHour(forecastHour);
      setIsPlaying(false);
      setIsPreloadingForPlay(false);
      setIsLoopPreloading(true);
      showTransientFrameStatus("Loading loop frames");
      return;
    }

    setRenderMode("tiles");
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
    webpDefaultEnabled,
    renderMode,
    showTransientFrameStatus,
  ]);

  const handleZoomRoutingSignal = useCallback((payload: { zoom: number; gestureActive: boolean }) => {
    setMapZoom(payload.zoom);
    setZoomGestureActive(payload.gestureActive);
  }, []);

  useEffect(() => {
    if (isPlaying && isScrubbing) {
      setIsScrubbing(false);
    }
  }, [isPlaying, isScrubbing]);

  useEffect(() => {
    return () => {
      clearFrameStatusTimer();
      if (scrubRafRef.current !== null) {
        window.cancelAnimationFrame(scrubRafRef.current);
      }
    };
  }, [clearFrameStatusTimer]);

  useEffect(() => {
    if (frameHours.length === 0) {
      return;
    }

    const nextTarget = nearestFrame(frameHours, targetForecastHour);
    if (nextTarget === forecastHour) {
      return;
    }
    setForecastHour(nextTarget);
  }, [targetForecastHour, forecastHour, frameHours]);

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
    : isLoopPreloading
      ? `Loading loop frames ${preloadBufferedCount}/${preloadTotal}`
      : `Loading frames ${preloadBufferedCount}/${preloadTotal}`;
  const activeLoopUrl = isLoopDisplayActive ? (loopUrlByHour.get(forecastHour) ?? null) : null;

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
          model={model}
          prefetchTileUrls={prefetchTileUrls}
          crossfade={false}
          loopImageUrl={activeLoopUrl}
          loopActive={isLoopDisplayActive}
          onFrameSettled={handleFrameSettled}
          onTileReady={handleTileReady}
          onFrameLoadingChange={handleFrameLoadingChange}
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
              <div className="text-[10px] text-muted-foreground">
                {isLoopPreloading
                  ? `Ready ${loopProgress.ready} • Failed ${loopProgress.failed}`
                  : `Ready ${bufferSnapshot.bufferedCount} • Failed ${bufferSnapshot.failedCount}`}
              </div>
            ) : null}
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

        {(isPlaying || isPreloadingForPlay || isLoopPreloading) && (
          <div className="absolute left-4 top-20 z-50 rounded-md border border-border/40 bg-[hsl(var(--toolbar))]/90 px-2.5 py-1.5 text-[10px] text-foreground shadow-lg backdrop-blur-md">
            {isLoopPreloading || renderMode !== "tiles"
              ? `loopReady ${loopProgress.ready} • loopFailed ${loopProgress.failed} • loopTotal ${loopProgress.total}`
              : `aheadReady ${bufferSnapshot.bufferedAheadCount} • aheadTerminal ${bufferSnapshot.terminalAheadCount} • inflightTiles ${bufferSnapshot.inFlightCount} • queueDepth ${bufferSnapshot.queueDepth}`}
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

        <MapLegend legend={legend} onOpacityChange={setOpacity} />

        <BottomForecastControls
          forecastHour={forecastHour}
          availableFrames={frameHours}
          onForecastHourChange={requestForecastHour}
          onScrubStateChange={setIsScrubbing}
          isPlaying={controlsIsPlaying}
          setIsPlaying={handleSetIsPlaying}
          runDateTimeISO={runDateTimeISO}
          disabled={loading}
          playDisabled={loading || frameHours.length === 0}
          transientStatus={frameStatusMessage}
        />
      </div>
    </div>
  );
}

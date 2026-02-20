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
  type VarRow,
  fetchFrames,
  fetchModels,
  fetchRegions,
  fetchRuns,
  fetchVars,
} from "@/lib/api";
import { DEFAULTS, VARIABLE_LABELS } from "@/lib/config";
import { buildRunOptions } from "@/lib/run-options";
import { buildTileUrlFromFrame } from "@/lib/tiles";
import { useSampleTooltip } from "@/lib/use-sample-tooltip";

const AUTOPLAY_TICK_MS = 400;
const AUTOPLAY_READY_AHEAD = 2;
const AUTOPLAY_SKIP_WINDOW = 3;
const FRAME_STATUS_BADGE_MS = 900;
const READY_URL_TTL_MS = 30_000;
const READY_URL_LIMIT = 160;

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

function makeRegionLabel(id: string): string {
  return id.toUpperCase();
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
  const [models, setModels] = useState<Option[]>([]);
  const [regions, setRegions] = useState<Option[]>([]);
  const [runs, setRuns] = useState<string[]>([]);
  const [variables, setVariables] = useState<Option[]>([]);
  const [frameRows, setFrameRows] = useState<FrameRow[]>([]);

  const [model, setModel] = useState(DEFAULTS.model);
  const [region, setRegion] = useState(DEFAULTS.region);
  const [run, setRun] = useState(DEFAULTS.run);
  const [variable, setVariable] = useState(DEFAULTS.variable);
  const [forecastHour, setForecastHour] = useState(0);
  const [targetForecastHour, setTargetForecastHour] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);
  const [isScrubbing, setIsScrubbing] = useState(false);
  const [opacity, setOpacity] = useState(DEFAULTS.overlayOpacity);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [settledTileUrl, setSettledTileUrl] = useState<string | null>(null);
  const [mapLoadingTileUrl, setMapLoadingTileUrl] = useState<string | null>(null);
  const [frameStatusMessage, setFrameStatusMessage] = useState<string | null>(null);
  const [showZoomHint, setShowZoomHint] = useState(false);
  const latestTileUrlRef = useRef<string>("");
  const readyTileUrlsRef = useRef<Map<string, number>>(new Map());
  const autoplayPrimedRef = useRef(false);
  const frameStatusTimerRef = useRef<number | null>(null);
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

  const currentFrame = frameByHour.get(forecastHour) ?? frameRows[0] ?? null;
  const latestRunId = runs[0] ?? frameRows[0]?.run ?? null;
  const resolvedRunForRequests = run === "latest" ? (latestRunId ?? "latest") : run;

  const runOptions = useMemo<Option[]>(() => {
    return buildRunOptions(runs, latestRunId);
  }, [runs, latestRunId]);

  const tileUrlForHour = useCallback(
    (fh: number): string => {
      const fallbackFh = frameHours[0] ?? 0;
      const resolvedFh = Number.isFinite(fh) ? fh : fallbackFh;
      return buildTileUrlFromFrame({
        model,
        region,
        run: resolvedRunForRequests,
        varKey: variable,
        fh: resolvedFh,
        frameRow: frameByHour.get(resolvedFh) ?? frameRows[0] ?? null,
      });
    },
    [model, region, resolvedRunForRequests, variable, frameHours, frameByHour, frameRows]
  );

  const tileUrl = useMemo(() => {
    return tileUrlForHour(forecastHour);
  }, [tileUrlForHour, forecastHour]);

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
      region,
      run: resolvedRunForRequests,
      varKey: variable,
      fh: forecastHour,
      key: "iso32f",
    });
  }, [currentFrame, model, region, resolvedRunForRequests, variable, forecastHour]);

  const legend = useMemo(() => {
    const normalizedMeta = extractLegendMeta(currentFrame) ?? extractLegendMeta(frameRows[0] ?? null);
    return buildLegend(normalizedMeta, opacity);
  }, [currentFrame, frameRows, opacity]);

  const prefetchTileUrls = useMemo(() => {
    if (frameHours.length < 2) return [];

    const prefetchCount = isPlaying ? 4 : isScrubbing ? 2 : 0;
    if (prefetchCount <= 0) {
      return [];
    }

    const currentIndex = frameHours.indexOf(forecastHour);
    const start = currentIndex >= 0 ? currentIndex : 0;
    const nextHours = Array.from({ length: prefetchCount }, (_, idx) => {
      const i = start + idx + 1;
      return i >= frameHours.length ? Number.NaN : frameHours[i];
    });
    const dedup = Array.from(new Set(nextHours.filter((fh) => Number.isFinite(fh) && fh !== forecastHour)));
    return dedup.map((fh) => tileUrlForHour(fh));
  }, [frameHours, forecastHour, tileUrlForHour, isPlaying, isScrubbing]);

  const effectiveRunId = currentFrame?.run ?? (run !== "latest" ? run : latestRunId);
  const runDateTimeISO = runIdToIso(effectiveRunId);

  // ── Hover-for-data tooltip ──────────────────────────────────────────
  const { tooltip, onHover, onHoverEnd } = useSampleTooltip({
    model,
    region,
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
    if (loadedTileUrl === latestTileUrlRef.current) {
      setSettledTileUrl(loadedTileUrl);
    }
  }, [markTileReady]);

  const handleTileReady = useCallback((loadedTileUrl: string) => {
    markTileReady(loadedTileUrl);
    if (loadedTileUrl === latestTileUrlRef.current) {
      setSettledTileUrl(loadedTileUrl);
    }
  }, [markTileReady]);

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

  // ── Fast-path: fire all discovery fetches in parallel on mount ─────────────────────────────
  // On a 50 ms RTT connection the standard sequential waterfall (models → regions →
  // runs + vars → frames) costs 200-400 ms before MapLibre can start loading tiles.
  // Here we fire all five fetches simultaneously. If they all succeed and the user has
  // not yet touched any selector, we apply the results in one batched update and skip
  // the waterfall entirely. If any fetch fails we bail out silently and let the
  // sequential waterfall effects (which run in parallel on mount) finish normally.
  useEffect(() => {
    let cancelled = false;

    Promise.all([
      fetchModels(),
      fetchRegions(DEFAULTS.model),
      fetchRuns(DEFAULTS.model, DEFAULTS.region),
      fetchVars(DEFAULTS.model, DEFAULTS.region, DEFAULTS.run),
      fetchFrames(DEFAULTS.model, DEFAULTS.region, DEFAULTS.run, DEFAULTS.variable),
    ])
      .then(([modelsData, regionsData, runsData, varsData, framesData]) => {
        if (cancelled) return;

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

        const regionOptions = regionsData.map((id) => ({
          value: id,
          label: makeRegionLabel(id),
        }));
        const regionIds = regionOptions.map((opt) => opt.value);
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
      .catch(() => {
        // Fast-path failed silently. The sequential waterfall handles recovery.
      });

    return () => {
      cancelled = true;
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (bootstrappedRef.current) return;
    let cancelled = false;

    async function loadModels() {
      setLoading(true);
      setError(null);
      try {
        const data = await fetchModels();
        if (cancelled) return;
        const options = data.map((item) => ({ value: item.id, label: item.name || item.id }));
        setModels(options);
        const modelIds = options.map((opt) => opt.value);
        const nextModel = pickPreferred(modelIds, DEFAULTS.model);
        setModel(nextModel);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load models");
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    loadModels();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (bootstrappedRef.current) return;
    if (!model) return;
    let cancelled = false;

    async function loadRegions() {
      setError(null);
      try {
        const data = await fetchRegions(model);
        if (cancelled) return;
        const options = data.map((id) => ({ value: id, label: makeRegionLabel(id) }));
        setRegions(options);
        const regionIds = options.map((opt) => opt.value);
        const nextRegion = pickPreferred(regionIds, DEFAULTS.region);
        setRegion((prev) => (regionIds.includes(prev) ? prev : nextRegion));
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load regions");
      }
    }

    loadRegions();
    return () => {
      cancelled = true;
    };
  }, [model]);

  useEffect(() => {
    if (bootstrappedRef.current) return;
    if (!model || !region) return;
    let cancelled = false;

    async function loadRunsAndVars() {
      setError(null);
      try {
        const runData = await fetchRuns(model, region);
        if (cancelled) return;

        const nextRun = run !== "latest" && runData.includes(run) ? run : "latest";
        const varData = await fetchVars(model, region, nextRun);
        if (cancelled) return;

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
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load runs/variables");
      }
    }

    loadRunsAndVars();
    return () => {
      cancelled = true;
    };
  }, [model, region, run]);

  useEffect(() => {
    setFrameRows([]);
    setForecastHour(0);
    setTargetForecastHour(0);
  }, [model, region]);

  useEffect(() => {
    if (bootstrappedRef.current) return;
    if (!model || !region || !variable) return;
    let cancelled = false;

    async function loadFrames() {
      setError(null);
      try {
        const rows = await fetchFrames(model, region, resolvedRunForRequests, variable);
        if (cancelled) return;
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
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load frames");
        setFrameRows([]);
      }
    }

    loadFrames();
    return () => {
      cancelled = true;
    };
  }, [model, region, run, variable, resolvedRunForRequests]);

  useEffect(() => {
    let cancelled = false;

    const interval = window.setInterval(() => {
      if (document.hidden || !model || !region || !variable) {
        return;
      }
      fetchFrames(model, region, resolvedRunForRequests, variable)
        .then((rows) => {
          if (cancelled) {
            return;
          }
          setFrameRows(rows);
          const frames = rows.map((row) => Number(row.fh)).filter(Number.isFinite);
          setForecastHour((prev) => nearestFrame(frames, prev));
          setTargetForecastHour((prev) => nearestFrame(frames, prev));
        })
        .catch(() => {
          // Background refresh should not interrupt active UI.
        });
    }, 30000);

    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [model, region, run, variable, resolvedRunForRequests]);

  useEffect(() => {
    if (!isPlaying || frameHours.length === 0) return;

    const interval = window.setInterval(() => {
      const currentIndex = frameHours.indexOf(forecastHour);
      if (currentIndex < 0) return;

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
  }, [isPlaying, frameHours, forecastHour, isTileReady, tileUrlForHour, showTransientFrameStatus]);

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

  useEffect(() => {
    if (isPlaying && isScrubbing) {
      setIsScrubbing(false);
    }
  }, [isPlaying, isScrubbing]);

  useEffect(() => {
    return () => {
      clearFrameStatusTimer();
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
          opacity={opacity}
          mode={isPlaying ? "autoplay" : "scrub"}
          variable={variable}
          model={model}
          prefetchTileUrls={prefetchTileUrls}
          crossfade={false}
          onFrameSettled={handleFrameSettled}
          onTileReady={handleTileReady}
          onFrameLoadingChange={handleFrameLoadingChange}
          onZoomHint={setShowZoomHint}
          onMapHover={onHover}
          onMapHoverEnd={onHoverEnd}
        />

        {isScrubLoading && (
          <div className="absolute left-1/2 top-4 z-40 flex -translate-x-1/2 items-center gap-2 rounded-md border border-border/50 bg-[hsl(var(--toolbar))]/95 px-3 py-2 text-xs shadow-xl backdrop-blur-md">
            <AlertCircle className="h-3.5 w-3.5" />
            Loading...
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
          onForecastHourChange={setTargetForecastHour}
          onScrubStateChange={setIsScrubbing}
          isPlaying={isPlaying}
          setIsPlaying={setIsPlaying}
          runDateTimeISO={runDateTimeISO}
          disabled={loading}
          transientStatus={frameStatusMessage}
        />
      </div>
    </div>
  );
}

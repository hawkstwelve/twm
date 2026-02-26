import { API_ORIGIN, API_V4_BASE } from "@/lib/config";

export type ModelOption = {
  id: string;
  name: string;
};

export type CapabilityVariable = {
  var_key: string;
  display_name?: string;
  kind?: string | null;
  units?: string | null;
  order?: number | null;
  default_fh?: number | null;
  buildable?: boolean;
  color_map_id?: string | null;
  constraints?: Record<string, unknown>;
  derived?: boolean;
  derive_strategy_id?: string | null;
};

export type CapabilityModel = {
  model_id: string;
  name: string;
  product?: string | null;
  canonical_region?: string | null;
  defaults?: Record<string, unknown>;
  constraints?: Record<string, unknown>;
  run_discovery?: Record<string, unknown>;
  variables: Record<string, CapabilityVariable>;
};

export type CapabilitiesResponse = {
  contract_version: string;
  supported_models: string[];
  model_catalog: Record<string, CapabilityModel>;
  availability: Record<
    string,
    {
      latest_run: string | null;
      published_runs: string[];
      latest_run_ready?: boolean;
      latest_run_ready_vars?: string[];
      latest_run_ready_frame_count?: number;
    }
  >;
};

export type RegionPreset = {
  label?: string;
  bbox: [number, number, number, number];
  defaultCenter: [number, number];
  defaultZoom: number;
  minZoom?: number;
  maxZoom?: number;
};

export type LegendStops = [number | string, string][];

export type LegendMeta = {
  kind?: string;
  display_name?: string;
  legend_title?: string;
  units?: string;
  legend_stops?: LegendStops;
  legend?: { type?: string; stops?: LegendStops };
  colors?: string[];
  levels?: number[];
  ptype_order?: string[];
  ptype_breaks?: Record<string, { offset: number; count: number }>;
  ptype_levels?: Record<string, number[]>;
  range?: [number, number];
  bins_per_ptype?: number;
  contours?: Record<
    string,
    {
      format?: string;
      path?: string;
      srs?: string;
      level?: number;
    }
  >;
};

export type FrameRow = {
  fh: number;
  has_cog: boolean;
  run?: string;
  tile_url_template?: string;
  loop_webp_url?: string;
  loop_webp_tier0_url?: string;
  loop_webp_tier1_url?: string;
  meta?: {
    meta?: LegendMeta | null;
  } | null;
};

export type LoopManifestFrame = {
  fh: number;
  url: string;
};

export type LoopManifestTier = {
  tier: number;
  max_dim?: number;
  frames: LoopManifestFrame[];
};

export type LoopManifestResponse = {
  manifest_version: number;
  run: string;
  model: string;
  var: string;
  bbox?: [number, number, number, number];
  projection?: string;
  loop_tiers: LoopManifestTier[];
};

export type RunManifestFrame = {
  fh: number;
  valid_time?: string;
};

export type RunManifestVariable = {
  display_name?: string;
  name?: string;
  label?: string;
  frames?: RunManifestFrame[];
};

export type RunManifestResponse = {
  contract_version?: string;
  model: string;
  run: string;
  region?: string;
  last_updated?: string;
  variables: Record<string, RunManifestVariable>;
};

export type VarRow =
  | string
  | {
      id: string;
      display_name?: string;
      name?: string;
      label?: string;
    };

type FetchOptions = {
  signal?: AbortSignal;
};

async function fetchJson<T>(url: string, options?: FetchOptions): Promise<T> {
  const response = await fetch(url, { credentials: "omit", signal: options?.signal });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isRunManifestResponse(value: unknown): value is RunManifestResponse {
  if (!isObject(value)) {
    return false;
  }
  if (typeof value.model !== "string" || typeof value.run !== "string") {
    return false;
  }
  if (!isObject(value.variables)) {
    return false;
  }

  for (const varEntry of Object.values(value.variables)) {
    if (!isObject(varEntry)) {
      return false;
    }
    if ("frames" in varEntry && !Array.isArray(varEntry.frames)) {
      return false;
    }
    if (Array.isArray(varEntry.frames)) {
      for (const frame of varEntry.frames) {
        if (!isObject(frame)) {
          return false;
        }
        if (!Number.isFinite(Number(frame.fh))) {
          return false;
        }
      }
    }
  }
  return true;
}

const REGIONS_CACHE_KEY = "twf_v3_regions_cache";
const REGIONS_ETAG_KEY = "twf_v3_regions_etag";

type RegionsResponse = {
  regions: Record<string, RegionPreset>;
};

export async function fetchRegionPresets(options?: FetchOptions): Promise<Record<string, RegionPreset>> {
  const cachedRaw = localStorage.getItem(REGIONS_CACHE_KEY);
  const etag = localStorage.getItem(REGIONS_ETAG_KEY);
  const headers: Record<string, string> = {};
  if (etag) {
    headers["If-None-Match"] = etag;
  }

  const response = await fetch(`${API_ORIGIN}/api/regions`, {
    credentials: "omit",
    headers,
    signal: options?.signal,
  });

  if (response.status === 304 && cachedRaw) {
    try {
      const parsed = JSON.parse(cachedRaw) as RegionsResponse;
      return parsed.regions ?? {};
    } catch {
      return {};
    }
  }

  if (!response.ok) {
    if (cachedRaw) {
      try {
        const parsed = JSON.parse(cachedRaw) as RegionsResponse;
        return parsed.regions ?? {};
      } catch {
        return {};
      }
    }
    throw new Error(`Request failed: ${response.status} ${response.statusText}`);
  }

  const payload = (await response.json()) as RegionsResponse;
  const nextEtag = response.headers.get("ETag");
  localStorage.setItem(REGIONS_CACHE_KEY, JSON.stringify(payload));
  if (nextEtag) {
    localStorage.setItem(REGIONS_ETAG_KEY, nextEtag);
  }
  return payload.regions ?? {};
}

export async function fetchModels(options?: FetchOptions): Promise<ModelOption[]> {
  return fetchJson<ModelOption[]>(`${API_V4_BASE}/models`, options);
}

export async function fetchCapabilities(options?: FetchOptions): Promise<CapabilitiesResponse> {
  return fetchJson<CapabilitiesResponse>(`${API_V4_BASE}/capabilities`, options);
}

export async function fetchRegions(model: string, options?: FetchOptions): Promise<string[]> {
  void model;
  const regions = await fetchRegionPresets(options);
  return Object.keys(regions);
}

export async function fetchRuns(model: string, options?: FetchOptions): Promise<string[]> {
  return fetchJson<string[]>(
    `${API_V4_BASE}/${encodeURIComponent(model)}/runs`,
    options
  );
}

export async function fetchVars(model: string, run: string, options?: FetchOptions): Promise<VarRow[]> {
  const runKey = run || "latest";
  return fetchJson<VarRow[]>(
    `${API_V4_BASE}/${encodeURIComponent(model)}/${encodeURIComponent(runKey)}/vars`,
    options
  );
}

export async function fetchManifest(
  model: string,
  run: string,
  options?: FetchOptions
): Promise<RunManifestResponse> {
  const runKey = run || "latest";
  const payload = await fetchJson<unknown>(
    `${API_V4_BASE}/${encodeURIComponent(model)}/${encodeURIComponent(runKey)}/manifest`,
    options
  );
  if (!isRunManifestResponse(payload)) {
    throw new Error("Invalid manifest response shape");
  }
  return payload;
}

export async function fetchFrames(
  model: string,
  run: string,
  varKey: string,
  options?: FetchOptions
): Promise<FrameRow[]> {
  const runKey = run || "latest";
  const response = await fetchJson<FrameRow[]>(
    `${API_V4_BASE}/${encodeURIComponent(model)}/${encodeURIComponent(runKey)}/${encodeURIComponent(varKey)}/frames`,
    options
  );
  if (!Array.isArray(response)) {
    return [];
  }
  return response
    .filter((row) => row && row.has_cog && Number.isFinite(Number(row.fh)))
    .sort((a, b) => Number(a.fh) - Number(b.fh));
}

export async function fetchLoopManifest(
  model: string,
  run: string,
  varKey: string,
  options?: FetchOptions
): Promise<LoopManifestResponse | null> {
  const runKey = run || "latest";
  try {
    const response = await fetchJson<LoopManifestResponse>(
      `${API_V4_BASE}/${encodeURIComponent(model)}/${encodeURIComponent(runKey)}/${encodeURIComponent(varKey)}/loop-manifest`,
      options
    );
    if (!response || !Array.isArray(response.loop_tiers)) {
      return null;
    }
    return response;
  } catch {
    return null;
  }
}

// ── Sample (hover-for-data) ──────────────────────────────────────────

export type SampleResult = {
  value: number;
  units: string;
  model: string;
  run?: string;
  var: string;
  fh: number;
  valid_time: string;
  lat: number;
  lon: number;
  noData: boolean;
  label?: string;
  desc?: string;
};

export async function fetchSample(params: {
  model: string;
  run: string;
  var: string;
  fh: number;
  lat: number;
  lon: number;
  signal?: AbortSignal;
}): Promise<SampleResult | null> {
  const qs = new URLSearchParams({
    model: params.model,
    run: params.run,
    var: params.var,
    fh: String(params.fh),
    lat: String(params.lat),
    lon: String(params.lon),
  });
  const response = await fetch(`${API_V4_BASE}/sample?${qs}`, { credentials: "omit", signal: params.signal });
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`Sample request failed: ${response.status}`);
  }
  const payload = (await response.json()) as SampleResult;
  if (payload.noData || payload.value === null || Number.isNaN(Number(payload.value))) {
    return null;
  }
  return {
    ...payload,
    value: Number(payload.value),
  };
}

export function buildContourUrl(params: {
  model: string;
  run: string;
  varKey: string;
  fh: number;
  key: string;
}): string {
  const enc = encodeURIComponent;
  return `${API_V4_BASE}/${enc(params.model)}/${enc(params.run)}/${enc(params.varKey)}/${enc(params.fh)}/contours/${enc(params.key)}`;
}

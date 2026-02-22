import { API_BASE } from "@/lib/config";

export type ModelOption = {
  id: string;
  name: string;
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
  meta?: {
    meta?: LegendMeta | null;
  } | null;
};

export type VarRow =
  | string
  | {
      id: string;
      display_name?: string;
      name?: string;
      label?: string;
    };

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url, { credentials: "omit" });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<T>;
}

const REGIONS_CACHE_KEY = "twf_v3_regions_cache";
const REGIONS_ETAG_KEY = "twf_v3_regions_etag";

type RegionsResponse = {
  regions: Record<string, RegionPreset>;
};

export async function fetchRegionPresets(): Promise<Record<string, RegionPreset>> {
  const cachedRaw = localStorage.getItem(REGIONS_CACHE_KEY);
  const etag = localStorage.getItem(REGIONS_ETAG_KEY);
  const headers: Record<string, string> = {};
  if (etag) {
    headers["If-None-Match"] = etag;
  }

  const response = await fetch(API_BASE.replace(/\/api\/v3$/, "") + "/api/regions", {
    credentials: "omit",
    headers,
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

export async function fetchModels(): Promise<ModelOption[]> {
  return fetchJson<ModelOption[]>(`${API_BASE}/models`);
}

export async function fetchRegions(model: string): Promise<string[]> {
  void model;
  const regions = await fetchRegionPresets();
  return Object.keys(regions);
}

export async function fetchRuns(model: string): Promise<string[]> {
  return fetchJson<string[]>(
    `${API_BASE}/${encodeURIComponent(model)}/runs`
  );
}

export async function fetchVars(model: string, run: string): Promise<VarRow[]> {
  const runKey = run || "latest";
  return fetchJson<VarRow[]>(
    `${API_BASE}/${encodeURIComponent(model)}/${encodeURIComponent(runKey)}/vars`
  );
}

export async function fetchFrames(
  model: string,
  run: string,
  varKey: string
): Promise<FrameRow[]> {
  const runKey = run || "latest";
  const response = await fetchJson<FrameRow[]>(
    `${API_BASE}/${encodeURIComponent(model)}/${encodeURIComponent(runKey)}/${encodeURIComponent(varKey)}/frames`
  );
  if (!Array.isArray(response)) {
    return [];
  }
  return response
    .filter((row) => row && row.has_cog && Number.isFinite(Number(row.fh)))
    .sort((a, b) => Number(a.fh) - Number(b.fh));
}

// ── Sample (hover-for-data) ──────────────────────────────────────────

export type SampleResult = {
  value: number;
  units: string;
  model: string;
  var: string;
  fh: number;
  valid_time: string;
  lat: number;
  lon: number;
};

export async function fetchSample(params: {
  model: string;
  run: string;
  var: string;
  fh: number;
  lat: number;
  lon: number;
}): Promise<SampleResult | null> {
  const qs = new URLSearchParams({
    model: params.model,
    run: params.run,
    var: params.var,
    fh: String(params.fh),
    lat: String(params.lat),
    lon: String(params.lon),
  });
  const response = await fetch(`${API_BASE}/sample?${qs}`, { credentials: "omit" });
  if (response.status === 204 || response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`Sample request failed: ${response.status}`);
  }
  return response.json() as Promise<SampleResult>;
}

export function buildContourUrl(params: {
  model: string;
  run: string;
  varKey: string;
  fh: number;
  key: string;
}): string {
  const enc = encodeURIComponent;
  return `${API_BASE}/${enc(params.model)}/${enc(params.run)}/${enc(params.varKey)}/${enc(params.fh)}/contours/${enc(params.key)}`;
}

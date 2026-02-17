import { API_BASE } from "@/lib/config";

export type ModelOption = {
  id: string;
  name: string;
};

export type LegendStops = [number | string, string][];

export type LegendMeta = {
  kind?: string;
  display_name?: string;
  legend_title?: string;
  units?: string;
  legend_stops?: LegendStops;
  colors?: string[];
  levels?: number[];
  ptype_order?: string[];
  ptype_breaks?: Record<string, { offset: number; count: number }>;
  ptype_levels?: Record<string, number[]>;
  range?: [number, number];
  bins_per_ptype?: number;
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

export async function fetchModels(): Promise<ModelOption[]> {
  return fetchJson<ModelOption[]>(`${API_BASE}/models`);
}

export async function fetchRegions(model: string): Promise<string[]> {
  return fetchJson<string[]>(`${API_BASE}/${encodeURIComponent(model)}/regions`);
}

export async function fetchRuns(model: string, region: string): Promise<string[]> {
  return fetchJson<string[]>(
    `${API_BASE}/${encodeURIComponent(model)}/${encodeURIComponent(region)}/runs`
  );
}

export async function fetchVars(model: string, region: string, run: string): Promise<VarRow[]> {
  const runKey = run || "latest";
  return fetchJson<VarRow[]>(
    `${API_BASE}/${encodeURIComponent(model)}/${encodeURIComponent(region)}/${encodeURIComponent(runKey)}/vars`
  );
}

export async function fetchFrames(
  model: string,
  region: string,
  run: string,
  varKey: string
): Promise<FrameRow[]> {
  const runKey = run || "latest";
  const response = await fetchJson<FrameRow[]>(
    `${API_BASE}/${encodeURIComponent(model)}/${encodeURIComponent(region)}/${encodeURIComponent(runKey)}/${encodeURIComponent(varKey)}/frames`
  );
  if (!Array.isArray(response)) {
    return [];
  }
  return response
    .filter((row) => row && row.has_cog && Number.isFinite(Number(row.fh)))
    .sort((a, b) => Number(a.fh) - Number(b.fh));
}

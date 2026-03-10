export type PermalinkState = {
  model?: string;
  run?: string;
  var?: string;
  fh?: number;
  region?: string;
  lat?: number;
  lon?: number;
  z?: number;
  loop?: boolean;
};

function readStringParam(params: URLSearchParams, key: string): string | undefined {
  const raw = params.get(key);
  if (!raw) {
    return undefined;
  }
  const trimmed = raw.trim();
  return trimmed || undefined;
}

function readFiniteNumberParam(params: URLSearchParams, key: string): number | undefined {
  const raw = params.get(key);
  if (raw === null) {
    return undefined;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : undefined;
}

export function readPermalink(): PermalinkState {
  if (typeof window === "undefined") {
    return {};
  }

  const params = new URLSearchParams(window.location.search);
  const state: PermalinkState = {};

  const model = readStringParam(params, "m");
  if (model) {
    state.model = model;
  }

  const run = readStringParam(params, "r");
  if (run) {
    state.run = run;
  }

  const varKey = readStringParam(params, "v");
  if (varKey) {
    state.var = varKey;
  }

  const region = readStringParam(params, "reg");
  if (region) {
    state.region = region;
  }

  const fh = readFiniteNumberParam(params, "fh");
  if (Number.isFinite(fh) && Number(fh) >= 0) {
    state.fh = Number(fh);
  }

  const lat = readFiniteNumberParam(params, "lat");
  if (Number.isFinite(lat) && Number(lat) >= -90 && Number(lat) <= 90) {
    state.lat = Number(lat);
  }

  const lon = readFiniteNumberParam(params, "lon");
  if (Number.isFinite(lon) && Number(lon) >= -180 && Number(lon) <= 180) {
    state.lon = Number(lon);
  }

  const z = readFiniteNumberParam(params, "z");
  if (Number.isFinite(z) && Number(z) >= 0 && Number(z) <= 24) {
    state.z = Number(z);
  }

  const loop = params.get("loop");
  if (loop === "1") {
    state.loop = true;
  } else if (loop === "0") {
    state.loop = false;
  }

  return state;
}

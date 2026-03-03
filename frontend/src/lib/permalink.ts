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

export function buildPermalinkSearch(state: PermalinkState): string {
  const params = new URLSearchParams();

  if (state.model) {
    params.set("m", state.model);
  }
  if (state.run) {
    params.set("r", state.run);
  }
  if (state.var) {
    params.set("v", state.var);
  }
  if (Number.isFinite(state.fh) && Number(state.fh) >= 0) {
    params.set("fh", String(Math.round(Number(state.fh))));
  }
  if (state.region) {
    params.set("reg", state.region);
  }
  if (Number.isFinite(state.lat) && Number(state.lat) >= -90 && Number(state.lat) <= 90) {
    params.set("lat", Number(state.lat).toFixed(5));
  }
  if (Number.isFinite(state.lon) && Number(state.lon) >= -180 && Number(state.lon) <= 180) {
    params.set("lon", Number(state.lon).toFixed(5));
  }
  if (Number.isFinite(state.z) && Number(state.z) >= 0 && Number(state.z) <= 24) {
    params.set("z", Number(state.z).toFixed(2));
  }
  if (typeof state.loop === "boolean") {
    params.set("loop", state.loop ? "1" : "0");
  }

  const encoded = params.toString();
  return encoded ? `?${encoded}` : "";
}

export function replaceUrlQuery(search: string): void {
  if (typeof window === "undefined") {
    return;
  }
  const normalizedSearch = search || "";
  const { pathname, hash } = window.location;
  window.history.replaceState(null, "", `${pathname}${normalizedSearch}${hash}`);
}

import type { PermalinkState } from "@/lib/permalink-read";

export type { PermalinkState } from "@/lib/permalink-read";
export { readPermalink } from "@/lib/permalink-read";

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

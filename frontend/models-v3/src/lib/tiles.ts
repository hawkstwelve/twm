import { TILES_BASE } from "@/lib/config";
import type { FrameRow } from "@/lib/api";

function baseRoot() {
  // Strip any accidental trailing /api/v3 or /tiles/v3 (or v2) so we can append paths cleanly.
  return TILES_BASE.replace(/\/?(api\/v3|tiles\/v3|api\/v2|tiles\/v2)\/?$/i, "");
}

export function normalizeTemplatePath(template: string): string {
  // Ensure any template path uses /tiles/v3 (never v2).
  return template.replace(/\/tiles\/(?!v3\/)/, "/tiles/v3/");
}

export function toAbsoluteTileTemplate(template: string): string {
  const normalized = normalizeTemplatePath(template);
  if (normalized.startsWith("http://") || normalized.startsWith("https://")) {
    return normalized;
  }
  const root = baseRoot().replace(/\/$/, "");
  const path = normalized.startsWith("/") ? normalized : `/${normalized}`;
  return `${root}${path}`;
}

export function buildFallbackTileUrl(params: {
  model: string;
  region?: string;
  run: string;
  varKey: string;
  fh: number;
}): string {
  void params.region;
  const root = baseRoot().replace(/\/$/, "");
  const enc = encodeURIComponent;
  return `${root}/tiles/v3/${enc(params.model)}/${enc(params.run)}/${enc(params.varKey)}/${enc(params.fh)}/{z}/{x}/{y}.png`;
}

export function buildTileUrlFromFrame(params: {
  model: string;
  region?: string;
  run: string;
  varKey: string;
  fh: number;
  frameRow?: FrameRow | null;
}): string {
  if (params.frameRow?.tile_url_template) {
    return toAbsoluteTileTemplate(params.frameRow.tile_url_template);
  }
  return buildFallbackTileUrl(params);
}

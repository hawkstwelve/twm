function trimTrailingSlash(value: string): string {
  return value.replace(/\/$/, "");
}

const API_ORIGIN_ENV = String(import.meta.env.VITE_API_ORIGIN ?? "").trim();
const TILES_BASE_ENV = String(import.meta.env.VITE_TILES_BASE ?? API_ORIGIN_ENV).trim();

// Default to same-origin root paths so viewer works under nested routes like /viewer.
export const API_ORIGIN = API_ORIGIN_ENV ? trimTrailingSlash(API_ORIGIN_ENV) : "";
export const API_V4_BASE = `${API_ORIGIN}/api/v4`;
export const TILES_BASE = TILES_BASE_ENV ? trimTrailingSlash(TILES_BASE_ENV) : "";

export const WEBP_RENDER_MODE_THRESHOLDS = {
  tier0Max: 5.8,
  tier1Max: 6.6,
  hysteresis: 0.2,
  dwellMs: 200,
};

export const MAP_VIEW_DEFAULTS = {
  region: "conus",
  center: [39.83, -98.58] as [number, number],
  zoom: 4,
};

export const OVERLAY_DEFAULT_OPACITY = 0.9;

export type PlaybackBufferPolicy = {
  bufferTarget: number;
  minStartBuffer: number;
  minAheadWhilePlaying: number;
};

export function getPlaybackBufferPolicy(params: {
  totalFrames: number;
  autoplayTickMs: number;
}): PlaybackBufferPolicy {
  const totalFrames = Math.max(0, Number(params.totalFrames) || 0);
  const tickMs = Math.max(60, Number(params.autoplayTickMs) || 250);

  let bufferTarget = 12;
  if (totalFrames >= 85) {
    bufferTarget = 12;
  } else if (totalFrames >= 49) {
    bufferTarget = totalFrames >= 56 ? 16 : 14;
  } else if (totalFrames >= 30) {
    bufferTarget = 10;
  } else {
    bufferTarget = Math.max(6, Math.min(10, totalFrames));
  }

  const minStartBuffer = totalFrames >= 49 ? 3 : 2;

  let minAheadWhilePlaying = 5;
  if (tickMs <= 180) {
    minAheadWhilePlaying = 7;
  } else if (tickMs <= 250) {
    minAheadWhilePlaying = 6;
  } else if (tickMs >= 350) {
    minAheadWhilePlaying = 4;
  }

  return {
    bufferTarget: Math.max(minStartBuffer, Math.min(bufferTarget, totalFrames || bufferTarget)),
    minStartBuffer,
    minAheadWhilePlaying,
  };
}

export function isWebpDefaultRenderEnabled(): boolean {
  const envValue = String(import.meta.env.VITE_TWF_V3_WEBP_DEFAULT_ENABLED ?? "").trim().toLowerCase();
  if (envValue === "1" || envValue === "true" || envValue === "yes" || envValue === "on") {
    return true;
  }
  if (envValue === "0" || envValue === "false" || envValue === "no" || envValue === "off") {
    return false;
  }
  return true;
}

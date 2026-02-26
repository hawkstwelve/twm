export const API_BASE = "https://api.theweathermodels.com/api/v3";
export const API_V4_BASE = API_BASE.replace(/\/api\/v3$/i, "/api/v4");

export const TILES_BASE = "https://api.theweathermodels.com";

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

export function isAnimationDebugEnabled(): boolean {
  if (typeof window === "undefined") {
    return false;
  }
  return window.localStorage.getItem("twf_debug_animation") === "1";
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

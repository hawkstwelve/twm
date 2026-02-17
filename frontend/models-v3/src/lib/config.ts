const isLocalDevHost =
  window.location.hostname === "127.0.0.1" ||
  window.location.hostname === "localhost";

const isLocalDevPort =
  window.location.port === "5173" ||
  window.location.port === "4173" ||
  window.location.port === "8080";

export const API_BASE =
  isLocalDevHost && isLocalDevPort
    ? "http://127.0.0.1:8099/api/v2"
    : "https://api.sodakweather.com/api/v2";

export const TILES_BASE =
  isLocalDevHost && isLocalDevPort
    ? "http://127.0.0.1:8101"
    : "https://api.sodakweather.com";

export const DEFAULTS = {
  model: "hrrr",
  region: "pnw",
  run: "latest",
  variable: "tmp2m",
  center: [47.6, -122.3] as [number, number],
  zoom: 6,
  overlayOpacity: 0.85,
};

export const ALLOWED_VARIABLES = new Set(["tmp2m", "wspd10m", "radar_ptype", "precip_ptype", "qpf6h"]);

export const VARIABLE_LABELS: Record<string, string> = {
  tmp2m: "Surface Temperature",
  wspd10m: "Wind Speed",
  radar_ptype: "Composite Reflectivity + P-Type",
  precip_ptype: "Precip + Type",
  qpf6h: "6-hr Precip",
};

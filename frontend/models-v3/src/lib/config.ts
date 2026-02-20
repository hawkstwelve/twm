export const API_BASE = "https://api.sodakweather.com/api/v3";

export const TILES_BASE = "https://api.sodakweather.com";

export const DEFAULTS = {
  model: "hrrr",
  region: "pnw",
  run: "latest",
  variable: "tmp2m",
  center: [47.6, -122.3] as [number, number],
  zoom: 6,
  overlayOpacity: 0.9,
};

export const ALLOWED_VARIABLES = new Set(["tmp2m", "wspd10m", "radar_ptype", "precip_ptype", "qpf6h"]);

export const VARIABLE_LABELS: Record<string, string> = {
  tmp2m: "Surface Temperature",
  wspd10m: "Wind Speed",
  radar_ptype: "Composite Reflectivity + P-Type",
  precip_ptype: "Precip + Type",
  qpf6h: "6-hr Precip",
};

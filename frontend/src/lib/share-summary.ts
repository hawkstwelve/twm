type BuildShareSummaryInput = {
  modelId: string;
  runId: string;
  variableId: string;
  variableDisplayName?: string | null;
  regionId: string;
  regionLabel?: string | null;
  forecastHour: number | null;
  centerLat: number | null;
  centerLon: number | null;
  zoom: number | null;
  loopEnabled: boolean;
};

type ShareSummary = {
  shortSummary: string;
  detailsSummary: string;
};

const RUN_ID_RE = /^(\d{4})(\d{2})(\d{2})_(\d{2})z$/i;
const MONTHS_SHORT = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

const MODEL_LABELS: Record<string, string> = {
  hrrr: "HRRR",
  gfs: "GFS",
  nam: "NAM",
  nbm: "NBM",
  rap: "RAP",
  gefs: "GEFS",
  ecmwf: "ECMWF",
};

const VARIABLE_SPECIAL_CASES: Record<string, string> = {
  radar_ptype: "Radar & precip type",
  precip_ptype: "Precip type",
  qpf: "QPF",
  tmp2m: "2m temperature",
};

function titleCaseWords(value: string): string {
  return value
    .split(" ")
    .filter(Boolean)
    .map((part) => {
      if (part.length <= 3 && part === part.toUpperCase()) {
        return part;
      }
      return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
    })
    .join(" ");
}

function modelLabel(modelId: string): string {
  const key = modelId.trim().toLowerCase();
  if (!key) {
    return "Model";
  }
  return MODEL_LABELS[key] ?? key.toUpperCase();
}

function runLabel(runId: string): string {
  const trimmed = runId.trim();
  const match = trimmed.match(RUN_ID_RE);
  if (!match) {
    return trimmed || "Latest";
  }
  const [, yearRaw, monthRaw, dayRaw, hourRaw] = match;
  const year = Number(yearRaw);
  const month = Number(monthRaw);
  const day = Number(dayRaw);
  const hour = Number(hourRaw);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day) || !Number.isFinite(hour)) {
    return trimmed;
  }
  const monthLabel = MONTHS_SHORT[Math.max(0, Math.min(11, month - 1))] ?? monthRaw;
  return `${monthLabel} ${day}, ${year} ${hourRaw}Z`;
}

function variableLabel(variableId: string, preferred?: string | null): string {
  const preferredLabel = typeof preferred === "string" ? preferred.trim() : "";
  if (preferredLabel) {
    return preferredLabel;
  }
  const normalized = variableId.trim().toLowerCase();
  if (!normalized) {
    return "Variable";
  }
  if (VARIABLE_SPECIAL_CASES[normalized]) {
    return VARIABLE_SPECIAL_CASES[normalized];
  }
  const words = normalized.replace(/[_-]+/g, " ");
  return titleCaseWords(words);
}

function regionDisplayLabel(regionId: string, regionLabel?: string | null): string {
  const preferred = typeof regionLabel === "string" ? regionLabel.trim() : "";
  if (preferred) {
    return preferred;
  }
  const normalized = regionId.trim();
  if (!normalized) {
    return "Region";
  }
  return normalized.toUpperCase();
}

function formatCenter(lat: number | null, lon: number | null): string {
  const latValue = Number.isFinite(lat) ? (lat as number).toFixed(2) : "n/a";
  const lonValue = Number.isFinite(lon) ? (lon as number).toFixed(2) : "n/a";
  return `Center ${latValue}, ${lonValue}`;
}

function formatZoom(zoom: number | null): string {
  if (!Number.isFinite(zoom)) {
    return "Zoom n/a";
  }
  return `Zoom ${(zoom as number).toFixed(2)}`;
}

function formatForecastHour(forecastHour: number | null): string {
  if (!Number.isFinite(forecastHour)) {
    return "Forecast hour n/a";
  }
  return `Forecast hour ${Math.round(forecastHour as number)}`;
}

export function buildShareSummary(input: BuildShareSummaryInput): ShareSummary {
  const shortSummary = [
    modelLabel(input.modelId),
    runLabel(input.runId),
    formatForecastHour(input.forecastHour),
    variableLabel(input.variableId, input.variableDisplayName),
    regionDisplayLabel(input.regionId, input.regionLabel),
  ].join(" • ");

  const detailsSummary = [
    formatCenter(input.centerLat, input.centerLon),
    formatZoom(input.zoom),
    `Loop ${input.loopEnabled ? "on" : "off"}`,
  ].join(" • ");

  return { shortSummary, detailsSummary };
}


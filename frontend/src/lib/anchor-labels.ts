import type { Feature, FeatureCollection, Point } from "geojson";

export type AnchorFeatureProperties = {
  st?: string;
  state?: string;
  n_for_state?: number;
  area_km2?: number;
  label?: string;
  active?: boolean;
  value?: number | null;
  units?: string;
};

export type AnchorFeature = Feature<Point, AnchorFeatureProperties> & {
  id: string;
};

export type AnchorFeatureCollection = FeatureCollection<Point, AnchorFeatureProperties>;

export type AnchorBatchPoint = {
  id: string;
  lat: number;
  lon: number;
};

export type AnchorBatchResponse = {
  units: string;
  values: Record<string, number | null>;
};

export type AnchorDisplayMode = "always" | "active-only" | "hidden";

export type AnchorDisplayRule = {
  mode: AnchorDisplayMode;
  threshold?: number;
};

const DEFAULT_ANCHOR_DISPLAY_RULE: AnchorDisplayRule = Object.freeze({ mode: "always" });

export const ANCHOR_DISPLAY_RULES: Readonly<Record<string, AnchorDisplayRule>> = Object.freeze({
  tmp2m: { mode: "always" },
  dpt2m: { mode: "always" },
  dewpoint2m: { mode: "always" },
  dewpoint: { mode: "always" },
  wspd10m: { mode: "always" },
  wgst10m: { mode: "always" },
  precip_total: { mode: "active-only", threshold: 0.01 },
  snowfall_total: { mode: "active-only", threshold: 0.1 },
  snowfall_kuchera_total: { mode: "active-only", threshold: 0.1 },
  refc: { mode: "active-only", threshold: 15 },
  cref: { mode: "active-only", threshold: 15 },
  reflectivity: { mode: "active-only", threshold: 15 },
  radar_reflectivity: { mode: "active-only", threshold: 15 },
  radar_ptype: { mode: "hidden" },
});

function isObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

export function normalizeAnchorVariableKey(varKey: string): string {
  return varKey.trim().toLowerCase();
}

export function resolveAnchorDisplayRule(varKey: string): AnchorDisplayRule {
  const normalized = normalizeAnchorVariableKey(varKey);
  return ANCHOR_DISPLAY_RULES[normalized] ?? DEFAULT_ANCHOR_DISPLAY_RULE;
}

export function formatAnchorValueLabel(value: number): string {
  const rounded = Math.round(value * 10) / 10;
  return Number.isInteger(rounded) ? String(Math.round(rounded)) : rounded.toFixed(1);
}

export function anchorBatchPointsFromGeoJson(
  collection: AnchorFeatureCollection | null | undefined
): AnchorBatchPoint[] {
  if (!collection || !Array.isArray(collection.features)) {
    return [];
  }

  const points: AnchorBatchPoint[] = [];
  for (const feature of collection.features) {
    const featureId = typeof feature.id === "string" ? feature.id : null;
    const coordinates = feature.geometry?.type === "Point" ? feature.geometry.coordinates : null;
    const lon = Number(coordinates?.[0]);
    const lat = Number(coordinates?.[1]);
    if (!featureId || !Number.isFinite(lat) || !Number.isFinite(lon)) {
      continue;
    }
    points.push({ id: featureId, lat, lon });
  }
  return points;
}

export function buildAnchorDisplayGeoJson(params: {
  baseCollection: AnchorFeatureCollection;
  varKey: string;
  values: Record<string, number | null | undefined>;
  units?: string | null;
}): AnchorFeatureCollection {
  const rule = resolveAnchorDisplayRule(params.varKey);
  const units = typeof params.units === "string" ? params.units : "";

  return {
    type: "FeatureCollection",
    features: params.baseCollection.features.map((feature) => {
      const rawValue = params.values[String(feature.id)];
      const numericValue = Number(rawValue);
      const hasValue = Number.isFinite(numericValue);
      const isActive =
        rule.mode !== "hidden"
        && hasValue
        && (rule.mode !== "active-only" || numericValue > Number(rule.threshold ?? 0));

      return {
        ...feature,
        properties: {
          ...(feature.properties ?? {}),
          label: isActive ? formatAnchorValueLabel(numericValue) : "",
          active: isActive,
          value: isActive ? numericValue : null,
          units,
        },
      };
    }),
  };
}

export function buildInactiveAnchorFeatureCollection(
  baseCollection: AnchorFeatureCollection,
  units = ""
): AnchorFeatureCollection {
  return {
    type: "FeatureCollection",
    features: baseCollection.features.map((feature) => ({
      ...feature,
      properties: {
        ...(feature.properties ?? {}),
        label: "",
        active: false,
        value: null,
        units,
      },
    })),
  };
}

export function isAnchorFeatureCollection(value: unknown): value is AnchorFeatureCollection {
  if (!isObject(value) || value.type !== "FeatureCollection" || !Array.isArray(value.features)) {
    return false;
  }

  return value.features.every((feature) => {
    if (!isObject(feature) || feature.type !== "Feature") {
      return false;
    }
    if (typeof feature.id !== "string" || !feature.id.trim()) {
      return false;
    }
    if (!isObject(feature.geometry) || feature.geometry.type !== "Point") {
      return false;
    }
    const coordinates = feature.geometry.coordinates;
    return Array.isArray(coordinates)
      && coordinates.length >= 2
      && Number.isFinite(Number(coordinates[0]))
      && Number.isFinite(Number(coordinates[1]));
  });
}
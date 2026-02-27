import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import maplibregl, { type StyleSpecification } from "maplibre-gl";
import type { GeoJSON } from "geojson";

import { MAP_VIEW_DEFAULTS, TILES_BASE } from "@/lib/config";

const IS_HIDPI = typeof window !== "undefined" && window.devicePixelRatio > 1;
const CARTO_TILE_SUFFIX = IS_HIDPI ? "@2x" : "";
const CARTO_TILE_SIZE = IS_HIDPI ? 512 : 256;

const CARTO_LIGHT_BASE_TILES = [
  `https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
  `https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
  `https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
  `https://d.basemaps.cartocdn.com/light_all/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
];

const CARTO_LIGHT_LABEL_TILES = [
  `https://a.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
  `https://b.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
  `https://c.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
  `https://d.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
];

const CARTO_DARK_BASE_TILES = [
  `https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
  `https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
  `https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
  `https://d.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
];

const CARTO_DARK_LABEL_TILES = [
  `https://a.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
  `https://b.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
  `https://c.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
  `https://d.basemaps.cartocdn.com/dark_only_labels/{z}/{x}/{y}${CARTO_TILE_SUFFIX}.png`,
];

const BOUNDARIES_VECTOR_TILES_URL = `${TILES_BASE}/tiles/v3/boundaries/v1/tilejson.json`;

type RegionView = {
  center: [number, number];
  zoom: number;
  bbox?: [number, number, number, number];
  minZoom?: number;
  maxZoom?: number;
};

export type BasemapMode = "light" | "dark";

const SCRUB_SWAP_TIMEOUT_MS = 650;
const AUTOPLAY_SWAP_TIMEOUT_MS = 1500;
const SETTLE_TIMEOUT_MS = 1200;
const CONTINUOUS_CROSSFADE_MS = 120;
const MICRO_CROSSFADE_MS = 140;
const PREFETCH_BUFFER_COUNT = 8;
const OVERLAY_RASTER_CONTRAST = 0.08;
const OVERLAY_RASTER_SATURATION = 0.08;
const OVERLAY_RASTER_BRIGHTNESS_MIN = 0.02;
const OVERLAY_RASTER_BRIGHTNESS_MAX = 0.98;
const OVERLAY_RASTER_DARK_CONTRAST = 0.14;
const OVERLAY_RASTER_DARK_SATURATION = 0.14;
const OVERLAY_RASTER_DARK_BRIGHTNESS_MIN = 0.06;
const OVERLAY_RASTER_DARK_BRIGHTNESS_MAX = 1;
const OVERLAY_RASTER_DARK_GRAY_BOOST_CONTRAST = 0.2;
const OVERLAY_RASTER_DARK_GRAY_BOOST_SATURATION = 0.16;
const OVERLAY_RASTER_DARK_GRAY_BOOST_BRIGHTNESS_MIN = 0.1;
const OVERLAY_RASTER_DARK_GRAY_BOOST_BRIGHTNESS_MAX = 1;

// Keep inactive swap buffer warm at tiny opacity to avoid one-frame basemap flash.
const HIDDEN_SWAP_BUFFER_OPACITY = 0.001;
// Keep prefetch layers fully hidden by default to reduce overdraw/compositing cost.
// Prefetch layers are only warmed while an active prefetch URL is being requested.
const HIDDEN_PREFETCH_OPACITY = 0;
const WARM_PREFETCH_OPACITY = 0.001;
const PREFETCH_TILE_EVENT_BUDGET = 1;
const PREFETCH_READY_TIMEOUT_MS = 8000;
const WEBP_TO_TILE_STABLE_MS = 150;
const WEBP_TO_TILE_CROSSFADE_MS = 200;
const CONTOUR_SOURCE_ID = "twf-contours";
const CONTOUR_LAYER_ID = "twf-contours";
const STATE_BOUNDARY_SOURCE_ID = "twf-boundaries";
const COASTLINE_LAYER_ID = "twf-coastline";
const STATE_BOUNDARY_LAYER_ID = "twf-state-boundaries";
const COUNTRY_BOUNDARY_LAYER_ID = "twf-country-boundaries";
const COUNTY_BOUNDARY_LAYER_ID = "twf-county-boundaries";
const LAKE_MASK_LAYER_ID = "twf-lake-mask";
const LAKE_SHORELINE_LAYER_ID = "twf-lake-shoreline";
const LOOP_SOURCE_ID = "twf-loop-image";
const LOOP_LAYER_ID = "twf-loop-image";
const EMPTY_FEATURE_COLLECTION: GeoJSON.FeatureCollection = {
  type: "FeatureCollection",
  features: [],
};

const TRANSPARENT_PIXEL_DATA_URL =
  "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMB/ax7n7kAAAAASUVORK5CYII=";

const LOOP_CONUS_COORDINATES: [[number, number], [number, number], [number, number], [number, number]] = [
  [-125.0, 50.0],
  [-66.5, 50.0],
  [-66.5, 24.0],
  [-125.0, 24.0],
];

type OverlayBuffer = "a" | "b";
type PlaybackMode = "autoplay" | "scrub";
const GRAY_LOW_END_VARIABLES = new Set(["precip_total", "snowfall_total", "qpf6h", "wspd10m", "wgst10m"]);

function sourceId(buffer: OverlayBuffer): string {
  return `twf-overlay-${buffer}`;
}

function layerId(buffer: OverlayBuffer): string {
  return `twf-overlay-${buffer}`;
}

function otherBuffer(buffer: OverlayBuffer): OverlayBuffer {
  return buffer === "a" ? "b" : "a";
}

function prefetchSourceId(index: number): string {
  return `twf-prefetch-${index}`;
}

function prefetchLayerId(index: number): string {
  return `twf-prefetch-${index}`;
}

function getResamplingMode(variableKind?: string | null): "nearest" | "linear" {
  const normalizedKind = String(variableKind ?? "").trim().toLowerCase();
  if (normalizedKind === "discrete" || normalizedKind === "indexed" || normalizedKind === "categorical") {
    return "nearest";
  }
  return "linear";
}

function getOverlayPaintSettingsForDark(variable?: string): {
  contrast: number;
  saturation: number;
  brightnessMin: number;
  brightnessMax: number;
} {
  if (variable && GRAY_LOW_END_VARIABLES.has(variable)) {
    return {
      contrast: OVERLAY_RASTER_DARK_GRAY_BOOST_CONTRAST,
      saturation: OVERLAY_RASTER_DARK_GRAY_BOOST_SATURATION,
      brightnessMin: OVERLAY_RASTER_DARK_GRAY_BOOST_BRIGHTNESS_MIN,
      brightnessMax: OVERLAY_RASTER_DARK_GRAY_BOOST_BRIGHTNESS_MAX,
    };
  }

  return {
    contrast: OVERLAY_RASTER_DARK_CONTRAST,
    saturation: OVERLAY_RASTER_DARK_SATURATION,
    brightnessMin: OVERLAY_RASTER_DARK_BRIGHTNESS_MIN,
    brightnessMax: OVERLAY_RASTER_DARK_BRIGHTNESS_MAX,
  };
}

function getOverlayPaintSettings(variable?: string, basemapMode: BasemapMode = "light"): {
  contrast: number;
  saturation: number;
  brightnessMin: number;
  brightnessMax: number;
} {
  if (basemapMode === "dark") {
    return getOverlayPaintSettingsForDark(variable);
  }

  if (variable === "wspd10m" || variable === "wgst10m") {
    return {
      contrast: 0,
      saturation: 0,
      brightnessMin: 0,
      brightnessMax: 1,
    };
  }
  return {
    contrast: OVERLAY_RASTER_CONTRAST,
    saturation: OVERLAY_RASTER_SATURATION,
    brightnessMin: OVERLAY_RASTER_BRIGHTNESS_MIN,
    brightnessMax: OVERLAY_RASTER_BRIGHTNESS_MAX,
  };
}

function getBoundaryLineColor(basemapMode: BasemapMode): string {
  return basemapMode === "dark" ? "#f3f4f6" : "#000000";
}

function getLakeFillColor(basemapMode: BasemapMode): string {
  return basemapMode === "dark" ? "#2C353C" : "#d4dadc";
}

function getBasemapPaintSettings(basemapMode: BasemapMode): {
  "raster-brightness-min": number;
  "raster-brightness-max": number;
  "raster-contrast": number;
  "raster-saturation": number;
} {
  if (basemapMode === "dark") {
    return {
      "raster-brightness-min": 0.08,
      "raster-brightness-max": 0.94,
      "raster-contrast": -0.06,
      "raster-saturation": -0.08,
    };
  }

  return {
    "raster-brightness-min": 0,
    "raster-brightness-max": 1,
    "raster-contrast": 0,
    "raster-saturation": 0,
  };
}

function getMapBackgroundColor(basemapMode: BasemapMode): string {
  return basemapMode === "dark" ? "#1f2a33" : "#e8edf1";
}

function getLabelPaintSettings(basemapMode: BasemapMode): {
  "raster-resampling": "nearest" | "linear";
  "raster-opacity": number;
  "raster-contrast": number;
  "raster-saturation": number;
  "raster-brightness-min": number;
  "raster-brightness-max": number;
} {
  if (basemapMode === "dark") {
    return {
      // Use linear filtering to avoid blocky/pixelated labels on zoom.
      "raster-resampling": "linear",
      "raster-opacity": 1,
      "raster-contrast": 0.1,
      "raster-saturation": -0.06,
      "raster-brightness-min": 0.05,
      "raster-brightness-max": 1,
    };
  }
  return {
    // Use linear filtering to avoid blocky/pixelated labels on zoom.
    "raster-resampling": "linear",
    "raster-opacity": 1,
    "raster-contrast": 0.08,
    "raster-saturation": -0.06,
    "raster-brightness-min": 0,
    "raster-brightness-max": 1,
  };
}

function setLayerVisibility(map: maplibregl.Map, id: string, visible: boolean) {
  if (!map.getLayer(id)) {
    return;
  }
  map.setLayoutProperty(id, "visibility", visible ? "visible" : "none");
}

function styleFor(
  overlayUrl: string,
  opacity: number,
  variable?: string,
  variableKind?: string | null,
  overlayFadeOutZoom?: { start: number; end: number } | null,
  contourGeoJsonUrl?: string | null,
  basemapMode: BasemapMode = "light"
): StyleSpecification {
  const resamplingMode = getResamplingMode(variableKind);
  const paintSettings = getOverlayPaintSettings(variable, basemapMode);
  const basemapTiles = basemapMode === "dark" ? CARTO_DARK_BASE_TILES : CARTO_LIGHT_BASE_TILES;
  const labelTiles = basemapMode === "dark" ? CARTO_DARK_LABEL_TILES : CARTO_LIGHT_LABEL_TILES;
  const mapBackgroundColor = getMapBackgroundColor(basemapMode);
  const boundaryLineColor = getBoundaryLineColor(basemapMode);
  const lakeFillColor = getLakeFillColor(basemapMode);
  const basemapPaint = getBasemapPaintSettings(basemapMode);
  const labelPaint = getLabelPaintSettings(basemapMode);
  const overlayOpacity: any = overlayFadeOutZoom
    ? [
      "interpolate",
      ["linear"],
      ["zoom"],
      overlayFadeOutZoom.start,
      opacity,
      overlayFadeOutZoom.end,
      0,
    ]
    : opacity;
  const overlayPaint: any = {
    "raster-opacity": overlayOpacity,
    "raster-resampling": resamplingMode,
    "raster-fade-duration": 0,
    "raster-contrast": paintSettings.contrast,
    "raster-saturation": paintSettings.saturation,
    "raster-brightness-min": paintSettings.brightnessMin,
    "raster-brightness-max": paintSettings.brightnessMax,
  };
  const prefetchSources = Object.fromEntries(
    Array.from({ length: PREFETCH_BUFFER_COUNT }, (_, index) => [
      prefetchSourceId(index + 1),
      {
        type: "raster",
        tiles: [overlayUrl],
        tileSize: 512,
      },
    ])
  );
  const prefetchLayers = Array.from({ length: PREFETCH_BUFFER_COUNT }, (_, index) => ({
    id: prefetchLayerId(index + 1),
    type: "raster" as const,
    source: prefetchSourceId(index + 1),
    layout: { visibility: "none" as const },
    paint: overlayPaint,
  }));

  return {
    version: 8,
    sources: {
      "twf-basemap": {
        type: "raster",
        tiles: basemapTiles,
        tileSize: CARTO_TILE_SIZE,
      },
      [sourceId("a")]: {
        type: "raster",
        tiles: [overlayUrl],
        tileSize: 512,
      },
      [sourceId("b")]: {
        type: "raster",
        tiles: [overlayUrl],
        tileSize: 512,
      },
      ...prefetchSources,
      "twf-labels": {
        type: "raster",
        tiles: labelTiles,
        tileSize: CARTO_TILE_SIZE,
      },
      [STATE_BOUNDARY_SOURCE_ID]: {
        type: "vector",
        url: BOUNDARIES_VECTOR_TILES_URL,
      },
      [CONTOUR_SOURCE_ID]: {
        type: "geojson",
        data: contourGeoJsonUrl ?? EMPTY_FEATURE_COLLECTION,
      },
      [LOOP_SOURCE_ID]: {
        type: "image",
        url: TRANSPARENT_PIXEL_DATA_URL,
        coordinates: LOOP_CONUS_COORDINATES,
      },
    },
    layers: [
      {
        id: "twf-background",
        type: "background",
        paint: {
          "background-color": mapBackgroundColor,
        },
      },
      {
        id: "twf-basemap",
        type: "raster",
        source: "twf-basemap",
        paint: basemapPaint,
      },
      {
        id: layerId("a"),
        type: "raster",
        source: sourceId("a"),
        paint: overlayPaint,
      },
      {
        id: layerId("b"),
        type: "raster",
        source: sourceId("b"),
        paint: overlayPaint,
      },
      ...prefetchLayers,
      {
        id: COASTLINE_LAYER_ID,
        type: "line",
        source: STATE_BOUNDARY_SOURCE_ID,
        "source-layer": "hydro",
        filter: ["==", "kind", "coastline"],
        layout: {
          "line-join": "round",
          "line-cap": "round",
        },
        paint: {
          "line-color": boundaryLineColor,
          "line-opacity": 0.86,
          "line-width": ["interpolate", ["linear"], ["zoom"], 4, 0.95, 7, 1.3, 10, 1.7],
          "line-blur": ["interpolate", ["linear"], ["zoom"], 3, 0.18, 6, 0.1, 10, 0.04],
        },
      },
      {
        id: COUNTRY_BOUNDARY_LAYER_ID,
        type: "line",
        source: STATE_BOUNDARY_SOURCE_ID,
        "source-layer": "boundaries",
        filter: ["==", "kind", "country"],
        layout: {
          "line-join": "round",
          "line-cap": "round",
        },
        paint: {
          "line-color": boundaryLineColor,
          "line-opacity": 0.78,
          "line-width": ["interpolate", ["linear"], ["zoom"], 4, 0.9, 7, 1.2, 10, 1.55],
          "line-blur": ["interpolate", ["linear"], ["zoom"], 3, 0.16, 6, 0.08, 10, 0.03],
        },
      },
      {
        id: STATE_BOUNDARY_LAYER_ID,
        type: "line",
        source: STATE_BOUNDARY_SOURCE_ID,
        "source-layer": "boundaries",
        filter: ["==", "kind", "state"],
        layout: {
          "line-join": "round",
          "line-cap": "round",
        },
        paint: {
          "line-color": boundaryLineColor,
          "line-opacity": 0.92,
          "line-width": ["interpolate", ["linear"], ["zoom"], 4, 1.1, 7, 1.5, 10, 1.9],
          "line-blur": ["interpolate", ["linear"], ["zoom"], 3, 0.14, 6, 0.08, 10, 0.03],
        },
      },
      {
        id: COUNTY_BOUNDARY_LAYER_ID,
        type: "line",
        source: STATE_BOUNDARY_SOURCE_ID,
        "source-layer": "counties",
        minzoom: 5,
        maxzoom: 10,
        filter: ["==", "kind", "county"],
        layout: {
          "line-join": "round",
          "line-cap": "round",
        },
        paint: {
          "line-color": boundaryLineColor,
          "line-opacity": ["interpolate", ["linear"], ["zoom"], 5, 0.68, 6, 0.66, 7, 0.64, 8, 0.62, 10, 0.58],
          "line-width": ["interpolate", ["linear"], ["zoom"], 5, 0.58, 6, 0.72, 8, 0.88, 10, 1],
          "line-blur": ["interpolate", ["linear"], ["zoom"], 5, 0.12, 7, 0.08, 10, 0.02],
        },
      },
      {
        id: LAKE_MASK_LAYER_ID,
        type: "fill",
        source: STATE_BOUNDARY_SOURCE_ID,
        "source-layer": "hydro",
        filter: ["==", "kind", "great_lake_polygon"],
        paint: {
          "fill-color": lakeFillColor,
          "fill-opacity": 1,
        },
      },
      {
        id: LAKE_SHORELINE_LAYER_ID,
        type: "line",
        source: STATE_BOUNDARY_SOURCE_ID,
        "source-layer": "hydro",
        minzoom: 3,
        filter: ["==", "kind", "great_lake_shoreline"],
        layout: {
          "line-join": "round",
          "line-cap": "round",
        },
        paint: {
          "line-color": boundaryLineColor,
          "line-opacity": ["interpolate", ["linear"], ["zoom"], 3, 0.45, 4, 0.62, 5, 0.75, 7, 0.9, 10, 0.9],
          "line-width": ["interpolate", ["linear"], ["zoom"], 3, 0.5, 4, 0.75, 5, 1.05, 7, 1.4, 10, 1.8],
        },
      },
      {
        id: CONTOUR_LAYER_ID,
        type: "line",
        source: CONTOUR_SOURCE_ID,
        layout: {
          "line-join": "round",
          "line-cap": "round",
        },
        paint: {
          "line-color": boundaryLineColor,
          "line-opacity": 0.9,
          "line-width": ["interpolate", ["linear"], ["zoom"], 4, 1, 8, 2, 12, 3],
        },
      },
      {
        id: LOOP_LAYER_ID,
        type: "raster",
        source: LOOP_SOURCE_ID,
        layout: {
          visibility: "none",
        },
        paint: {
          "raster-opacity": opacity,
          "raster-resampling": resamplingMode,
          "raster-fade-duration": 0,
          "raster-contrast": paintSettings.contrast,
          "raster-saturation": paintSettings.saturation,
          "raster-brightness-min": paintSettings.brightnessMin,
          "raster-brightness-max": paintSettings.brightnessMax,
        },
      },
      {
        id: "twf-labels",
        type: "raster",
        source: "twf-labels",
        paint: labelPaint,
      },
    ],
  };
}

type MapCanvasProps = {
  tileUrl: string;
  contourGeoJsonUrl?: string | null;
  region: string;
  regionViews?: Record<string, RegionView>;
  opacity: number;
  mode: PlaybackMode;
  variable?: string;
  variableKind?: string | null;
  overlayFadeOutZoom?: { start: number; end: number } | null;
  zoomHintMinZoom?: number | null;
  basemapMode: BasemapMode;
  prefetchTileUrls?: string[];
  crossfade?: boolean;
  loopImageUrl?: string | null;
  loopActive?: boolean;
  onFrameSettled?: (tileUrl: string) => void;
  onTileReady?: (tileUrl: string) => void;
  onTileViewportReady?: (tileUrl: string) => void;
  onFrameLoadingChange?: (tileUrl: string, isLoading: boolean) => void;
  onZoomHint?: (show: boolean) => void;
  onZoomBucketChange?: (bucket: number) => void;
  onZoomRoutingSignal?: (payload: { zoom: number; gestureActive: boolean }) => void;
  onMapHover?: (lat: number, lon: number, x: number, y: number) => void;
  onMapHoverEnd?: () => void;
};

export function MapCanvas({
  tileUrl,
  contourGeoJsonUrl,
  region,
  regionViews,
  opacity,
  mode,
  variable,
  variableKind,
  overlayFadeOutZoom = null,
  zoomHintMinZoom = null,
  basemapMode,
  prefetchTileUrls = [],
  crossfade = false,
  loopImageUrl,
  loopActive = false,
  onFrameSettled,
  onTileReady,
  onTileViewportReady,
  onFrameLoadingChange,
  onZoomHint,
  onZoomBucketChange,
  onZoomRoutingSignal,
  onMapHover,
  onMapHoverEnd,
}: MapCanvasProps) {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [isLoaded, setIsLoaded] = useState(false);
  const activeBufferRef = useRef<OverlayBuffer>("a");
  const activeTileUrlRef = useRef(tileUrl);
  const swapTokenRef = useRef(0);
  const prefetchTokenRef = useRef(0);
  const prefetchUrlsRef = useRef<string[]>(Array.from({ length: PREFETCH_BUFFER_COUNT }, () => ""));
  const sourceRequestedUrlRef = useRef<Map<string, string>>(new Map());
  const sourceRequestTokenRef = useRef<Map<string, number>>(new Map());
  const sourceEventCountRef = useRef<Map<string, number>>(new Map());
  const fadeTokenRef = useRef(0);
  const fadeRafRef = useRef<number | null>(null);
  const tileViewportReadyTokenRef = useRef(0);
  const basemapStyleSwapTokenRef = useRef(0);
  const lastAppliedBasemapModeRef = useRef<BasemapMode>(basemapMode);
  const loopToTileRafRef = useRef<number | null>(null);
  const loopToTileStableTimerRef = useRef<number | null>(null);
  const loopToTileIdleCleanupRef = useRef<(() => void) | null>(null);
  const loopToTileTokenRef = useRef(0);
  const previousLoopActiveRef = useRef(loopActive);
  const isLoopToTileTransitioningRef = useRef(false);

  const view = useMemo(() => {
    return regionViews?.[region] ?? {
      center: [MAP_VIEW_DEFAULTS.center[1], MAP_VIEW_DEFAULTS.center[0]] as [number, number],
      zoom: MAP_VIEW_DEFAULTS.zoom,
    };
  }, [region, regionViews]);

  const initializeSourceTracking = useCallback((currentTileUrl: string) => {
    const sourceA = sourceId("a");
    const sourceB = sourceId("b");
    sourceRequestedUrlRef.current.set(sourceA, currentTileUrl);
    sourceRequestedUrlRef.current.set(sourceB, currentTileUrl);
    sourceRequestTokenRef.current.set(sourceA, 0);
    sourceRequestTokenRef.current.set(sourceB, 0);
    sourceEventCountRef.current.set(sourceA, 0);
    sourceEventCountRef.current.set(sourceB, 0);

    for (let idx = 1; idx <= PREFETCH_BUFFER_COUNT; idx += 1) {
      const prefetchSource = prefetchSourceId(idx);
      sourceRequestedUrlRef.current.set(prefetchSource, currentTileUrl);
      sourceRequestTokenRef.current.set(prefetchSource, 0);
      sourceEventCountRef.current.set(prefetchSource, 0);
    }
  }, []);

  const setLayerOpacity = useCallback((map: maplibregl.Map, id: string, value: number) => {
    if (!map.getLayer(id)) {
      return;
    }
    map.setPaintProperty(id, "raster-opacity", value);
  }, []);

  const setTilesSafe = useCallback(
    (
      source: maplibregl.RasterTileSource,
      tiles: string[],
      context: { sourceId: string; tileUrl: string; mode: string }
    ): boolean => {
      try {
        source.setTiles(tiles);
        return true;
      } catch (error) {
        if (error instanceof Error && error.name === "AbortError") {
          console.debug("[map] ignored setTiles AbortError", context);
          return false;
        }
        console.warn("[map] setTiles failed", { ...context, error });
        return false;
      }
    },
    []
  );

  const setLayerRasterPaint = useCallback(
    (
      map: maplibregl.Map,
      id: string,
      variableId?: string,
      variableKindId?: string | null,
      basemapModeValue: BasemapMode = "light"
    ) => {
      if (!map.getLayer(id)) {
        return;
      }
      const resamplingMode = getResamplingMode(variableKindId);
      const paintSettings = getOverlayPaintSettings(variableId, basemapModeValue);
      map.setPaintProperty(id, "raster-resampling", resamplingMode);
      map.setPaintProperty(id, "raster-contrast", paintSettings.contrast);
      map.setPaintProperty(id, "raster-saturation", paintSettings.saturation);
      map.setPaintProperty(id, "raster-brightness-min", paintSettings.brightnessMin);
      map.setPaintProperty(id, "raster-brightness-max", paintSettings.brightnessMax);
    },
    []
  );

  const enforceLayerOrder = useCallback((map: maplibregl.Map) => {
    if (!map.getLayer("twf-labels")) {
      return;
    }

    const beforeId = map.getLayer(CONTOUR_LAYER_ID) ? CONTOUR_LAYER_ID : "twf-labels";
    const overlayIds = [
      layerId("a"),
      layerId("b"),
      ...Array.from({ length: PREFETCH_BUFFER_COUNT }, (_, index) => prefetchLayerId(index + 1)),
    ];

    overlayIds.forEach((id) => {
      if (map.getLayer(id)) {
        map.moveLayer(id, beforeId);
      }
    });

    if (map.getLayer(CONTOUR_LAYER_ID)) {
      map.moveLayer(CONTOUR_LAYER_ID, "twf-labels");
    }
    if (map.getLayer(LOOP_LAYER_ID)) {
      map.moveLayer(LOOP_LAYER_ID, "twf-labels");
    }
    if (map.getLayer(COASTLINE_LAYER_ID)) {
      map.moveLayer(COASTLINE_LAYER_ID, "twf-labels");
    }
    if (map.getLayer(COUNTRY_BOUNDARY_LAYER_ID)) {
      map.moveLayer(COUNTRY_BOUNDARY_LAYER_ID, "twf-labels");
    }
    if (map.getLayer(STATE_BOUNDARY_LAYER_ID)) {
      map.moveLayer(STATE_BOUNDARY_LAYER_ID, "twf-labels");
    }
    if (map.getLayer(COUNTY_BOUNDARY_LAYER_ID)) {
      map.moveLayer(COUNTY_BOUNDARY_LAYER_ID, "twf-labels");
    }
    if (map.getLayer(LAKE_MASK_LAYER_ID)) {
      map.moveLayer(LAKE_MASK_LAYER_ID, "twf-labels");
    }
    if (map.getLayer(LAKE_SHORELINE_LAYER_ID)) {
      map.moveLayer(LAKE_SHORELINE_LAYER_ID, "twf-labels");
    }
    map.moveLayer("twf-labels");
  }, []);

  const notifySettled = useCallback(
    (map: maplibregl.Map, source: string, url: string) => {
      let done = false;
      let timeoutId: number | null = null;

      const cleanup = () => {
        map.off("sourcedata", onSourceData);
        if (timeoutId !== null) {
          window.clearTimeout(timeoutId);
          timeoutId = null;
        }
      };

      const fire = () => {
        if (done) return;
        done = true;
        cleanup();
        onTileReady?.(url);
        onFrameSettled?.(url);
      };

      const onSourceData = (event: maplibregl.MapSourceDataEvent) => {
        if (event.sourceId !== source) {
          return;
        }
        sourceEventCountRef.current.set(source, (sourceEventCountRef.current.get(source) ?? 0) + 1);
        if (map.isSourceLoaded(source)) {
          window.requestAnimationFrame(() => fire());
        }
      };

      if (map.isSourceLoaded(source)) {
        window.requestAnimationFrame(() => fire());
        return () => {
          done = true;
          cleanup();
        };
      }

      map.on("sourcedata", onSourceData);
      timeoutId = window.setTimeout(() => {
        console.warn("[map] settle fallback timeout", { sourceId: source, tileUrl: url });
        // Never mark settled from timeout; wait for real source readiness.
      }, SETTLE_TIMEOUT_MS);

      return () => {
        done = true;
        cleanup();
      };
    },
    [onTileReady, onFrameSettled]
  );

  const cancelCrossfade = useCallback(() => {
    fadeTokenRef.current += 1;
    if (fadeRafRef.current !== null) {
      window.cancelAnimationFrame(fadeRafRef.current);
      fadeRafRef.current = null;
    }
  }, []);

  const cancelLoopToTileTransition = useCallback(() => {
    loopToTileTokenRef.current += 1;
    if (loopToTileRafRef.current !== null) {
      window.cancelAnimationFrame(loopToTileRafRef.current);
      loopToTileRafRef.current = null;
    }
    if (loopToTileStableTimerRef.current !== null) {
      window.clearTimeout(loopToTileStableTimerRef.current);
      loopToTileStableTimerRef.current = null;
    }
    if (loopToTileIdleCleanupRef.current) {
      loopToTileIdleCleanupRef.current();
      loopToTileIdleCleanupRef.current = null;
    }
    isLoopToTileTransitioningRef.current = false;
  }, []);

  const runCrossfade = useCallback(
    (map: maplibregl.Map, fromBuffer: OverlayBuffer, toBuffer: OverlayBuffer, targetOpacity: number) => {
      cancelCrossfade();
      const token = fadeTokenRef.current;
      const started = performance.now();

      const tick = (now: number) => {
        if (token !== fadeTokenRef.current) {
          return;
        }
        const progress = Math.min(1, (now - started) / CONTINUOUS_CROSSFADE_MS);
        const fromOpacity = targetOpacity * (1 - progress);
        const toOpacity = targetOpacity * progress;

        setLayerOpacity(map, layerId(fromBuffer), fromOpacity);
        setLayerOpacity(map, layerId(toBuffer), toOpacity);

        if (progress < 1) {
          fadeRafRef.current = window.requestAnimationFrame(tick);
          return;
        }

        setLayerOpacity(map, layerId(toBuffer), targetOpacity);
        // Defer old-buffer hide by 2 paint ticks to avoid white flash.
        window.requestAnimationFrame(() => {
          window.requestAnimationFrame(() => {
            if (token !== fadeTokenRef.current) {
              return;
            }
            setLayerOpacity(map, layerId(fromBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
          });
        });

        fadeRafRef.current = null;
      };

      setLayerOpacity(map, layerId(fromBuffer), targetOpacity);
      setLayerOpacity(map, layerId(toBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
      fadeRafRef.current = window.requestAnimationFrame(tick);
    },
    [cancelCrossfade, setLayerOpacity]
  );

  const runMicroCrossfade = useCallback(
    (map: maplibregl.Map, fromBuffer: OverlayBuffer, toBuffer: OverlayBuffer, targetOpacity: number, token: number) => {
      const started = performance.now();
      
      const tick = (now: number) => {
        if (token !== swapTokenRef.current) {
          return;
        }
        const elapsed = now - started;
        const progress = Math.min(1, elapsed / MICRO_CROSSFADE_MS);
        
        // Quick fade: new layer fades in while old layer stays visible, then old fades out
        const toOpacity = targetOpacity * progress;
        setLayerOpacity(map, layerId(toBuffer), toOpacity);
        
        if (progress < 1) {
          window.requestAnimationFrame(tick);
        } else {
          // Once new layer is fully visible, defer old-layer hide by 2 paint ticks
          // to avoid a brief basemap flash during rapid swaps.
          window.requestAnimationFrame(() => {
            window.requestAnimationFrame(() => {
              if (token !== swapTokenRef.current) {
                return;
              }
              setLayerOpacity(map, layerId(fromBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
            });
          });
        }
      };
      
      // Start with old layer at full opacity, new layer hidden
      setLayerOpacity(map, layerId(fromBuffer), targetOpacity);
      setLayerOpacity(map, layerId(toBuffer), 0);
      window.requestAnimationFrame(tick);
    },
    [setLayerOpacity]
  );

  const waitForSourceReady = useCallback(
    (
      map: maplibregl.Map,
      source: string,
      expectedUrl: string,
      expectedRequestToken: number,
      minEventCount: number,
      modeValue: PlaybackMode,
      onReady: () => void,
      onTimeout?: () => void,
      timeoutMsOverride?: number
    ) => {
      const timeoutMs = timeoutMsOverride
        ?? (modeValue === "autoplay" ? AUTOPLAY_SWAP_TIMEOUT_MS : SCRUB_SWAP_TIMEOUT_MS);
      let done = false;
      let timeoutId: number | null = null;

      const cleanup = () => {
        map.off("sourcedata", onSourceData);
        if (timeoutId !== null) {
          window.clearTimeout(timeoutId);
          timeoutId = null;
        }
      };

      const finishReady = () => {
        if (done) return;
        done = true;
        cleanup();
        onReady();
      };

      const finishTimeout = () => {
        if (done) return;
        if (modeValue === "autoplay") {
          done = true;
          cleanup();
        }
        onTimeout?.();
      };

      const readyForMode = () => {
        const requested = sourceRequestedUrlRef.current.get(source);
        const token = sourceRequestTokenRef.current.get(source) ?? 0;
        const eventCount = sourceEventCountRef.current.get(source) ?? 0;
        return (
          map.isSourceLoaded(source) &&
          requested === expectedUrl &&
          token === expectedRequestToken &&
          eventCount > minEventCount
        );
      };

      const finishReadyAfterRender = () => {
        if (done) return;
        // Double RAF ensures tiles are rendered before swap
        window.requestAnimationFrame(() => {
          window.requestAnimationFrame(() => {
            if (!done) {
              finishReady();
            }
          });
        });
      };

      const onSourceData = (event: maplibregl.MapSourceDataEvent) => {
        if (event.sourceId !== source) {
          return;
        }
        sourceEventCountRef.current.set(source, (sourceEventCountRef.current.get(source) ?? 0) + 1);
        if (readyForMode()) {
          finishReadyAfterRender();
        }
      };

      map.on("sourcedata", onSourceData);

      timeoutId = window.setTimeout(() => finishTimeout(), timeoutMs);

      if (readyForMode()) {
        finishReadyAfterRender();
      }

      return () => {
        done = true;
        cleanup();
      };
    },
    []
  );

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) {
      return;
    }

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: styleFor(tileUrl, opacity, variable, variableKind, overlayFadeOutZoom, contourGeoJsonUrl, basemapMode),
      center: view.center,
      zoom: view.zoom,
      minZoom: view.minZoom ?? 3,
      maxZoom: view.maxZoom ?? 11,
      attributionControl: false,
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-left");
    map.addControl(new maplibregl.AttributionControl({ compact: true }), "bottom-right");

    const handleMapError = (event: { error?: unknown }) => {
      const err = event?.error;
      if (err instanceof Error && err.name === "AbortError") {
        // Expected when setTiles() rapidly supersedes in-flight requests.
        return;
      }
      if (err) {
        console.warn("[map] MapLibre error", err);
      }
    };

    map.on("error", handleMapError as any);

    map.on("load", () => {
      setIsLoaded(true);
      initializeSourceTracking(tileUrl);
      lastAppliedBasemapModeRef.current = basemapMode;
      enforceLayerOrder(map);
    });

    mapRef.current = map;

    return () => {
      map.off("error", handleMapError as any);
      cancelCrossfade();
      cancelLoopToTileTransition();
      map.remove();
      mapRef.current = null;
      setIsLoaded(false);
    };
  }, [cancelCrossfade, cancelLoopToTileTransition, enforceLayerOrder, initializeSourceTracking]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }
    const source = map.getSource(CONTOUR_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
    if (!source || typeof source.setData !== "function") {
      return;
    }
    source.setData((contourGeoJsonUrl ?? EMPTY_FEATURE_COLLECTION) as any);
  }, [contourGeoJsonUrl, isLoaded]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded || !map.getLayer(CONTOUR_LAYER_ID)) {
      return;
    }
    map.setLayoutProperty(
      CONTOUR_LAYER_ID,
      "visibility",
      variable === "tmp2m" ? "visible" : "none"
    );
    enforceLayerOrder(map);
  }, [isLoaded, variable, enforceLayerOrder]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }
    if (lastAppliedBasemapModeRef.current === basemapMode) {
      return;
    }

    const token = ++basemapStyleSwapTokenRef.current;
    lastAppliedBasemapModeRef.current = basemapMode;
    cancelCrossfade();

    const style = styleFor(
      activeTileUrlRef.current,
      opacity,
      variable,
      variableKind,
      overlayFadeOutZoom,
      contourGeoJsonUrl,
      basemapMode
    );

    const onStyleData = () => {
      if (token !== basemapStyleSwapTokenRef.current) {
        return;
      }

      initializeSourceTracking(activeTileUrlRef.current);

      const activeBuffer = activeBufferRef.current;
      const inactiveBuffer = otherBuffer(activeBuffer);
      if (loopActive) {
        setLayerVisibility(map, layerId(activeBuffer), true);
        setLayerOpacity(map, layerId(activeBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
        setLayerOpacity(map, layerId(inactiveBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
        setLayerVisibility(map, layerId(inactiveBuffer), false);
      } else {
        setLayerVisibility(map, layerId(activeBuffer), true);
        setLayerOpacity(map, layerId(activeBuffer), opacity);
        setLayerOpacity(map, layerId(inactiveBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
        setLayerVisibility(map, layerId(inactiveBuffer), false);
      }

      if (loopImageUrl) {
        const loopSource = map.getSource(LOOP_SOURCE_ID) as maplibregl.ImageSource | undefined;
        if (loopSource && typeof loopSource.updateImage === "function") {
          loopSource.updateImage({
            url: loopImageUrl,
            coordinates: LOOP_CONUS_COORDINATES,
          });
        }
      }

      setLayerVisibility(
        map,
        LOOP_LAYER_ID,
        Boolean((loopActive || isLoopToTileTransitioningRef.current) && loopImageUrl)
      );
      setLayerOpacity(map, LOOP_LAYER_ID, opacity);
      for (let idx = 1; idx <= PREFETCH_BUFFER_COUNT; idx += 1) {
        setLayerOpacity(map, prefetchLayerId(idx), HIDDEN_PREFETCH_OPACITY);
        setLayerVisibility(map, prefetchLayerId(idx), false);
      }
      setLayerVisibility(
        map,
        CONTOUR_LAYER_ID,
        variable === "tmp2m" && !loopActive && !isLoopToTileTransitioningRef.current
      );

      setLayerRasterPaint(map, layerId("a"), variable, variableKind, basemapMode);
      setLayerRasterPaint(map, layerId("b"), variable, variableKind, basemapMode);
      for (let idx = 1; idx <= PREFETCH_BUFFER_COUNT; idx += 1) {
        setLayerRasterPaint(map, prefetchLayerId(idx), variable, variableKind, basemapMode);
      }
      setLayerRasterPaint(map, LOOP_LAYER_ID, variable, variableKind, basemapMode);

      enforceLayerOrder(map);
    };

    map.once("styledata", onStyleData);
    map.setStyle(style);

    return () => {
      map.off("styledata", onStyleData);
    };
  }, [
    basemapMode,
    isLoaded,
    cancelCrossfade,
    contourGeoJsonUrl,
    enforceLayerOrder,
    initializeSourceTracking,
    loopActive,
    loopImageUrl,
    overlayFadeOutZoom,
    opacity,
    setLayerOpacity,
    setLayerRasterPaint,
    variable,
    variableKind,
  ]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }

    const lastHintStateRef = { current: false };
    const lastZoomBucketRef = { current: Number.NaN };
    const gestureActiveRef = { current: false };
    let rafId: number | null = null;

    const emitRoutingSignal = () => {
      if (!onZoomRoutingSignal) {
        return;
      }
      onZoomRoutingSignal({ zoom: map.getZoom(), gestureActive: gestureActiveRef.current });
    };

    const scheduleRoutingSignal = () => {
      if (!onZoomRoutingSignal) {
        return;
      }
      if (rafId !== null) {
        return;
      }
      rafId = window.requestAnimationFrame(() => {
        rafId = null;
        emitRoutingSignal();
      });
    };

    const checkZoom = () => {
      const zoom = map.getZoom();
      const bucket = Math.max(0, Math.floor(zoom));
      if (bucket !== lastZoomBucketRef.current) {
        lastZoomBucketRef.current = bucket;
        onZoomBucketChange?.(bucket);
      }
      if (onZoomHint) {
        const shouldShow = Number.isFinite(zoomHintMinZoom) && zoom >= Number(zoomHintMinZoom);
        if (shouldShow !== lastHintStateRef.current) {
          lastHintStateRef.current = shouldShow;
          onZoomHint(shouldShow);
        }
      }
      scheduleRoutingSignal();
    };

    const handleZoomStart = () => {
      gestureActiveRef.current = true;
      emitRoutingSignal();
    };

    const handleZoomEnd = () => {
      gestureActiveRef.current = false;
      const zoom = map.getZoom();
      const bucket = Math.max(0, Math.floor(zoom));
      console.debug("[map] zoom", { zoom: Number(zoom.toFixed(2)), bucket });
      emitRoutingSignal();
    };

    map.on("zoomstart", handleZoomStart);
    map.on("zoomend", handleZoomEnd);
    map.on("moveend", checkZoom);
    map.on("zoom", checkZoom);
    checkZoom();
    emitRoutingSignal();

    return () => {
      map.off("zoomstart", handleZoomStart);
      map.off("zoomend", handleZoomEnd);
      map.off("moveend", checkZoom);
      map.off("zoom", checkZoom);
      if (rafId !== null) {
        window.cancelAnimationFrame(rafId);
        rafId = null;
      }
      if (onZoomHint && lastHintStateRef.current) {
        onZoomHint(false);
      }
    };
  }, [isLoaded, zoomHintMinZoom, onZoomHint, onZoomBucketChange, onZoomRoutingSignal]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded || !loopActive) {
      return;
    }

    onFrameLoadingChange?.(tileUrl, false);

    const emitReady = () => {
      onTileReady?.(tileUrl);
      onFrameSettled?.(tileUrl);
      onTileViewportReady?.(tileUrl);
    };

    if (tileUrl === activeTileUrlRef.current) {
      const active = activeBufferRef.current;
      const source = sourceId(active);
      setLayerVisibility(map, layerId(active), true);
      setLayerOpacity(map, layerId(active), HIDDEN_SWAP_BUFFER_OPACITY);
      const readyCleanup = waitForSourceReady(
        map,
        source,
        tileUrl,
        sourceRequestTokenRef.current.get(source) ?? 0,
        -1,
        "autoplay",
        emitReady,
        () => {
          console.warn("[map] loop warm ready timeout", { sourceId: source, tileUrl });
        },
        PREFETCH_READY_TIMEOUT_MS
      );
      return () => {
        readyCleanup?.();
      };
    }

    const inactiveBuffer = otherBuffer(activeBufferRef.current);
    const inactiveSourceId = sourceId(inactiveBuffer);
    const inactiveSource = map.getSource(inactiveSourceId) as maplibregl.RasterTileSource | undefined;
    if (!inactiveSource || typeof inactiveSource.setTiles !== "function") {
      return;
    }

    setLayerVisibility(map, layerId(inactiveBuffer), true);
    setLayerOpacity(map, layerId(inactiveBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
    if (
      !setTilesSafe(inactiveSource, [tileUrl], {
        sourceId: inactiveSourceId,
        tileUrl,
        mode: "loop-warm",
      })
    ) {
      return;
    }
    sourceRequestedUrlRef.current.set(inactiveSourceId, tileUrl);
    const nextSwapRequestToken = (sourceRequestTokenRef.current.get(inactiveSourceId) ?? 0) + 1;
    sourceRequestTokenRef.current.set(inactiveSourceId, nextSwapRequestToken);
    const eventBaseline = sourceEventCountRef.current.get(inactiveSourceId) ?? 0;
    const warmToken = ++swapTokenRef.current;

    const readyCleanup = waitForSourceReady(
      map,
      inactiveSourceId,
      tileUrl,
      nextSwapRequestToken,
      eventBaseline,
      "autoplay",
      () => {
        if (warmToken !== swapTokenRef.current) {
          return;
        }
        const previousActive = activeBufferRef.current;
        activeBufferRef.current = inactiveBuffer;
        activeTileUrlRef.current = tileUrl;
        setLayerVisibility(map, layerId(previousActive), false);
        setLayerVisibility(map, layerId(inactiveBuffer), true);
        setLayerOpacity(map, layerId(inactiveBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
        emitReady();
      },
      () => {
        if (warmToken !== swapTokenRef.current) {
          return;
        }
        console.warn("[map] loop warm swap timeout", { sourceId: inactiveSourceId, tileUrl, token: warmToken });
      },
      PREFETCH_READY_TIMEOUT_MS
    );

    return () => {
      readyCleanup?.();
    };
  }, [
    isLoaded,
    loopActive,
    tileUrl,
    waitForSourceReady,
    setTilesSafe,
    setLayerOpacity,
    onTileReady,
    onFrameSettled,
    onTileViewportReady,
    onFrameLoadingChange,
  ]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }

    // Foreground tile swap work is disabled while loop mode is active.
    // A separate warm-path effect keeps the active tile buffer up to date
    // at tiny opacity to avoid flashes during WebP -> tile handoff.
    if (loopActive) {
      onFrameLoadingChange?.(tileUrl, false);
      return;
    }
    let settledCleanup: (() => void) | undefined;

    if (tileUrl === activeTileUrlRef.current) {
      const source = sourceId(activeBufferRef.current);
      setLayerVisibility(map, layerId(activeBufferRef.current), true);
      setLayerVisibility(map, layerId(otherBuffer(activeBufferRef.current)), false);
      onFrameLoadingChange?.(tileUrl, false);
      const readyCleanup = waitForSourceReady(
        map,
        source,
        tileUrl,
        sourceRequestTokenRef.current.get(source) ?? 0,
        -1,
        mode,
        () => {
          settledCleanup = notifySettled(map, source, tileUrl);
        },
        () => {
          onFrameLoadingChange?.(tileUrl, true);
          console.warn("[map] ready timeout", { sourceId: source, tileUrl, mode });
        }
      );
      return () => {
        readyCleanup?.();
        settledCleanup?.();
      };
    }

    const inactiveBuffer = otherBuffer(activeBufferRef.current);
    setLayerVisibility(map, layerId(inactiveBuffer), true);
    const inactiveSource = map.getSource(sourceId(inactiveBuffer)) as
      | maplibregl.RasterTileSource
      | undefined;
    if (!inactiveSource || typeof inactiveSource.setTiles !== "function") {
      return;
    }

    const inactiveSourceId = sourceId(inactiveBuffer);
    onFrameLoadingChange?.(tileUrl, true);
    if (
      !setTilesSafe(inactiveSource, [tileUrl], {
        sourceId: inactiveSourceId,
        tileUrl,
        mode: mode,
      })
    ) {
      onFrameLoadingChange?.(tileUrl, false);
      return;
    }
    sourceRequestedUrlRef.current.set(inactiveSourceId, tileUrl);
    const nextSwapRequestToken = (sourceRequestTokenRef.current.get(inactiveSourceId) ?? 0) + 1;
    sourceRequestTokenRef.current.set(inactiveSourceId, nextSwapRequestToken);
    const swapSourceEventBaseline = sourceEventCountRef.current.get(inactiveSourceId) ?? 0;
    const token = ++swapTokenRef.current;

    const finishSwap = (skipSettleNotify = false) => {
      if (token !== swapTokenRef.current) {
        return;
      }

      const previousActive = activeBufferRef.current;
      activeBufferRef.current = inactiveBuffer;
      activeTileUrlRef.current = tileUrl;
      setLayerVisibility(map, layerId(previousActive), true);
      setLayerVisibility(map, layerId(inactiveBuffer), true);

      if (mode === "scrub") {
        cancelCrossfade();
        // Anti-flash scrub swap: keep previous frame visible for extra paint ticks
        // while the next frame is promoted to full opacity, then hide previous.
        // This avoids a brief basemap-white flash between frames.
        setLayerOpacity(map, layerId(previousActive), opacity);
        setLayerOpacity(map, layerId(inactiveBuffer), opacity);
        window.requestAnimationFrame(() => {
          window.requestAnimationFrame(() => {
            if (token !== swapTokenRef.current) {
              return;
            }
            setLayerOpacity(map, layerId(previousActive), HIDDEN_SWAP_BUFFER_OPACITY);
          });
        });
      } else if (crossfade) {
        runCrossfade(map, previousActive, inactiveBuffer, opacity);
      } else {
        cancelCrossfade();
        // Use micro-crossfade for smooth transition without noticeable flash
        runMicroCrossfade(map, previousActive, inactiveBuffer, opacity, token);
      }
      onFrameLoadingChange?.(tileUrl, false);
      if (!skipSettleNotify) {
        settledCleanup = notifySettled(map, sourceId(inactiveBuffer), tileUrl);
      }

      // After promotion, keep only the active buffer visible so MapLibre stops
      // maintaining/reloading stale tiles on the inactive source.
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => {
          if (token !== swapTokenRef.current) {
            return;
          }
          setLayerVisibility(map, layerId(previousActive), false);
          setLayerVisibility(map, layerId(inactiveBuffer), true);
        });
      });
    };

    const readyCleanup = waitForSourceReady(map, inactiveSourceId, tileUrl, nextSwapRequestToken, swapSourceEventBaseline, mode, finishSwap, () => {
      if (token !== swapTokenRef.current) {
        return;
      }
      onFrameLoadingChange?.(tileUrl, true);
      console.warn("[map] swap timeout", { sourceId: inactiveSourceId, tileUrl, token, mode });
    });

    return () => {
      readyCleanup?.();
      settledCleanup?.();
    };
  }, [
    tileUrl,
    isLoaded,
    loopActive,
    mode,
    opacity,
    crossfade,
    waitForSourceReady,
    setTilesSafe,
    runCrossfade,
    cancelCrossfade,
    setLayerOpacity,
    notifySettled,
    onTileReady,
    onFrameSettled,
    onFrameLoadingChange,
  ]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }

    const token = ++prefetchTokenRef.current;
    const urls = Array.from({ length: PREFETCH_BUFFER_COUNT }, (_, idx) => prefetchTileUrls[idx] ?? "");
    const cleanups: Array<() => void> = [];

    urls.forEach((url, idx) => {
      const source = map.getSource(prefetchSourceId(idx + 1)) as maplibregl.RasterTileSource | undefined;
      if (!source || typeof source.setTiles !== "function") {
        return;
      }

      if (!url) {
        prefetchUrlsRef.current[idx] = "";
        setLayerOpacity(map, prefetchLayerId(idx + 1), HIDDEN_PREFETCH_OPACITY);
        setLayerVisibility(map, prefetchLayerId(idx + 1), false);
        return;
      }

      if (prefetchUrlsRef.current[idx] === url) {
        return;
      }

      prefetchUrlsRef.current[idx] = url;
      // Show the layer so MapLibre actually requests the tiles (visibility:none skips them).
      setLayerVisibility(map, prefetchLayerId(idx + 1), true);
      setLayerOpacity(map, prefetchLayerId(idx + 1), WARM_PREFETCH_OPACITY);
      if (
        !setTilesSafe(source, [url], {
          sourceId: prefetchSourceId(idx + 1),
          tileUrl: url,
          mode: "prefetch",
        })
      ) {
        setLayerOpacity(map, prefetchLayerId(idx + 1), HIDDEN_PREFETCH_OPACITY);
        setLayerVisibility(map, prefetchLayerId(idx + 1), false);
        return;
      }
      const prefetchSource = prefetchSourceId(idx + 1);
      sourceRequestedUrlRef.current.set(prefetchSource, url);
      const nextPrefetchRequestToken = (sourceRequestTokenRef.current.get(prefetchSource) ?? 0) + 1;
      sourceRequestTokenRef.current.set(prefetchSource, nextPrefetchRequestToken);
      const prefetchEventBaseline = sourceEventCountRef.current.get(prefetchSource) ?? 0;
      const prefetchEventBudgetThreshold = prefetchEventBaseline + PREFETCH_TILE_EVENT_BUDGET - 1;

      const cleanup = waitForSourceReady(
        map,
        prefetchSource,
        url,
        nextPrefetchRequestToken,
        prefetchEventBudgetThreshold,
        "autoplay",
        () => {
          if (token !== prefetchTokenRef.current) {
            return;
          }
          if (prefetchUrlsRef.current[idx] !== url) {
            return;
          }
          // Important: App.tsx autoplay waits on URLs being marked ready.
          // Prefetch sources should contribute to that readiness cache.
          onTileReady?.(url);
          // Tiles are now in the browser cache — hide the layer so MapLibre stops
          // issuing new requests when the viewport changes.
          setLayerOpacity(map, prefetchLayerId(idx + 1), HIDDEN_PREFETCH_OPACITY);
          setLayerVisibility(map, prefetchLayerId(idx + 1), false);
        },
        () => {
          if (token !== prefetchTokenRef.current) {
            return;
          }
          if (prefetchUrlsRef.current[idx] !== url) {
            return;
          }
          // Best-effort: don't let autoplay deadlock if MapLibre never reports
          // the prefetch source as fully loaded within the timeout window.
          console.warn("[map] prefetch ready fallback timeout", {
            sourceId: prefetchSourceId(idx + 1),
            tileUrl: url,
            token,
          });
          setLayerOpacity(map, prefetchLayerId(idx + 1), HIDDEN_PREFETCH_OPACITY);
          setLayerVisibility(map, prefetchLayerId(idx + 1), false);
        },
        PREFETCH_READY_TIMEOUT_MS
      );

      if (cleanup) {
        cleanups.push(cleanup);
      }
    });

    return () => {
      cleanups.forEach((cleanup) => cleanup());
    };
  }, [prefetchTileUrls, isLoaded, waitForSourceReady, setTilesSafe, onTileReady]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }

    const loopSource = map.getSource(LOOP_SOURCE_ID) as maplibregl.ImageSource | undefined;
    if (loopSource && typeof loopSource.updateImage === "function" && loopImageUrl) {
      try {
        loopSource.updateImage({
          url: loopImageUrl,
          coordinates: LOOP_CONUS_COORDINATES,
        });
      } catch (error) {
        console.warn("[map] failed to update loop image source", { loopImageUrl, error });
      }
    }

    setLayerVisibility(
      map,
      LOOP_LAYER_ID,
      Boolean((loopActive || isLoopToTileTransitioningRef.current) && loopImageUrl)
    );
    setLayerVisibility(
      map,
      CONTOUR_LAYER_ID,
      variable === "tmp2m" && !loopActive && !isLoopToTileTransitioningRef.current
    );
    enforceLayerOrder(map);
  }, [isLoaded, loopImageUrl, loopActive, variable, enforceLayerOrder]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }

    const wasLoopActive = previousLoopActiveRef.current;
    previousLoopActiveRef.current = loopActive;

    const activeBuffer = activeBufferRef.current;
    const inactiveBuffer = otherBuffer(activeBuffer);
    const targetOpacity = Math.max(0, Math.min(1, opacity));

    if (!crossfade) {
      cancelCrossfade();
    }
    cancelLoopToTileTransition();

    if (loopActive) {
      isLoopToTileTransitioningRef.current = false;
      setLayerVisibility(map, layerId(activeBuffer), true);
      setLayerVisibility(map, layerId(inactiveBuffer), false);
      setLayerOpacity(map, layerId(activeBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
      setLayerOpacity(map, layerId(inactiveBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
      setLayerVisibility(map, LOOP_LAYER_ID, Boolean(loopImageUrl));
      setLayerOpacity(map, LOOP_LAYER_ID, targetOpacity);
      setLayerVisibility(map, CONTOUR_LAYER_ID, false);
    } else if (wasLoopActive && loopImageUrl) {
      isLoopToTileTransitioningRef.current = true;
      const transitionToken = ++loopToTileTokenRef.current;
      setLayerVisibility(map, layerId(activeBuffer), true);
      setLayerVisibility(map, layerId(inactiveBuffer), false);
      setLayerOpacity(map, layerId(activeBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
      setLayerOpacity(map, layerId(inactiveBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
      setLayerVisibility(map, LOOP_LAYER_ID, true);
      setLayerOpacity(map, LOOP_LAYER_ID, targetOpacity);

      const startCrossfade = () => {
        if (transitionToken !== loopToTileTokenRef.current) {
          return;
        }
        if (loopToTileIdleCleanupRef.current) {
          loopToTileIdleCleanupRef.current();
          loopToTileIdleCleanupRef.current = null;
        }
        const startedAt = performance.now();
        const tick = (now: number) => {
          if (transitionToken !== loopToTileTokenRef.current) {
            return;
          }
          const progress = Math.min(1, (now - startedAt) / WEBP_TO_TILE_CROSSFADE_MS);
          const tileOpacity = HIDDEN_SWAP_BUFFER_OPACITY + (targetOpacity - HIDDEN_SWAP_BUFFER_OPACITY) * progress;
          const loopOpacity = targetOpacity * (1 - progress);
          setLayerOpacity(map, layerId(activeBuffer), tileOpacity);
          setLayerOpacity(map, LOOP_LAYER_ID, loopOpacity);

          if (progress < 1) {
            loopToTileRafRef.current = window.requestAnimationFrame(tick);
            return;
          }

          setLayerOpacity(map, layerId(activeBuffer), targetOpacity);
          setLayerOpacity(map, LOOP_LAYER_ID, targetOpacity);
          setLayerVisibility(map, LOOP_LAYER_ID, false);
          setLayerVisibility(map, CONTOUR_LAYER_ID, variable === "tmp2m");
          isLoopToTileTransitioningRef.current = false;
          loopToTileRafRef.current = null;
        };

        loopToTileRafRef.current = window.requestAnimationFrame(tick);
      };

      const onIdle = () => {
        if (transitionToken !== loopToTileTokenRef.current) {
          return;
        }
        if (loopToTileStableTimerRef.current !== null) {
          window.clearTimeout(loopToTileStableTimerRef.current);
        }
        loopToTileStableTimerRef.current = window.setTimeout(() => {
          loopToTileStableTimerRef.current = null;
          startCrossfade();
        }, WEBP_TO_TILE_STABLE_MS);
      };

      map.on("idle", onIdle);
      loopToTileIdleCleanupRef.current = () => {
        map.off("idle", onIdle);
      };
      window.requestAnimationFrame(() => {
        if (map.areTilesLoaded()) {
          onIdle();
        }
      });
    } else {
      isLoopToTileTransitioningRef.current = false;
      setLayerVisibility(map, layerId(activeBuffer), true);
      setLayerOpacity(map, layerId(activeBuffer), targetOpacity);
      setLayerOpacity(map, layerId(inactiveBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
      setLayerVisibility(map, layerId(inactiveBuffer), false);
      setLayerVisibility(map, LOOP_LAYER_ID, false);
      setLayerOpacity(map, LOOP_LAYER_ID, targetOpacity);
      setLayerVisibility(map, CONTOUR_LAYER_ID, variable === "tmp2m");
    }
    for (let idx = 1; idx <= PREFETCH_BUFFER_COUNT; idx += 1) {
      setLayerOpacity(map, prefetchLayerId(idx), HIDDEN_PREFETCH_OPACITY);
    }
  }, [
    opacity,
    isLoaded,
    crossfade,
    cancelCrossfade,
    cancelLoopToTileTransition,
    setLayerOpacity,
    loopActive,
    loopImageUrl,
  ]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }

    const token = ++tileViewportReadyTokenRef.current;
    const activeSource = sourceId(activeBufferRef.current);
    const expectedTileUrl = tileUrl;

    const maybeNotify = () => {
      if (token !== tileViewportReadyTokenRef.current) {
        return;
      }
      if (activeTileUrlRef.current !== expectedTileUrl) {
        return;
      }
      if (!map.isSourceLoaded(activeSource)) {
        return;
      }
      onTileViewportReady?.(expectedTileUrl);
    };

    map.on("idle", maybeNotify);
    window.requestAnimationFrame(() => maybeNotify());

    return () => {
      map.off("idle", maybeNotify);
    };
  }, [isLoaded, tileUrl, onTileViewportReady]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }

    setLayerRasterPaint(map, layerId("a"), variable, variableKind, basemapMode);
    setLayerRasterPaint(map, layerId("b"), variable, variableKind, basemapMode);
    for (let idx = 1; idx <= PREFETCH_BUFFER_COUNT; idx += 1) {
      setLayerRasterPaint(map, prefetchLayerId(idx), variable, variableKind, basemapMode);
    }
    setLayerRasterPaint(map, LOOP_LAYER_ID, variable, variableKind, basemapMode);
  }, [isLoaded, variable, variableKind, basemapMode, setLayerRasterPaint]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }
    if (view.bbox) {
      const [west, south, east, north] = view.bbox;
      map.fitBounds([[west, south], [east, north]], { duration: 600, padding: 24 });
    } else {
      map.easeTo({ center: view.center, zoom: view.zoom, duration: 600 });
    }
  }, [view, isLoaded]);

  // ── Hover events for sample tooltip ──────────────────────────────────
  const onMapHoverRef = useRef(onMapHover);
  onMapHoverRef.current = onMapHover;
  const onMapHoverEndRef = useRef(onMapHoverEnd);
  onMapHoverEndRef.current = onMapHoverEnd;

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) return;
    const canvas = map.getCanvas();
    canvas.style.cursor = "";

    const handleMove = (e: maplibregl.MapMouseEvent) => {
      const { lng, lat } = e.lngLat;
      const { x, y } = e.point;
      canvas.style.cursor = onMapHoverRef.current ? "crosshair" : "";
      onMapHoverRef.current?.(lat, lng, x, y);
    };

    const handleLeave = () => {
      canvas.style.cursor = "";
      onMapHoverEndRef.current?.();
    };

    map.on("mousemove", handleMove);
    canvas.addEventListener("mouseleave", handleLeave);

    return () => {
      map.off("mousemove", handleMove);
      canvas.removeEventListener("mouseleave", handleLeave);
      canvas.style.cursor = "";
    };
  }, [isLoaded]);

  return (
    <div
      ref={mapContainerRef}
      className="absolute inset-0"
      style={{ backgroundColor: getMapBackgroundColor(basemapMode) }}
      aria-label="Weather map"
    />
  );
}

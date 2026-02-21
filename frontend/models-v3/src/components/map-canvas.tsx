import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import maplibregl, { type StyleSpecification } from "maplibre-gl";
import type { GeoJSON } from "geojson";

import { DEFAULTS } from "@/lib/config";

const BASEMAP_ATTRIBUTION =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors ' +
  '&copy; <a href="https://carto.com/attributions">CARTO</a>';

const CARTO_LIGHT_BASE_TILES = [
  "https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
  "https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
  "https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
  "https://d.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png",
];

const CARTO_LIGHT_LABEL_TILES = [
  "https://a.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}.png",
  "https://b.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}.png",
  "https://c.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}.png",
  "https://d.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}.png",
];

const REGION_VIEWS: Record<string, { center: [number, number]; zoom: number }> = {
  pnw: { center: [-120.8, 45.6], zoom: 6 },
};

const SCRUB_SWAP_TIMEOUT_MS = 650;
const AUTOPLAY_SWAP_TIMEOUT_MS = 1500;
const SETTLE_TIMEOUT_MS = 1200;
const CONTINUOUS_CROSSFADE_MS = 120;
const MICRO_CROSSFADE_MS = 140;
const PREFETCH_BUFFER_COUNT = 4;
const OVERLAY_RASTER_CONTRAST = 0.08;
const OVERLAY_RASTER_SATURATION = 0.08;
const OVERLAY_RASTER_BRIGHTNESS_MIN = 0.02;
const OVERLAY_RASTER_BRIGHTNESS_MAX = 0.98;

// Keep inactive swap buffer warm at tiny opacity to avoid one-frame basemap flash.
const HIDDEN_SWAP_BUFFER_OPACITY = 0.001;
// Keep prefetch layers fully hidden by default to reduce overdraw/compositing cost.
// Prefetch layers are only warmed while an active prefetch URL is being requested.
const HIDDEN_PREFETCH_OPACITY = 0;
const WARM_PREFETCH_OPACITY = 0.001;
const CONTOUR_SOURCE_ID = "twf-contours";
const CONTOUR_LAYER_ID = "twf-contours";
const EMPTY_FEATURE_COLLECTION: GeoJSON.FeatureCollection = {
  type: "FeatureCollection",
  features: [],
};

type OverlayBuffer = "a" | "b";
type PlaybackMode = "autoplay" | "scrub";

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

function getResamplingMode(variable?: string): "nearest" | "linear" {
  // Discrete/categorical variables use nearest to preserve exact values.
  // Continuous variables (tmp2m, wspd10m, etc.) use linear for smooth display.
  if (
    variable
    && (
      variable.includes("radar")
      || variable.includes("ptype")
      || variable === "refc"
      || variable === "tmp850"
    )
  ) {
    return "nearest";
  }
  return "linear";
}

function getOverlayPaintSettings(variable?: string): {
  contrast: number;
  saturation: number;
  brightnessMin: number;
  brightnessMax: number;
} {
  if (variable === "wspd10m") {
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

function styleFor(
  overlayUrl: string,
  opacity: number,
  variable?: string,
  model?: string,
  contourGeoJsonUrl?: string | null
): StyleSpecification {
  const resamplingMode = getResamplingMode(variable);
  const paintSettings = getOverlayPaintSettings(variable);
  const overlayOpacity: any = model === "gfs"
    ? ["interpolate", ["linear"], ["zoom"], 6, opacity, 7, 0]
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
  return {
    version: 8,
    sources: {
      "twf-basemap": {
        type: "raster",
        tiles: CARTO_LIGHT_BASE_TILES,
        tileSize: 256,
        attribution: BASEMAP_ATTRIBUTION,
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
      [prefetchSourceId(1)]: {
        type: "raster",
        tiles: [overlayUrl],
        tileSize: 512,
      },
      [prefetchSourceId(2)]: {
        type: "raster",
        tiles: [overlayUrl],
        tileSize: 512,
      },
      [prefetchSourceId(3)]: {
        type: "raster",
        tiles: [overlayUrl],
        tileSize: 512,
      },
      [prefetchSourceId(4)]: {
        type: "raster",
        tiles: [overlayUrl],
        tileSize: 512,
      },
      "twf-labels": {
        type: "raster",
        tiles: CARTO_LIGHT_LABEL_TILES,
        tileSize: 256,
      },
      [CONTOUR_SOURCE_ID]: {
        type: "geojson",
        data: contourGeoJsonUrl ?? EMPTY_FEATURE_COLLECTION,
      },
    },
    layers: [
      {
        id: "twf-basemap",
        type: "raster",
        source: "twf-basemap",
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
      {
        id: prefetchLayerId(1),
        type: "raster",
        source: prefetchSourceId(1),
        paint: overlayPaint,
      },
      {
        id: prefetchLayerId(2),
        type: "raster",
        source: prefetchSourceId(2),
        paint: overlayPaint,
      },
      {
        id: prefetchLayerId(3),
        type: "raster",
        source: prefetchSourceId(3),
        paint: overlayPaint,
      },
      {
        id: prefetchLayerId(4),
        type: "raster",
        source: prefetchSourceId(4),
        paint: overlayPaint,
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
          "line-color": "#000000",
          "line-opacity": 0.9,
          "line-width": ["interpolate", ["linear"], ["zoom"], 4, 1, 8, 2, 12, 3],
        },
      },
      {
        id: "twf-labels",
        type: "raster",
        source: "twf-labels",
      },
    ],
  };
}

type MapCanvasProps = {
  tileUrl: string;
  contourGeoJsonUrl?: string | null;
  region: string;
  opacity: number;
  mode: PlaybackMode;
  variable?: string;
  model?: string;
  prefetchTileUrls?: string[];
  crossfade?: boolean;
  onFrameSettled?: (tileUrl: string) => void;
  onTileReady?: (tileUrl: string) => void;
  onFrameLoadingChange?: (tileUrl: string, isLoading: boolean) => void;
  onZoomHint?: (show: boolean) => void;
  onMapHover?: (lat: number, lon: number, x: number, y: number) => void;
  onMapHoverEnd?: () => void;
};

export function MapCanvas({
  tileUrl,
  contourGeoJsonUrl,
  region,
  opacity,
  mode,
  variable,
  model,
  prefetchTileUrls = [],
  crossfade = false,
  onFrameSettled,
  onTileReady,
  onFrameLoadingChange,
  onZoomHint,
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

  const view = useMemo(() => {
    return REGION_VIEWS[region] ?? {
      center: [DEFAULTS.center[1], DEFAULTS.center[0]] as [number, number],
      zoom: DEFAULTS.zoom,
    };
  }, [region]);

  const setLayerOpacity = useCallback((map: maplibregl.Map, id: string, value: number) => {
    if (!map.getLayer(id)) {
      return;
    }
    map.setPaintProperty(id, "raster-opacity", value);
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
      onTimeout?: () => void
    ) => {
      const timeoutMs = modeValue === "autoplay" ? AUTOPLAY_SWAP_TIMEOUT_MS : SCRUB_SWAP_TIMEOUT_MS;
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
      style: styleFor(tileUrl, opacity, variable, model, contourGeoJsonUrl),
      center: view.center,
      zoom: view.zoom,
      minZoom: 3,
      maxZoom: 11,
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-left");

    map.on("load", () => {
      setIsLoaded(true);

      const sourceA = sourceId("a");
      const sourceB = sourceId("b");
      sourceRequestedUrlRef.current.set(sourceA, tileUrl);
      sourceRequestedUrlRef.current.set(sourceB, tileUrl);
      sourceRequestTokenRef.current.set(sourceA, 0);
      sourceRequestTokenRef.current.set(sourceB, 0);
      sourceEventCountRef.current.set(sourceA, 0);
      sourceEventCountRef.current.set(sourceB, 0);
      for (let idx = 1; idx <= PREFETCH_BUFFER_COUNT; idx += 1) {
        const prefetchSource = prefetchSourceId(idx);
        sourceRequestedUrlRef.current.set(prefetchSource, tileUrl);
        sourceRequestTokenRef.current.set(prefetchSource, 0);
        sourceEventCountRef.current.set(prefetchSource, 0);
      }
    });

    mapRef.current = map;

    return () => {
      cancelCrossfade();
      map.remove();
      mapRef.current = null;
      setIsLoaded(false);
    };
  }, [cancelCrossfade]);

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
  }, [isLoaded, variable]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded || !onZoomHint) {
      return;
    }

    const lastHintStateRef = { current: false };

    const checkZoom = () => {
      const zoom = map.getZoom();
      const shouldShow = model === "gfs" && zoom >= 7;
      if (shouldShow !== lastHintStateRef.current) {
        lastHintStateRef.current = shouldShow;
        onZoomHint(shouldShow);
      }
    };

    map.on("moveend", checkZoom);
    checkZoom();

    return () => {
      map.off("moveend", checkZoom);
      if (lastHintStateRef.current) {
        onZoomHint(false);
      }
    };
  }, [isLoaded, model, onZoomHint]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }
    let settledCleanup: (() => void) | undefined;

    if (tileUrl === activeTileUrlRef.current) {
      const source = sourceId(activeBufferRef.current);
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
    const inactiveSource = map.getSource(sourceId(inactiveBuffer)) as
      | maplibregl.RasterTileSource
      | undefined;
    if (!inactiveSource || typeof inactiveSource.setTiles !== "function") {
      return;
    }

    onFrameLoadingChange?.(tileUrl, true);
    inactiveSource.setTiles([tileUrl]);
    const inactiveSourceId = sourceId(inactiveBuffer);
    sourceRequestedUrlRef.current.set(inactiveSourceId, tileUrl);
    const nextSwapRequestToken = (sourceRequestTokenRef.current.get(inactiveSourceId) ?? 0) + 1;
    sourceRequestTokenRef.current.set(inactiveSourceId, nextSwapRequestToken);
    const swapSourceEventBaseline = sourceEventCountRef.current.get(inactiveSourceId) ?? 0;
    const token = ++swapTokenRef.current;
    console.debug("[map] swap start", { sourceId: inactiveSourceId, tileUrl, mode, token });

    const finishSwap = (skipSettleNotify = false) => {
      if (token !== swapTokenRef.current) {
        return;
      }

      const previousActive = activeBufferRef.current;
      activeBufferRef.current = inactiveBuffer;
      activeTileUrlRef.current = tileUrl;

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
      console.debug("[map] swap end", { sourceId: sourceId(inactiveBuffer), tileUrl, mode, token });
      if (!skipSettleNotify) {
        settledCleanup = notifySettled(map, sourceId(inactiveBuffer), tileUrl);
      }
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
    mode,
    opacity,
    crossfade,
    waitForSourceReady,
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
        return;
      }

      if (prefetchUrlsRef.current[idx] === url) {
        return;
      }

      prefetchUrlsRef.current[idx] = url;
  setLayerOpacity(map, prefetchLayerId(idx + 1), WARM_PREFETCH_OPACITY);
      source.setTiles([url]);
      const prefetchSource = prefetchSourceId(idx + 1);
      sourceRequestedUrlRef.current.set(prefetchSource, url);
      const nextPrefetchRequestToken = (sourceRequestTokenRef.current.get(prefetchSource) ?? 0) + 1;
      sourceRequestTokenRef.current.set(prefetchSource, nextPrefetchRequestToken);
      const prefetchEventBaseline = sourceEventCountRef.current.get(prefetchSource) ?? 0;

      const cleanup = waitForSourceReady(
        map,
        prefetchSource,
        url,
        nextPrefetchRequestToken,
        prefetchEventBaseline,
        "scrub",
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
          setLayerOpacity(map, prefetchLayerId(idx + 1), HIDDEN_PREFETCH_OPACITY);
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
        }
      );

      if (cleanup) {
        cleanups.push(cleanup);
      }
    });

    return () => {
      cleanups.forEach((cleanup) => cleanup());
    };
  }, [prefetchTileUrls, isLoaded, waitForSourceReady, onTileReady]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }

    const activeBuffer = activeBufferRef.current;
    const inactiveBuffer = otherBuffer(activeBuffer);

    if (!crossfade) {
      cancelCrossfade();
    }

    setLayerOpacity(map, layerId(activeBuffer), opacity);
    setLayerOpacity(map, layerId(inactiveBuffer), HIDDEN_SWAP_BUFFER_OPACITY);
    for (let idx = 1; idx <= PREFETCH_BUFFER_COUNT; idx += 1) {
      setLayerOpacity(map, prefetchLayerId(idx), HIDDEN_PREFETCH_OPACITY);
    }
  }, [opacity, isLoaded, crossfade, cancelCrossfade, setLayerOpacity]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }
    map.easeTo({ center: view.center, zoom: view.zoom, duration: 600 });
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

  return <div ref={mapContainerRef} className="absolute inset-0" aria-label="Weather map" />;
}

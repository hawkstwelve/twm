import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import maplibregl, { type StyleSpecification } from "maplibre-gl";

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
const MICRO_CROSSFADE_MS = 60; // Very brief crossfade to avoid white flash
const PREFETCH_BUFFER_COUNT = 4;

// Keep hidden raster layers at a tiny opacity so MapLibre still requests/renders their tiles.
// This helps avoid a one-frame "basemap flash" when swapping buffers.
const HIDDEN_OPACITY = 0.001;

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
  // radar_ptype needs bilinear/linear for better clarity
  // All other variables use nearest to preserve discrete data
  if (variable && (variable.includes("radar") || variable.includes("ptype"))) {
    return "linear";
  }
  return "nearest";
}

function styleFor(overlayUrl: string, opacity: number, variable?: string, model?: string): StyleSpecification {
  const resamplingMode = getResamplingMode(variable);
  const overlayOpacity: any = model === "gfs"
    ? ["interpolate", ["linear"], ["zoom"], 6, opacity, 7, 0]
    : opacity;
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
        tileSize: 256,
      },
      [sourceId("b")]: {
        type: "raster",
        tiles: [overlayUrl],
        tileSize: 256,
      },
      [prefetchSourceId(1)]: {
        type: "raster",
        tiles: [overlayUrl],
        tileSize: 256,
      },
      [prefetchSourceId(2)]: {
        type: "raster",
        tiles: [overlayUrl],
        tileSize: 256,
      },
      [prefetchSourceId(3)]: {
        type: "raster",
        tiles: [overlayUrl],
        tileSize: 256,
      },
      [prefetchSourceId(4)]: {
        type: "raster",
        tiles: [overlayUrl],
        tileSize: 256,
      },
      "twf-labels": {
        type: "raster",
        tiles: CARTO_LIGHT_LABEL_TILES,
        tileSize: 256,
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
        paint: {
          "raster-opacity": overlayOpacity,
          "raster-resampling": resamplingMode,
          "raster-fade-duration": 0,
        },
      },
      {
        id: layerId("b"),
        type: "raster",
        source: sourceId("b"),
        paint: {
          "raster-opacity": overlayOpacity,
          "raster-resampling": resamplingMode,
          "raster-fade-duration": 0,
        },
      },
      {
        id: prefetchLayerId(1),
        type: "raster",
        source: prefetchSourceId(1),
        paint: {
          "raster-opacity": overlayOpacity,
          "raster-resampling": resamplingMode,
          "raster-fade-duration": 0,
        },
      },
      {
        id: prefetchLayerId(2),
        type: "raster",
        source: prefetchSourceId(2),
        paint: {
          "raster-opacity": overlayOpacity,
          "raster-resampling": resamplingMode,
          "raster-fade-duration": 0,
        },
      },
      {
        id: prefetchLayerId(3),
        type: "raster",
        source: prefetchSourceId(3),
        paint: {
          "raster-opacity": overlayOpacity,
          "raster-resampling": resamplingMode,
          "raster-fade-duration": 0,
        },
      },
      {
        id: prefetchLayerId(4),
        type: "raster",
        source: prefetchSourceId(4),
        paint: {
          "raster-opacity": overlayOpacity,
          "raster-resampling": resamplingMode,
          "raster-fade-duration": 0,
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
  region: string;
  opacity: number;
  mode: PlaybackMode;
  variable?: string;
  model?: string;
  prefetchTileUrls?: string[];
  crossfade?: boolean;
  onFrameSettled?: (tileUrl: string) => void;
  onTileReady?: (tileUrl: string) => void;
  onZoomHint?: (show: boolean) => void;
};

export function MapCanvas({
  tileUrl,
  region,
  opacity,
  mode,
  variable,
  model,
  prefetchTileUrls = [],
  crossfade = false,
  onFrameSettled,
  onTileReady,
  onZoomHint,
}: MapCanvasProps) {
  const mapContainerRef = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [isLoaded, setIsLoaded] = useState(false);
  const activeBufferRef = useRef<OverlayBuffer>("a");
  const activeTileUrlRef = useRef(tileUrl);
  const swapTokenRef = useRef(0);
  const prefetchTokenRef = useRef(0);
  const prefetchUrlsRef = useRef<string[]>(Array.from({ length: PREFETCH_BUFFER_COUNT }, () => ""));
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
        fire();
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

        // Leave the old buffer at a tiny opacity so its tiles remain warm.
        setLayerOpacity(map, layerId(fromBuffer), HIDDEN_OPACITY);
        setLayerOpacity(map, layerId(toBuffer), targetOpacity);

        fadeRafRef.current = null;
      };

      setLayerOpacity(map, layerId(fromBuffer), targetOpacity);
      setLayerOpacity(map, layerId(toBuffer), HIDDEN_OPACITY);
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
          // Once new layer is fully visible, hide old layer
          setLayerOpacity(map, layerId(fromBuffer), HIDDEN_OPACITY);
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
        done = true;
        cleanup();
        onTimeout?.();
      };

      const readyForMode = () => {
        return map.isSourceLoaded(source);
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
      style: styleFor(tileUrl, opacity, variable, model),
      center: view.center,
      zoom: view.zoom,
      minZoom: 3,
      maxZoom: 11,
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-left");

    map.on("load", () => {
      setIsLoaded(true);
    });

    mapRef.current = map;
    map.on("move", () => {
      // zoom is fractional; round for readability
      console.log("zoom", map.getZoom().toFixed(2));
    });

    return () => {
      cancelCrossfade();
      map.remove();
      mapRef.current = null;
      setIsLoaded(false);
    };
  }, [cancelCrossfade]);

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
      const readyCleanup = waitForSourceReady(
        map,
        source,
        mode,
        () => {
          settledCleanup = notifySettled(map, source, tileUrl);
        },
        () => {
          if (mode === "autoplay") {
            console.warn("[map] ready fallback timeout", { sourceId: source, tileUrl });
            onTileReady?.(tileUrl);
            onFrameSettled?.(tileUrl);
          }
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

    inactiveSource.setTiles([tileUrl]);
    const token = ++swapTokenRef.current;
    console.debug("[map] swap start", { sourceId: sourceId(inactiveBuffer), tileUrl, mode, token });

    const finishSwap = (skipSettleNotify = false) => {
      if (token !== swapTokenRef.current) {
        return;
      }

      const previousActive = activeBufferRef.current;
      activeBufferRef.current = inactiveBuffer;
      activeTileUrlRef.current = tileUrl;

      if (crossfade) {
        runCrossfade(map, previousActive, inactiveBuffer, opacity);
      } else {
        cancelCrossfade();
        // Use micro-crossfade for smooth transition without noticeable flash
        runMicroCrossfade(map, previousActive, inactiveBuffer, opacity, token);
      }
      console.debug("[map] swap end", { sourceId: sourceId(inactiveBuffer), tileUrl, mode, token });
      if (!skipSettleNotify) {
        settledCleanup = notifySettled(map, sourceId(inactiveBuffer), tileUrl);
      }
    };

    const readyCleanup = waitForSourceReady(map, sourceId(inactiveBuffer), mode, finishSwap, () => {
      if (token !== swapTokenRef.current) {
        return;
      }
      if (mode === "autoplay") {
        console.warn("[map] swap fallback timeout", { sourceId: sourceId(inactiveBuffer), tileUrl, token });
        onTileReady?.(tileUrl);
        onFrameSettled?.(tileUrl);
        finishSwap(true);
      }
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
        return;
      }

      if (prefetchUrlsRef.current[idx] === url) {
        return;
      }

      prefetchUrlsRef.current[idx] = url;
      source.setTiles([url]);

      const cleanup = waitForSourceReady(
        map,
        prefetchSourceId(idx + 1),
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
          onTileReady?.(url);
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
    setLayerOpacity(map, layerId(inactiveBuffer), HIDDEN_OPACITY);
    for (let idx = 1; idx <= PREFETCH_BUFFER_COUNT; idx += 1) {
      setLayerOpacity(map, prefetchLayerId(idx), HIDDEN_OPACITY);
    }
  }, [opacity, isLoaded, crossfade, cancelCrossfade, setLayerOpacity]);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }
    map.easeTo({ center: view.center, zoom: view.zoom, duration: 600 });
  }, [view, isLoaded]);

  return <div ref={mapContainerRef} className="absolute inset-0" aria-label="Weather map" />;
}

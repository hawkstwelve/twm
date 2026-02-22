import { useCallback, useEffect, useRef, useState } from "react";
import { type SampleResult, fetchSample } from "@/lib/api";

// ── LRU Cache ────────────────────────────────────────────────────────

const LRU_CAPACITY = 256;

type LRUEntry = { key: string; value: SampleResult | null };

class LRUCache {
  private map = new Map<string, SampleResult | null>();
  private keys: string[] = [];

  get(key: string): SampleResult | null | undefined {
    if (!this.map.has(key)) return undefined;
    // Move to end (most recently used)
    this.keys = this.keys.filter((k) => k !== key);
    this.keys.push(key);
    return this.map.get(key)!;
  }

  set(key: string, value: SampleResult | null): void {
    if (this.map.has(key)) {
      this.keys = this.keys.filter((k) => k !== key);
    } else if (this.keys.length >= LRU_CAPACITY) {
      const evict = this.keys.shift()!;
      this.map.delete(evict);
    }
    this.keys.push(key);
    this.map.set(key, value);
  }

  clear(): void {
    this.map.clear();
    this.keys = [];
  }
}

// ── Round lat/lon to 2 decimals (~1.1 km precision) ──────────────────

function roundCoord(v: number): number {
  return Math.round(v * 100) / 100;
}

function cacheKey(
  model: string,
  run: string,
  varId: string,
  fh: number,
  lat: number,
  lon: number
): string {
  return `${model}/${run}/${varId}/${fh}/${roundCoord(lat)}/${roundCoord(lon)}`;
}

// ── Debounce interval (ms) ───────────────────────────────────────────

const DEBOUNCE_MS = 80;

// ── Hook ─────────────────────────────────────────────────────────────

export type SampleTooltipState = {
  value: number;
  units: string;
  x: number;
  y: number;
} | null;

export type SampleContext = {
  model: string;
  run: string;
  varId: string;
  fh: number;
};

export function useSampleTooltip(ctx: SampleContext) {
  const [tooltip, setTooltip] = useState<SampleTooltipState>(null);
  const genRef = useRef(0);
  const timerRef = useRef<number | null>(null);
  const cacheRef = useRef(new LRUCache());
  const prevCtxRef = useRef<string>("");

  // Clear cache when model/run/var change
  const ctxFingerprint = `${ctx.model}/${ctx.run}/${ctx.varId}`;
  useEffect(() => {
    if (ctxFingerprint !== prevCtxRef.current) {
      cacheRef.current.clear();
      prevCtxRef.current = ctxFingerprint;
    }
  }, [ctxFingerprint]);

  const onHover = useCallback(
    (lat: number, lon: number, x: number, y: number) => {
      // Cancel any pending debounce
      if (timerRef.current !== null) {
        window.clearTimeout(timerRef.current);
      }

      timerRef.current = window.setTimeout(() => {
        timerRef.current = null;
        const gen = ++genRef.current;
        const roundedLat = roundCoord(lat);
        const roundedLon = roundCoord(lon);
        const key = cacheKey(ctx.model, ctx.run, ctx.varId, ctx.fh, roundedLat, roundedLon);

        // Check LRU cache
        const cached = cacheRef.current.get(key);
        if (cached !== undefined) {
          if (gen !== genRef.current) return; // stale
          if (cached === null) {
            setTooltip(null);
          } else {
            setTooltip({ value: cached.value, units: cached.units, x, y });
          }
          return;
        }

        // Fetch from API
        fetchSample({
          model: ctx.model,
          run: ctx.run,
          var: ctx.varId,
          fh: ctx.fh,
          lat: roundedLat,
          lon: roundedLon,
        })
          .then((result) => {
            cacheRef.current.set(key, result);
            if (gen !== genRef.current) return; // stale — cursor already moved
            if (!result) {
              setTooltip(null);
              return;
            }
            setTooltip({ value: result.value, units: result.units, x, y });
          })
          .catch(() => {
            // Silently drop errors — don't flash error state for hover
            if (gen !== genRef.current) return;
            setTooltip(null);
          });
      }, DEBOUNCE_MS);
    },
    [ctx.model, ctx.run, ctx.varId, ctx.fh]
  );

  const onHoverEnd = useCallback(() => {
    // Bump generation to discard any in-flight response
    genRef.current++;
    if (timerRef.current !== null) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
    setTooltip(null);
  }, []);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (timerRef.current !== null) {
        window.clearTimeout(timerRef.current);
      }
    };
  }, []);

  return { tooltip, onHover, onHoverEnd };
}

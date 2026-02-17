import { useEffect, useRef, useState } from "react";
import { AlertCircle, ChevronDown, ChevronUp } from "lucide-react";

import { Slider } from "@/components/ui/slider";
import { cn } from "@/lib/utils";

export type LegendEntry = {
  value: number;
  color: string;
};

export type LegendPayload = {
  title: string;
  units?: string;
  kind?: string;
  id?: string;
  ptype_breaks?: Record<string, { offset: number; count: number }>;
  ptype_order?: string[];
  bins_per_ptype?: number;
  entries: LegendEntry[];
  opacity: number;
};

function formatValue(value: number): string {
  if (Number.isInteger(value)) return value.toString();
  if (Math.abs(value) < 0.1) return value.toFixed(2);
  if (Math.abs(value) < 1) return value.toFixed(1);
  return value.toFixed(0);
}

function UnavailablePlaceholder() {
  return (
    <div className="flex items-center gap-1.5 rounded-md border border-border/40 bg-[hsl(var(--toolbar))]/95 px-2 py-2 shadow-xl backdrop-blur-md">
      <AlertCircle className="h-3.5 w-3.5 shrink-0 text-muted-foreground/70" />
      <span className="text-xs font-medium text-muted-foreground/80">Legend unavailable</span>
    </div>
  );
}

const RADAR_GROUP_LABELS = ["Rain", "Snow", "Sleet", "Freezing Rain"];
const DEFAULT_PTYPE_ORDER = ["rain", "snow", "sleet", "frzr"];
const LEGEND_COLLAPSED_STORAGE_KEY = "twf.legend.collapsed";

type RadarLegendGroup = {
  label: string;
  entries: LegendEntry[];
};

type PrecipPtypeLegendRow = {
  label: string;
  min: number;
  max: number;
  colors: string[];
};

function radarGroupLabelForCode(code: string, index: number): string {
  const normalized = code.toLowerCase();
  if (normalized === "rain") return "Rain";
  if (normalized === "snow") return "Snow";
  if (normalized === "sleet") return "Sleet";
  if (normalized === "frzr") return "Freezing Rain";
  return RADAR_GROUP_LABELS[index] ?? `Type ${index + 1}`;
}

function readCollapsedPreference(): boolean {
  if (typeof window === "undefined") return true;
  try {
    const stored = window.localStorage.getItem(LEGEND_COLLAPSED_STORAGE_KEY);
    if (stored === null) return true;
    return stored === "true";
  } catch {
    return true;
  }
}

function writeCollapsedPreference(value: boolean): void {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(LEGEND_COLLAPSED_STORAGE_KEY, String(value));
  } catch {
    // Ignore storage errors (private mode/quota).
  }
}

function isRadarPtypeLegend(legend: LegendPayload): boolean {
  const kind = legend.kind?.toLowerCase() ?? "";
  const id = legend.id?.toLowerCase() ?? "";
  return (
    kind.includes("radar_ptype") ||
    kind.includes("radar_ptype_combo") ||
    id.includes("radar") ||
    id === "radar_ptype"
  );
}

function isPrecipPtypeLegend(legend: LegendPayload): boolean {
  const kind = legend.kind?.toLowerCase() ?? "";
  const id = legend.id?.toLowerCase() ?? "";
  return kind.includes("precip_ptype") || id === "precip_ptype";
}

function groupRadarEntries(
  entries: LegendEntry[],
  ptypeBreaks?: Record<string, { offset: number; count: number }>,
  ptypeOrder?: string[]
): RadarLegendGroup[] {
  const isZero = (value: number) => Math.abs(value) < 1e-9;

  if (ptypeBreaks) {
    const orderedTypes = (Array.isArray(ptypeOrder) && ptypeOrder.length > 0 ? ptypeOrder : DEFAULT_PTYPE_ORDER).filter(
      (ptype) => ptypeBreaks[ptype]
    );
    const groupedByMeta: RadarLegendGroup[] = [];

    for (let index = 0; index < orderedTypes.length; index += 1) {
      const ptype = orderedTypes[index];
      const boundary = ptypeBreaks[ptype];
      if (!boundary) continue;
      const offset = Number(boundary.offset);
      const count = Number(boundary.count);
      if (!Number.isFinite(offset) || !Number.isFinite(count) || offset < 0 || count <= 0) {
        continue;
      }
      const slice = entries
        .slice(offset, offset + count)
        .filter((entry) => !isZero(entry.value))
        .slice()
        .reverse();
      if (slice.length === 0) continue;
      groupedByMeta.push({
        label: radarGroupLabelForCode(ptype, index),
        entries: slice,
      });
    }

    if (groupedByMeta.length > 0) {
      return groupedByMeta;
    }
  }

  // Fallback: split displayed sequence on zero-value delimiters.
  const displayed = entries.slice().reverse();
  const fallbackGroups: RadarLegendGroup[] = [];
  let current: LegendEntry[] = [];

  for (const entry of displayed) {
    if (isZero(entry.value)) {
      if (current.length > 0) {
        fallbackGroups.push({
          label: RADAR_GROUP_LABELS[fallbackGroups.length] ?? `Type ${fallbackGroups.length + 1}`,
          entries: current,
        });
        current = [];
      }
      continue;
    }
    current.push(entry);
  }

  if (current.length > 0) {
    fallbackGroups.push({
      label: RADAR_GROUP_LABELS[fallbackGroups.length] ?? `Type ${fallbackGroups.length + 1}`,
      entries: current,
    });
  }

  return fallbackGroups;
}

function groupPrecipPtypeRows(
  entries: LegendEntry[],
  ptypeBreaks?: Record<string, { offset: number; count: number }>,
  ptypeOrder?: string[]
): PrecipPtypeLegendRow[] {
  if (!ptypeBreaks) return [];
  const orderedTypes = (Array.isArray(ptypeOrder) && ptypeOrder.length > 0 ? ptypeOrder : []).filter(
    (ptype) => ptypeBreaks[ptype]
  );
  if (orderedTypes.length === 0) return [];

  const rows: PrecipPtypeLegendRow[] = [];
  for (let index = 0; index < orderedTypes.length; index += 1) {
    const ptype = orderedTypes[index];
    const boundary = ptypeBreaks[ptype];
    if (!boundary) continue;
    const offset = Number(boundary.offset);
    const count = Number(boundary.count);
    if (!Number.isFinite(offset) || !Number.isFinite(count) || offset < 0 || count <= 0) {
      continue;
    }
    const segment = entries.slice(offset, offset + count);
    if (segment.length === 0) continue;
    const colors = segment.map((entry) => entry.color).filter(Boolean);
    if (colors.length === 0) continue;
    const min = Number(segment[0]?.value);
    const max = Number(segment[segment.length - 1]?.value);
    if (!Number.isFinite(min) || !Number.isFinite(max)) continue;
    rows.push({
      label: radarGroupLabelForCode(ptype, index),
      min,
      max,
      colors,
    });
  }

  return rows;
}

type MapLegendProps = {
  legend: LegendPayload | null;
  onOpacityChange: (opacity: number) => void;
};

export function MapLegend({ legend, onOpacityChange }: MapLegendProps) {
  const [collapsed, setCollapsed] = useState<boolean>(() => readCollapsedPreference());
  const [isSmallScreen, setIsSmallScreen] = useState(false);
  const [fadeKey, setFadeKey] = useState(0);
  const prevTitleRef = useRef(legend?.title);

  useEffect(() => {
    const mq = window.matchMedia("(max-width: 640px)");
    const handler = (query: MediaQueryList | MediaQueryListEvent) => {
      setIsSmallScreen(query.matches);
      if (query.matches) {
        setCollapsed(true);
        writeCollapsedPreference(true);
      }
    };
    handler(mq);
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, []);

  useEffect(() => {
    if (legend?.title !== prevTitleRef.current) {
      setFadeKey((value) => value + 1);
      prevTitleRef.current = legend?.title;
    }
  }, [legend?.title]);

  if (!legend) {
    return (
      <div className={cn("pointer-events-none fixed z-40", isSmallScreen ? "bottom-24 right-4" : "right-4 top-20")}>
        <UnavailablePlaceholder />
      </div>
    );
  }

  const opacityPercent = Math.round(legend.opacity * 100);
  const precipPtypeRows = isPrecipPtypeLegend(legend)
    ? groupPrecipPtypeRows(legend.entries, legend.ptype_breaks, legend.ptype_order)
    : [];
  const showPrecipPtypeRows = precipPtypeRows.length > 0;
  const groupedRadarEntries = isRadarPtypeLegend(legend)
    ? groupRadarEntries(legend.entries, legend.ptype_breaks, legend.ptype_order)
    : [];
  const showGroupedRadar = groupedRadarEntries.length > 0;

  return (
    <div
      className={cn(
        "fixed z-40 flex flex-col max-h-[70vh] overflow-hidden rounded-md border border-border/50 bg-[hsl(var(--toolbar))]/95 shadow-xl backdrop-blur-md transition-all duration-200",
        showPrecipPtypeRows ? "w-[220px]" : "w-[120px]",
        isSmallScreen ? "bottom-24 right-4" : "right-4 top-20"
      )}
      role="complementary"
      aria-label="Map legend"
    >
      <button
        type="button"
        onClick={() =>
          setCollapsed((value) => {
            const next = !value;
            writeCollapsedPreference(next);
            return next;
          })
        }
        className="flex w-full items-center justify-between gap-1.5 border-b border-border/30 px-1.5 py-1 text-left transition-all duration-150 hover:bg-secondary/30 active:bg-secondary/50"
        aria-expanded={!collapsed}
        aria-controls="legend-body"
      >
        <div className="flex min-w-0 flex-col gap-0.5 overflow-hidden">
          <span className="truncate text-xs font-semibold tracking-tight text-foreground">{legend.title}</span>
          {legend.units && (
            <span className="text-[10px] font-medium text-muted-foreground/80">{legend.units}</span>
          )}
        </div>
        {collapsed ? (
          <ChevronDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground transition-transform duration-150" />
        ) : (
          <ChevronUp className="h-3.5 w-3.5 shrink-0 text-muted-foreground transition-transform duration-150" />
        )}
      </button>

      <div
        id="legend-body"
        className={cn("grid transition-[grid-template-rows] duration-200 ease-out", collapsed ? "grid-rows-[0fr]" : "grid-rows-[1fr]")}
      >
        <div className="overflow-hidden">
          <div key={fadeKey} className="flex flex-col gap-1.5 px-1.5 py-1.5 animate-in fade-in duration-200">
            <div className="max-h-[45vh] space-y-px overflow-y-auto scroll-smooth">
              {showPrecipPtypeRows
                ? precipPtypeRows.map((row, rowIndex) => (
                    <div
                      key={`precip-row-${row.label}-${rowIndex}`}
                      className={cn(rowIndex > 0 ? "mt-2 border-t border-border/20 pt-2" : "")}
                    >
                      <div className="mb-1 flex items-center justify-between gap-2 px-0.5">
                        <span className="text-[9px] font-semibold uppercase tracking-wide text-muted-foreground/85">
                          {row.label}
                        </span>
                        <span className="font-mono text-[9px] font-medium tabular-nums text-foreground/90">
                          {formatValue(row.min)}-{formatValue(row.max)} {legend.units ?? ""}
                        </span>
                      </div>
                      <div
                        className="h-3 rounded-[2px] border border-border/40 shadow-sm"
                        style={{ backgroundImage: `linear-gradient(to right, ${row.colors.join(", ")})` }}
                      />
                    </div>
                  ))
                : showGroupedRadar
                ? groupedRadarEntries.map((group, groupIndex) => (
                    <div
                      key={`group-${groupIndex}`}
                      className={cn(groupIndex > 0 ? "mt-2 border-t border-border/20 pt-2" : "")}
                    >
                      <div className="mb-1 px-0.5 text-[9px] font-semibold uppercase tracking-wide text-muted-foreground/85">
                        {group.label}
                      </div>
                      {group.entries.map((entry, index) => (
                        <div
                          key={`${entry.value}-${entry.color}-${groupIndex}-${index}`}
                          className={cn(
                            "flex items-center gap-1.5 rounded-[2px] px-0.5 py-0.5 transition-colors duration-150",
                            index % 2 === 0 ? "bg-secondary/20" : "bg-transparent"
                          )}
                        >
                          <span
                            className="h-3 w-3 shrink-0 rounded-[2px] border border-border/30 shadow-sm"
                            style={{ backgroundColor: entry.color }}
                          />
                          <span className="font-mono text-[10px] font-medium leading-none tabular-nums tracking-tight text-foreground/95">
                            {formatValue(entry.value)}
                          </span>
                        </div>
                      ))}
                    </div>
                  ))
                : legend.entries.slice().reverse().map((entry, index) => (
                    <div
                      key={`${entry.value}-${entry.color}-${index}`}
                      className={cn(
                        "flex items-center gap-1.5 rounded-[2px] px-0.5 py-0.5 transition-colors duration-150",
                        index % 2 === 0 ? "bg-secondary/20" : "bg-transparent"
                      )}
                    >
                      <span
                        className="h-3 w-3 shrink-0 rounded-[2px] border border-border/30 shadow-sm"
                        style={{ backgroundColor: entry.color }}
                      />
                      <span className="font-mono text-[10px] font-medium leading-none tabular-nums tracking-tight text-foreground/95">
                        {formatValue(entry.value)}
                      </span>
                    </div>
                  ))}
            </div>

            <div className="border-t border-border/30 pt-1.5">
              <div className="mb-1 flex items-center justify-between">
                <span className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
                  Opacity
                </span>
                <span className="font-mono text-[10px] font-medium tabular-nums tracking-tight text-foreground/90">
                  {opacityPercent}%
                </span>
              </div>
              <Slider
                value={[opacityPercent]}
                onValueChange={([value]) => onOpacityChange((value ?? 100) / 100)}
                min={0}
                max={100}
                step={1}
                className="w-full transition-opacity duration-150"
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

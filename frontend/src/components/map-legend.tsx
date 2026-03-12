import { useEffect, useRef, useState, type Ref } from "react";
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
  return value.toFixed(1);
}

function UnavailablePlaceholder() {
  return (
    <div className="flex items-center gap-1.5 rounded-xl glass px-2.5 py-2">
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

const DENSE_LEGEND_THRESHOLD = 18;
const DENSE_GRADIENT_LABEL_COUNT = 6;
const DENSE_GRADIENT_HEIGHT = 268;

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

function buildDenseLegendTicks(entries: LegendEntry[], targetCount = DENSE_GRADIENT_LABEL_COUNT): LegendEntry[] {
  const displayed = entries.slice().reverse();
  if (displayed.length === 0) return [];

  const lastIndex = displayed.length - 1;
  const indices = Array.from({ length: targetCount }, (_, index) => {
    const ratio = targetCount === 1 ? 0 : index / (targetCount - 1);
    return Math.round(ratio * lastIndex);
  }).filter((value, index, arr) => index === 0 || value !== arr[index - 1]);

  return indices.map((index) => displayed[index]);
}

function DenseGradientLegend({ entries }: { entries: LegendEntry[] }) {
  const displayed = entries.slice().reverse();
  const ticks = buildDenseLegendTicks(entries);
  const stopCount = Math.max(displayed.length - 1, 1);
  const gradientStops = displayed
    .map((entry, index) => `${entry.color} ${(index / stopCount) * 100}%`)
    .join(", ");

  return (
    <div className="py-1.5">
      <div className="flex justify-start px-0.5">
        <div className="inline-grid grid-cols-[26px_auto] items-stretch gap-1.5">
          <div
            className="rounded-[14px] bg-black/14 p-[3px] ring-1 ring-inset ring-white/12 shadow-[inset_0_1px_0_rgba(255,255,255,0.06),0_6px_16px_rgba(0,0,0,0.18)]"
            style={{ height: DENSE_GRADIENT_HEIGHT }}
          >
            <div
              className="h-full w-full rounded-[11px] shadow-[inset_0_1px_0_rgba(255,255,255,0.16)]"
              style={{ backgroundImage: `linear-gradient(to bottom, ${gradientStops})` }}
            />
          </div>
          <div className="flex flex-col justify-between py-[2px]" style={{ height: DENSE_GRADIENT_HEIGHT }}>
            {ticks.map((entry, index) => (
              <div key={`${entry.value}-${index}`} className="flex items-center gap-1">
                <span className="h-px w-[7px] shrink-0 rounded-full bg-white/45" />
                <span className="font-mono text-[10px] font-semibold leading-none tabular-nums tracking-tight text-foreground/95 whitespace-nowrap">
                  {formatValue(entry.value)}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
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
      const slice = entries.slice(offset, offset + count);
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

  // Fallback: split sequence on zero-value delimiters in native order.
  // Reversing here flips group labels (rain↔frzr, snow↔sleet) when
  // sidecars don't provide ptype metadata.
  const displayed = entries.slice();
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
  containerRef?: Ref<HTMLDivElement>;
  showOpacityControl?: boolean;
};

export function MapLegend({
  legend,
  onOpacityChange,
  containerRef,
  showOpacityControl = true,
}: MapLegendProps) {
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
      <div
        ref={containerRef}
        className={cn("pointer-events-none fixed z-[55]", isSmallScreen ? "right-3 top-40" : "right-4 top-[4.35rem]")}
      >
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
  const showDenseLegend = !showPrecipPtypeRows && !showGroupedRadar && legend.entries.length > DENSE_LEGEND_THRESHOLD;

  return (
    <div
      ref={containerRef}
      className={cn(
        "fixed z-[55] flex flex-col max-h-[70vh] overflow-hidden rounded-xl glass bg-black/34 shadow-[0_6px_22px_rgba(0,0,0,0.3)] transition-all duration-200",
        showPrecipPtypeRows ? "w-[220px]" : showDenseLegend ? "w-[122px]" : "w-[156px]",
        isSmallScreen ? "right-3 top-40 max-w-[min(72vw,220px)]" : "right-4 top-[4.35rem]"
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
        className="flex w-full items-center justify-between gap-1.5 border-b border-border/25 px-1.5 py-1 text-left transition-all duration-150 hover:bg-secondary/25 active:bg-secondary/45"
        aria-expanded={!collapsed}
        aria-controls="legend-body"
      >
        <span className="block min-w-0 text-sm font-semibold tracking-tight text-foreground/95">
          {legend.units ? `${legend.title} (${legend.units})` : legend.title}
        </span>
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
            <div className={cn(showDenseLegend ? "" : "legend-scroll max-h-[45vh] space-y-px overflow-y-auto scroll-smooth")}>
              {showPrecipPtypeRows
                ? precipPtypeRows.map((row, rowIndex) => (
                    <div
                      key={`precip-row-${row.label}-${rowIndex}`}
                      className={cn(rowIndex > 0 ? "mt-2 border-t border-border/20 pt-2" : "")}
                    >
                      <div className="mb-1 flex items-center justify-between gap-2 px-0.5">
                        <span className="text-[9px] font-medium uppercase tracking-wide text-foreground/62">
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
                      <div className="mb-1 px-0.5 text-[9px] font-medium uppercase tracking-wide text-foreground/62">
                        {group.label}
                      </div>
                      {group.entries.map((entry, index) => (
                        <div
                          key={`${entry.value}-${entry.color}-${groupIndex}-${index}`}
                          className="flex items-center gap-1.5 rounded-[2px] px-0.5 py-0.5 transition-colors duration-150"
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
                : showDenseLegend
                ? <DenseGradientLegend entries={legend.entries} />
                : legend.entries.slice().reverse().map((entry, index) => (
                    <div
                      key={`${entry.value}-${entry.color}-${index}`}
                      className="flex items-center gap-1.5 rounded-[2px] px-0.5 py-0.5 transition-colors duration-150"
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

            {showOpacityControl ? (
              <div className="border-t border-border/30 pt-1.5">
                <div className="mb-1 flex items-center justify-between">
                  <span className="text-[10px] font-medium uppercase tracking-wider text-foreground/65">
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
                  className="w-full transition-opacity duration-150 [&>*:first-child]:h-2.5 [&>*:first-child]:bg-secondary/55 [&>*:nth-child(2)]:h-[18px] [&>*:nth-child(2)]:w-[18px]"
                />
              </div>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}

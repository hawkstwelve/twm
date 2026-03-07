import { useState, type ComponentType, type ReactNode } from "react";
import {
  Boxes,
  CalendarClock,
  ChevronDown,
  Layers,
  MapPin,
  Send,
  SlidersHorizontal,
} from "lucide-react";

import { cn } from "@/lib/utils";
import { Slider } from "@/components/ui/slider";
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
} from "@/components/ui/select";

type Option = {
  value: string;
  label: string;
};

type VariableOption = Option & {
  group: string | null;
};

type WeatherToolbarProps = {
  region: string;
  onRegionChange: (value: string) => void;
  model: string;
  onModelChange: (value: string) => void;
  run: string;
  onRunChange: (value: string) => void;
  variable: string;
  onVariableChange: (value: string) => void;
  regions: Option[];
  models: Option[];
  runs: Option[];
  variables: VariableOption[];
  disabled?: boolean;
  pointLabelsEnabled: boolean;
  onPointLabelsEnabledChange: (next: boolean) => void;
  legendVisible: boolean;
  onLegendVisibleChange: (next: boolean) => void;
  opacity: number;
  onOpacityChange: (next: number) => void;
  onPostToTwf?: () => void;
};

function ToolbarSelect(props: {
  label: string;
  icon: ComponentType<{ className?: string }>;
  value: string;
  onValueChange: (value: string) => void;
  options: (Option | VariableOption)[];
  disabled?: boolean;
  placeholder: string;
  grouped?: boolean;
  triggerClassName?: string;
}) {
  const { label, icon: Icon, value, onValueChange, options, disabled, placeholder, grouped, triggerClassName } = props;
  const selectedLabel = options.find((opt) => opt.value === value)?.label ?? placeholder;

  let content: ReactNode;
  if (grouped) {
    const GROUP_ORDER = ["Radar & Precipitation Type", "Temperature", "Precipitation", "Wind"];
    const groups = new Map<string, Option[]>();
    const ungrouped: Option[] = [];
    for (const opt of options) {
      const g = "group" in opt && typeof opt.group === "string" ? opt.group : null;
      if (g) {
        let list = groups.get(g);
        if (!list) {
          list = [];
          groups.set(g, list);
        }
        list.push(opt);
      } else {
        ungrouped.push(opt);
      }
    }
    const orderedGroups = GROUP_ORDER.filter((g) => groups.has(g));
    for (const g of groups.keys()) {
      if (!orderedGroups.includes(g)) {
        orderedGroups.push(g);
      }
    }
    content = (
      <>
        {orderedGroups.map((g) => (
          <SelectGroup key={g}>
            <SelectLabel className="px-2 pt-1.5 pb-0.5 text-[10px] font-semibold uppercase tracking-wider text-white/60">
              {g}
            </SelectLabel>
            {groups.get(g)!.map((opt) => (
              <SelectItem key={opt.value} value={opt.value} className="text-xs font-medium">
                {opt.label}
              </SelectItem>
            ))}
          </SelectGroup>
        ))}
        {ungrouped.map((opt) => (
          <SelectItem key={opt.value} value={opt.value} className="text-xs font-medium">
            {opt.label}
          </SelectItem>
        ))}
      </>
    );
  } else {
    content = options.map((opt) => (
      <SelectItem key={opt.value} value={opt.value} className="text-xs font-medium">
        {opt.label}
      </SelectItem>
    ));
  }

  return (
    <div className="flex flex-col gap-1">
      <span className="flex items-center gap-1 text-[10px] font-medium uppercase tracking-wider text-muted-foreground/80">
        <Icon className="h-3 w-3 opacity-70" />
        {label}
      </span>
      <Select value={value} onValueChange={onValueChange} disabled={disabled || options.length === 0}>
        <SelectTrigger
          className={cn(
            "h-8 w-full border-border/50 bg-secondary/40 text-xs font-medium text-foreground shadow-sm transition-all duration-150 hover:border-border hover:bg-secondary/60 focus:border-primary/50 focus:ring-1 focus:ring-primary/30 [&>span]:line-clamp-none",
            triggerClassName
          )}
        >
          <span className="whitespace-nowrap pr-1">{selectedLabel}</span>
        </SelectTrigger>
        <SelectContent>{content}</SelectContent>
      </Select>
    </div>
  );
}

function DisplayToggle(props: {
  label: string;
  description?: string;
  checked: boolean;
  onToggle: () => void;
}) {
  const { label, description, checked, onToggle } = props;

  return (
    <button
      type="button"
      onClick={onToggle}
      aria-pressed={checked}
      className={cn(
        "flex w-full items-center justify-between gap-3 rounded-lg border px-3 py-2 text-left transition-all duration-150",
        checked
          ? "border-white/20 bg-white/12 text-white hover:bg-white/18"
          : "border-white/10 bg-black/18 text-white/78 hover:bg-black/28"
      )}
    >
      <div className="min-w-0">
        <div className="text-sm font-semibold">{label}</div>
        {description ? <div className="text-[11px] text-white/58">{description}</div> : null}
      </div>
      <div
        className={cn(
          "relative h-5 w-9 shrink-0 rounded-full transition-colors duration-150",
          checked ? "bg-[#354d42]" : "bg-white/18"
        )}
      >
        <span
          className={cn(
            "absolute top-0.5 h-4 w-4 rounded-full bg-white shadow-sm transition-transform duration-150",
            checked ? "translate-x-[18px]" : "translate-x-0.5"
          )}
        />
      </div>
    </button>
  );
}

function ShareButton({ onClick, compact = false }: { onClick: () => void; compact?: boolean }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-md border border-emerald-300/25 bg-[linear-gradient(to_top_right,#1f342f_0%,#526d5c_100%)] font-semibold text-emerald-50 shadow-sm transition-all duration-150 hover:brightness-110 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-emerald-300/45",
        compact ? "h-9 px-3 text-xs" : "h-8 px-2.5 text-xs"
      )}
      title="Share"
      aria-label="Share"
    >
      <Send className={cn(compact ? "h-4 w-4" : "h-3.5 w-3.5")} />
      Share
    </button>
  );
}

export function WeatherToolbar(props: WeatherToolbarProps) {
  const {
    region,
    onRegionChange,
    model,
    onModelChange,
    run,
    onRunChange,
    variable,
    onVariableChange,
    regions,
    models,
    runs,
    variables,
    disabled = false,
    pointLabelsEnabled,
    onPointLabelsEnabledChange,
    legendVisible,
    onLegendVisibleChange,
    opacity,
    onOpacityChange,
    onPostToTwf,
  } = props;
  const [mobilePanelOpen, setMobilePanelOpen] = useState(false);

  const selectedModelLabel = models.find((opt) => opt.value === model)?.label ?? "Model";
  const selectedVariableLabel = variables.find((opt) => opt.value === variable)?.label ?? "Variable";
  const selectedRunLabel = (runs.find((opt) => opt.value === run)?.label ?? "Run").replace(
    /^Latest\s*\((.*)\)$/,
    "$1"
  );

  return (
    <header role="toolbar" aria-label="Weather model controls" className="fixed top-[4.35rem] z-50 w-full px-3 sm:px-4">
      <div className="hidden sm:block">
        <div className="flex items-start">
          <div className="glass-strong inline-flex max-w-[calc(100vw-9rem)] items-end gap-2.5 rounded-2xl border border-white/12 px-4 py-3 shadow-[0_18px_40px_rgba(0,0,0,0.34)]">
            <ToolbarSelect
              label="Region"
              icon={MapPin}
              value={region}
              onValueChange={onRegionChange}
              options={regions}
              disabled={disabled}
              placeholder="Region"
              triggerClassName="min-w-[160px]"
            />

            <ToolbarSelect
              label="Model"
              icon={Boxes}
              value={model}
              onValueChange={onModelChange}
              options={models}
              disabled={disabled}
              placeholder="Model"
              triggerClassName="min-w-[160px]"
            />

            <ToolbarSelect
              label="Run"
              icon={CalendarClock}
              value={run}
              onValueChange={onRunChange}
              options={runs}
              disabled={disabled}
              placeholder="Run"
              triggerClassName="min-w-[160px]"
            />

            <ToolbarSelect
              label="Variable"
              icon={Layers}
              value={variable}
              onValueChange={onVariableChange}
              options={variables}
              disabled={disabled}
              placeholder="Variable"
              grouped
              triggerClassName="min-w-[224px] max-w-[260px]"
            />
          </div>
        </div>
      </div>

      <div className="glass-strong relative rounded-2xl border border-white/12 px-3 py-2.5 pb-2 shadow-[0_18px_40px_rgba(0,0,0,0.34)] sm:hidden">
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setMobilePanelOpen((value) => !value)}
            className={cn(
              "inline-flex h-9 items-center gap-2 rounded-md border px-3 text-xs font-semibold transition-all duration-150",
              mobilePanelOpen
                ? "border-white/20 bg-white/14 text-white"
                : "border-white/10 bg-black/25 text-white/88 hover:bg-black/35"
            )}
            aria-expanded={mobilePanelOpen}
            aria-controls="mobile-layers-panel"
          >
            <SlidersHorizontal className="h-4 w-4" />
            Layers
            <ChevronDown className={cn("h-4 w-4 transition-transform duration-150", mobilePanelOpen ? "rotate-180" : "")} />
          </button>
        </div>

        <div className="flex items-center gap-2 pt-2 pr-24 text-[11px]">
          <span className="rounded-full border border-white/10 bg-white/8 px-2 py-1 font-medium text-white/68">
            {selectedRunLabel}
          </span>
          <span className="rounded-full border border-white/10 bg-white/8 px-2 py-1 font-medium text-white/82">
            {selectedModelLabel}
          </span>
          <span className="truncate rounded-full border border-white/10 bg-white/8 px-2 py-1 font-medium text-white/74">
            {selectedVariableLabel}
          </span>
        </div>

        {onPostToTwf ? (
          <div className="absolute right-3 bottom-2">
            <ShareButton onClick={onPostToTwf} compact />
          </div>
        ) : null}

        {mobilePanelOpen ? (
          <div
            id="mobile-layers-panel"
            className="glass-strong mt-3 mb-3 rounded-2xl border border-white/12 px-3 py-3 shadow-[0_16px_48px_rgba(0,0,0,0.45)]"
          >
            <div className="grid grid-cols-1 gap-3">
              <ToolbarSelect
                label="Region"
                icon={MapPin}
                value={region}
                onValueChange={onRegionChange}
                options={regions}
                disabled={disabled}
                placeholder="Region"
              />

              <ToolbarSelect
                label="Model"
                icon={Boxes}
                value={model}
                onValueChange={onModelChange}
                options={models}
                disabled={disabled}
                placeholder="Model"
              />

              <ToolbarSelect
                label="Run"
                icon={CalendarClock}
                value={run}
                onValueChange={onRunChange}
                options={runs}
                disabled={disabled}
                placeholder="Run"
              />

              <ToolbarSelect
                label="Variable"
                icon={Layers}
                value={variable}
                onValueChange={onVariableChange}
                options={variables}
                disabled={disabled}
                placeholder="Variable"
                grouped
              />
            </div>

            <div className="mt-4 border-t border-white/10 pt-3">
              <div className="mb-2 text-[10px] font-semibold uppercase tracking-[0.18em] text-white/48">
                Map Display
              </div>
              <div className="space-y-2">
                <DisplayToggle
                  label="City Labels"
                  checked={pointLabelsEnabled}
                  onToggle={() => onPointLabelsEnabledChange(!pointLabelsEnabled)}
                />
                <DisplayToggle
                  label="Legend"
                  checked={legendVisible}
                  onToggle={() => onLegendVisibleChange(!legendVisible)}
                />
                <div className="rounded-lg border border-white/10 bg-black/18 px-3 py-2">
                  <div className="mb-1 flex items-center justify-between">
                    <span className="text-[11px] font-semibold text-white">Opacity</span>
                    <span className="font-mono text-[10px] text-white/62">{Math.round(opacity * 100)}%</span>
                  </div>
                  <Slider
                    value={[Math.round(opacity * 100)]}
                    onValueChange={([value]) => onOpacityChange((value ?? 100) / 100)}
                    min={0}
                    max={100}
                    step={1}
                    className="w-full [&>*:first-child]:h-2 [&>*:first-child]:bg-secondary/55 [&>*:nth-child(2)]:h-4 [&>*:nth-child(2)]:w-4"
                  />
                </div>
              </div>
            </div>

            <div className="mt-4 border-t border-white/10 pt-3 text-[10px] leading-relaxed text-white/52">
              Maps:{" "}
              <a href="https://www.maplibre.org/" target="_blank" rel="noreferrer" className="underline underline-offset-2">
                MapLibre
              </a>
              {" "}|
              {" "}
              <a
                href="https://www.openstreetmap.org/copyright"
                target="_blank"
                rel="noreferrer"
                className="underline underline-offset-2"
              >
                OSM
              </a>
              {" "}|
              {" "}
              <a href="https://carto.com/attributions" target="_blank" rel="noreferrer" className="underline underline-offset-2">
                CARTO
              </a>
            </div>
          </div>
        ) : null}
      </div>
    </header>
  );
}

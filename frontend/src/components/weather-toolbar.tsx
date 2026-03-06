import type { ComponentType } from "react";
import { MapPin, Layers, CalendarClock, Boxes, Send } from "lucide-react";
import { cn } from "@/lib/utils";
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
}) {
  const { label, icon: Icon, value, onValueChange, options, disabled, placeholder, grouped } = props;
  const selectedLabel = options.find((opt) => opt.value === value)?.label ?? placeholder;

  // Build grouped content when the `grouped` flag is set and options have a group field.
  let content: React.ReactNode;
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
    // Include any groups not in GROUP_ORDER (future-proof).
    for (const g of groups.keys()) {
      if (!orderedGroups.includes(g)) {
        orderedGroups.push(g);
      }
    }
    content = (
      <>
        {orderedGroups.map((g) => (
          <SelectGroup key={g}>
            <SelectLabel className="text-[10px] font-semibold uppercase tracking-wider text-white/60 px-2 pt-1.5 pb-0.5">
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
        <SelectTrigger className="h-8 w-fit min-w-[160px] border-border/50 bg-secondary/40 text-xs font-medium text-foreground shadow-sm transition-all duration-150 hover:border-border hover:bg-secondary/60 focus:border-primary/50 focus:ring-1 focus:ring-primary/30 [&>span]:line-clamp-none">
          <span className="whitespace-nowrap pr-1">{selectedLabel}</span>
        </SelectTrigger>
        <SelectContent>
          {content}
        </SelectContent>
      </Select>
    </div>
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
    onPostToTwf,
  } = props;

  return (
    <header
      role="toolbar"
      aria-label="Weather model controls"
      className="glass-strong fixed top-14 z-50 w-full border-t-0 border-x-0 border-b-white/15 bg-black/60 [background-image:linear-gradient(to_bottom,rgba(0,0,0,0.72),rgba(0,0,0,0.56))]"
    >
      <div className="flex flex-wrap items-end gap-2.5 px-4 py-2.5">
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

        <div className="flex flex-col gap-1">
          <span className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground/80">
            Overlay
          </span>
          <button
            type="button"
            onClick={() => onPointLabelsEnabledChange(!pointLabelsEnabled)}
            aria-pressed={pointLabelsEnabled}
            className={cn(
              "inline-flex h-8 items-center gap-1.5 rounded-md border px-2.5 text-xs font-semibold shadow-sm transition-all duration-150 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-primary/35",
              pointLabelsEnabled
                ? "border-white/20 bg-white/14 text-white hover:bg-white/20"
                : "border-white/10 bg-black/25 text-white/70 hover:bg-black/35"
            )}
            title={pointLabelsEnabled ? "Hide point labels" : "Show point labels"}
          >
            <MapPin className="h-3.5 w-3.5" />
            Point Labels
          </button>
        </div>

        {onPostToTwf ? (
          <div className="ml-auto">
            <button
              type="button"
              onClick={onPostToTwf}
              className="inline-flex h-8 items-center gap-1.5 rounded-md border border-emerald-300/25 bg-[linear-gradient(to_top_right,#1f342f_0%,#526d5c_100%)] px-2.5 text-xs font-semibold text-emerald-50 shadow-sm transition-all duration-150 hover:brightness-110 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-emerald-300/45"
              title="Share"
              aria-label="Share"
            >
              <Send className="h-3.5 w-3.5" />
              Share
            </button>
          </div>
        ) : null}
      </div>
    </header>
  );
}

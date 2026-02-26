import type { ComponentType } from "react";
import { Map as MapIcon, MapPin, Moon, Sun, Layers, CalendarClock, Boxes } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
} from "@/components/ui/select";

type Option = {
  value: string;
  label: string;
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
  variables: Option[];
  basemapMode: "light" | "dark";
  onBasemapModeChange: (value: "light" | "dark") => void;
  disabled?: boolean;
};

function ToolbarSelect(props: {
  label: string;
  icon: ComponentType<{ className?: string }>;
  value: string;
  onValueChange: (value: string) => void;
  options: Option[];
  disabled?: boolean;
  placeholder: string;
}) {
  const { label, icon: Icon, value, onValueChange, options, disabled, placeholder } = props;
  const selectedLabel = options.find((opt) => opt.value === value)?.label ?? placeholder;

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
          {options.map((opt) => (
            <SelectItem key={opt.value} value={opt.value} className="text-xs font-medium">
              {opt.label}
            </SelectItem>
          ))}
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
    basemapMode,
    onBasemapModeChange,
    disabled = false,
  } = props;

  return (
    <header
      role="toolbar"
      aria-label="Weather model controls"
      className="sticky top-0 z-50 w-full border-b border-border/40 bg-[hsl(var(--toolbar))]/95 shadow-sm backdrop-blur-md"
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
        />

        <div className="ml-auto flex flex-col gap-1">
          <span className="flex items-center gap-1 text-[10px] font-medium uppercase tracking-wider text-muted-foreground/80">
            <MapIcon className="h-3 w-3 opacity-70" />
            Basemap
          </span>
          <Button
            type="button"
            variant="secondary"
            size="sm"
            disabled={disabled}
            aria-pressed={basemapMode === "dark"}
            onClick={() => onBasemapModeChange(basemapMode === "dark" ? "light" : "dark")}
            className="h-8 border border-border/50 bg-secondary/40 px-2.5 text-xs font-medium text-foreground shadow-sm transition-all duration-150 hover:border-border hover:bg-secondary/60"
          >
            {basemapMode === "dark" ? <Sun className="h-3.5 w-3.5" /> : <Moon className="h-3.5 w-3.5" />}
            <span>{basemapMode === "dark" ? "Dark" : "Light"}</span>
          </Button>
        </div>
      </div>
    </header>
  );
}

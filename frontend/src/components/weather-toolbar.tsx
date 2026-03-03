import type { ComponentType } from "react";
import { MapPin, Layers, CalendarClock, Boxes, Link as LinkIcon } from "lucide-react";
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
  disabled?: boolean;
  onCopyLink?: () => void;
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
    disabled = false,
    onCopyLink,
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
        />

        {onCopyLink ? (
          <div className="ml-auto flex flex-col gap-1">
            <span className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground/80">
              Share
            </span>
            <button
              type="button"
              onClick={onCopyLink}
              className="inline-flex h-8 items-center gap-1.5 rounded-md border border-border/50 bg-secondary/45 px-2.5 text-xs font-medium text-foreground shadow-sm transition-all duration-150 hover:border-border hover:bg-secondary/65 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-primary/35"
              title="Copy link"
              aria-label="Copy link"
            >
              <LinkIcon className="h-3.5 w-3.5" />
              Copy link
            </button>
          </div>
        ) : null}
      </div>
    </header>
  );
}

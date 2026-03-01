import { useEffect, useMemo, useRef, useState } from "react";
import { AlertCircle, Clock, Pause, Play } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Slider } from "@/components/ui/slider";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

type BottomForecastControlsProps = {
  forecastHour: number;
  availableFrames: number[];
  onForecastHourChange: (fh: number) => void;
  onScrubStateChange?: (isScrubbing: boolean) => void;
  isPlaying: boolean;
  setIsPlaying: (value: boolean) => void;
  runDateTimeISO: string | null;
  disabled: boolean;
  playDisabled?: boolean;
  transientStatus?: string | null;
};

function formatValidTime(runDateISO: string | null, forecastHour: number): {
  primary: string;
  secondary: string;
} | null {
  if (!runDateISO) return null;

  try {
    const runDate = new Date(runDateISO);
    if (Number.isNaN(runDate.getTime())) return null;

    const validDate = new Date(runDate.getTime() + forecastHour * 60 * 60 * 1000);

    const primary = new Intl.DateTimeFormat("en-US", {
      weekday: "short",
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      timeZoneName: "short",
    }).format(validDate);

    const secondary = `FH ${forecastHour}`;

    return { primary, secondary };
  } catch {
    return null;
  }
}

export function BottomForecastControls({
  forecastHour,
  availableFrames,
  onForecastHourChange,
  onScrubStateChange,
  isPlaying,
  setIsPlaying,
  runDateTimeISO,
  disabled,
  playDisabled = false,
  transientStatus,
}: BottomForecastControlsProps) {
  const DRAG_UPDATE_MS = 80;
  const [previewHour, setPreviewHour] = useState<number | null>(null);
  const [isScrubbing, setIsScrubbing] = useState(false);
  const lastDragEmitAtRef = useRef(0);
  const lastSentHourRef = useRef<number | null>(null);

  const validTime = useMemo(
    () => formatValidTime(runDateTimeISO, previewHour ?? forecastHour),
    [runDateTimeISO, forecastHour, previewHour]
  );

  const hasFrames = availableFrames.length > 0;
  const effectiveHour = previewHour ?? forecastHour;
  const sliderIndex = Math.max(0, availableFrames.indexOf(effectiveHour));

  useEffect(() => {
    setPreviewHour(null);
  }, [forecastHour]);

  useEffect(() => {
    onScrubStateChange?.(isScrubbing);
  }, [isScrubbing, onScrubStateChange]);

  useEffect(() => {
    if (isPlaying && isScrubbing) {
      setIsScrubbing(false);
    }
  }, [isPlaying, isScrubbing]);

  useEffect(() => {
    lastSentHourRef.current = forecastHour;
  }, [forecastHour]);

  const emitForecastHour = (next: number, force: boolean) => {
    const now = Date.now();
    const shouldEmit =
      force ||
      (lastSentHourRef.current !== next && now - lastDragEmitAtRef.current >= DRAG_UPDATE_MS);
    if (!shouldEmit) {
      return;
    }
    lastDragEmitAtRef.current = now;
    lastSentHourRef.current = next;
    onForecastHourChange(next);
  };

  return (
    <TooltipProvider delayDuration={300}>
      <div className="pointer-events-none fixed inset-x-0 bottom-0 z-40 flex items-end justify-center px-4 pb-5 sm:pb-6">
        <div className="pointer-events-auto flex w-full max-w-3xl flex-col gap-3 rounded-2xl glass-strong px-5 py-4 sm:flex-row sm:items-center sm:gap-5">
          <div className="flex shrink-0 items-center gap-2">
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant={isPlaying ? "default" : "outline"}
                  size="sm"
                  onClick={() => setIsPlaying(!isPlaying)}
                  disabled={disabled || !hasFrames || playDisabled}
                  aria-label={isPlaying ? "Pause animation" : "Play animation"}
                  className="h-10 w-10 p-0 transition-all duration-150 hover:scale-105 active:scale-95"
                >
                  {isPlaying ? (
                    <Pause className="h-4 w-4" />
                  ) : (
                    <Play className="h-4 w-4 translate-x-px" />
                  )}
                </Button>
              </TooltipTrigger>
              <TooltipContent side="top">
                {isPlaying ? "Pause" : "Play"} animation
              </TooltipContent>
            </Tooltip>
          </div>

          <div className="flex flex-1 flex-col gap-1.5">
            <div className="flex items-center justify-between">
              <span className="flex items-center gap-1.5 text-[10px] font-medium uppercase tracking-wider text-foreground/65">
                <Clock className="h-3 w-3" />
                Forecast Hour
              </span>
              <span className="font-mono text-xs font-semibold tabular-nums tracking-tight text-foreground/95 transition-all duration-150">
                {forecastHour}h
              </span>
            </div>
            <Slider
              value={[sliderIndex]}
              onValueChange={([value]) => {
                const next = availableFrames[Math.round(value ?? 0)];
                if (Number.isFinite(next)) {
                  if (!isScrubbing) {
                    setIsScrubbing(true);
                  }
                  setPreviewHour(next);
                  emitForecastHour(next, false);
                }
              }}
              onValueCommit={([value]) => {
                const next = availableFrames[Math.round(value ?? 0)];
                if (Number.isFinite(next)) {
                  setPreviewHour(null);
                  setIsScrubbing(false);
                  emitForecastHour(next, true);
                }
              }}
              min={0}
              max={Math.max(0, availableFrames.length - 1)}
              step={1}
              disabled={disabled || isPlaying || !hasFrames}
              className="w-full transition-opacity duration-150 [&>*:first-child]:h-2.5 [&>*:first-child]:bg-secondary/55 [&>*:nth-child(2)]:h-[22px] [&>*:nth-child(2)]:w-[22px]"
            />
          </div>

          <div className="flex shrink-0 flex-col items-end gap-1 border-l border-border/30 pl-5 sm:min-w-[220px]">
            {transientStatus ? (
              <div className="flex items-center gap-1.5 rounded-md border border-border/40 bg-background/40 px-2 py-1 text-[10px] text-foreground/90">
                <AlertCircle className="h-3 w-3" />
                {transientStatus}
              </div>
            ) : null}
            {validTime ? (
              <>
                <span className="text-sm font-semibold leading-tight tracking-tight text-foreground transition-all duration-200">
                  {validTime.primary}
                </span>
                <span className="text-[10px] font-medium uppercase tracking-wider text-foreground/65 transition-all duration-200">
                  {validTime.secondary}
                </span>
              </>
            ) : (
              <div className="flex items-center gap-1.5">
                <AlertCircle className="h-3 w-3 text-muted-foreground" />
                <span className="text-[10px] text-muted-foreground">Valid time unavailable</span>
              </div>
            )}
          </div>
        </div>
      </div>
    </TooltipProvider>
  );
}

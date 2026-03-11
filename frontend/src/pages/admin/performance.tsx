import type { ComponentType, ReactNode } from "react";
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { Activity, AlertCircle, Gauge, Globe, Layers, PauseCircle, RefreshCcw, TimerReset, X, Zap } from "lucide-react";

import {
  fetchAdminPerfBreakdown,
  fetchAdminPerfSummary,
  fetchAdminPerfTimeseries,
  fetchTwfStatus,
  type PerfBreakdownItem,
  type PerfMetricSummary,
  type PerfTimeseriesPoint,
  type TwfStatus,
} from "@/lib/admin-api";

type WindowValue = "24h" | "7d" | "30d";
type DeviceValue = "all" | "desktop" | "mobile";
type MetricKey =
  | "frame_change"
  | "loop_start"
  | "scrub_latency"
  | "animation_stall"
  | "viewer_first_frame"
  | "variable_switch"
  | "tile_fetch";

function formatMs(value: number | null | undefined): string {
  if (!Number.isFinite(value)) {
    return "—";
  }
  return `${Math.round(Number(value))} ms`;
}

function formatCount(value: number | null | undefined): string {
  if (!Number.isFinite(value)) {
    return "0";
  }
  return new Intl.NumberFormat("en-US").format(Number(value));
}

type MetricStatusTone = "good" | "warning" | "bad" | "unknown";

function getMetricStatus(metric?: PerfMetricSummary): {
  tone: MetricStatusTone;
  label: string;
  accentClassName: string;
  iconClassName: string;
  badgeClassName: string;
} {
  const target = metric?.target_ms ?? null;
  const p95 = metric?.p95_ms ?? null;

  if (target === null || p95 === null || target <= 0) {
    return {
      tone: "unknown",
      label: "Target unavailable",
      accentClassName: "text-[#9dd5bf]",
      iconClassName: "border-white/10 bg-white/[0.05] text-white/76",
      badgeClassName: "border-white/10 bg-white/[0.04] text-white/54",
    };
  }

  const ratio = p95 / target;

  if (ratio <= 0.8) {
    return {
      tone: "good",
      label: `Well under target ${formatMs(target)}`,
      accentClassName: "text-emerald-300",
      iconClassName: "border-emerald-400/25 bg-emerald-500/12 text-emerald-100",
      badgeClassName: "border-emerald-400/25 bg-emerald-500/12 text-emerald-100",
    };
  }

  if (ratio <= 1) {
    return {
      tone: "warning",
      label: `Near target ${formatMs(target)}`,
      accentClassName: "text-amber-300",
      iconClassName: "border-amber-400/25 bg-amber-500/12 text-amber-100",
      badgeClassName: "border-amber-400/25 bg-amber-500/12 text-amber-100",
    };
  }

  return {
    tone: "bad",
    label: `Over target ${formatMs(target)}`,
    accentClassName: "text-rose-300",
    iconClassName: "border-rose-400/25 bg-rose-500/12 text-rose-100",
    badgeClassName: "border-rose-400/25 bg-rose-500/12 text-rose-100",
  };
}

function MetricCard(props: {
  title: string;
  icon: ComponentType<{ className?: string }>;
  metric?: PerfMetricSummary;
  onClick?: () => void;
}) {
  const { title, icon: Icon, metric, onClick } = props;
  const status = getMetricStatus(metric);
  const p95 = metric?.p95_ms ?? null;
  const cardClassName =
    status.tone === "good"
      ? "border-emerald-400/20 bg-[linear-gradient(180deg,rgba(16,185,129,0.09),rgba(0,0,0,0.28))]"
      : status.tone === "warning"
        ? "border-amber-400/20 bg-[linear-gradient(180deg,rgba(245,158,11,0.09),rgba(0,0,0,0.28))]"
        : status.tone === "bad"
          ? "border-rose-400/20 bg-[linear-gradient(180deg,rgba(244,63,94,0.1),rgba(0,0,0,0.28))]"
          : "border-white/12 bg-black/28";
  const content = (
    <>
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-sm font-semibold text-white">{title}</div>
          <div className="mt-1 text-xs uppercase tracking-[0.22em] text-white/42">p95</div>
        </div>
        <div className={`rounded-xl border p-2 ${status.iconClassName}`}>
          <Icon className="h-4 w-4" />
        </div>
      </div>

      <div className={`mt-5 text-[2.2rem] font-semibold tracking-tight ${status.accentClassName}`}>{formatMs(p95)}</div>
      <div className="mt-2 flex items-center gap-2 text-sm text-white/62">
        <span className={status.accentClassName}>p50 {formatMs(metric?.p50_ms)}</span>
        <span className="text-white/24">•</span>
        <span>{formatCount(metric?.count)} samples</span>
      </div>
      <div className={`mt-4 inline-flex rounded-full border px-3 py-1 text-[11px] font-medium ${status.badgeClassName}`}>
        {status.label}
      </div>
      {onClick ? (
        <div className="mt-4 text-xs font-medium uppercase tracking-[0.18em] text-white/48">
          Open metric details
        </div>
      ) : null}
    </>
  );

  if (onClick) {
    return (
      <button
        type="button"
        onClick={onClick}
        className={`w-full rounded-[24px] border p-5 text-left shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl transition hover:-translate-y-0.5 hover:border-white/20 hover:bg-white/[0.05] ${cardClassName}`}
        aria-label={`Open ${title} details`}
      >
        {content}
      </button>
    );
  }

  return <section className={`rounded-[24px] border p-5 shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl ${cardClassName}`}>{content}</section>;
}

function TrendChart(props: {
  title: string;
  subtitle: string;
  points: PerfTimeseriesPoint[];
  lineColor: string;
}) {
  const { title, subtitle, points, lineColor } = props;
  const values = points.map((point) => point.p95_ms).filter((value): value is number => Number.isFinite(value));

  if (values.length === 0) {
    return (
      <section className="rounded-[28px] border border-white/12 bg-black/28 p-5 shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl">
        <div className="text-lg font-semibold text-white">{title}</div>
        <p className="mt-1 text-sm text-white/58">{subtitle}</p>
        <div className="mt-8 rounded-2xl border border-dashed border-white/10 bg-white/[0.03] px-4 py-10 text-sm text-white/48">
          No data yet for this window.
        </div>
      </section>
    );
  }

  const width = 680;
  const height = 220;
  const paddingX = 24;
  const paddingY = 20;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(1, max - min);
  const path = points
    .map((point, index) => {
      const x = paddingX + (index * (width - paddingX * 2)) / Math.max(1, points.length - 1);
      const rawValue = Number(point.p95_ms ?? min);
      const y = height - paddingY - ((rawValue - min) / span) * (height - paddingY * 2);
      return `${index === 0 ? "M" : "L"} ${x} ${y}`;
    })
    .join(" ");
  const areaPath = `${path} L ${width - paddingX} ${height - paddingY} L ${paddingX} ${height - paddingY} Z`;

  return (
    <section className="rounded-[28px] border border-white/12 bg-black/28 p-5 shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="text-lg font-semibold text-white">{title}</div>
          <p className="mt-1 text-sm text-white/58">{subtitle}</p>
        </div>
        <div className="rounded-full border border-white/10 bg-white/[0.04] px-3 py-1 text-xs font-medium text-white/60">
          p95 trend
        </div>
      </div>

      <div className="mt-5 overflow-hidden rounded-[22px] border border-white/10 bg-[linear-gradient(180deg,rgba(255,255,255,0.04),rgba(255,255,255,0.02))] p-4">
        <svg viewBox={`0 0 ${width} ${height}`} className="h-[220px] w-full">
          <defs>
            <linearGradient id={`area-${title.replace(/\s+/g, "-").toLowerCase()}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={lineColor} stopOpacity="0.34" />
              <stop offset="100%" stopColor={lineColor} stopOpacity="0.04" />
            </linearGradient>
          </defs>

          {[0, 1, 2, 3].map((row) => {
            const y = paddingY + (row * (height - paddingY * 2)) / 3;
            return <line key={row} x1={paddingX} y1={y} x2={width - paddingX} y2={y} stroke="rgba(255,255,255,0.08)" />;
          })}

          <path d={areaPath} fill={`url(#area-${title.replace(/\s+/g, "-").toLowerCase()})`} />
          <path d={path} fill="none" stroke={lineColor} strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />

          {points.map((point, index) => {
            const x = paddingX + (index * (width - paddingX * 2)) / Math.max(1, points.length - 1);
            const rawValue = Number(point.p95_ms ?? min);
            const y = height - paddingY - ((rawValue - min) / span) * (height - paddingY * 2);
            return <circle key={point.bucket_start} cx={x} cy={y} r="3.5" fill={lineColor} />;
          })}
        </svg>

        <div className="mt-4 flex items-center justify-between gap-2 text-[11px] uppercase tracking-[0.22em] text-white/42">
          <span>{points[0]?.bucket_start?.slice(0, 10) ?? ""}</span>
          <span>{points[points.length - 1]?.bucket_start?.slice(0, 10) ?? ""}</span>
        </div>
      </div>
    </section>
  );
}

function BreakdownList(props: { title: string; subtitle: string; items: PerfBreakdownItem[] }) {
  const { title, subtitle, items } = props;

  return (
    <section className="rounded-[28px] border border-white/12 bg-black/28 p-5 shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl">
      <div className="text-lg font-semibold text-white">{title}</div>
      <p className="mt-1 text-sm text-white/58">{subtitle}</p>

      <div className="mt-5 space-y-3">
        {items.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-white/10 bg-white/[0.03] px-4 py-6 text-sm text-white/48">
            No data yet for this breakdown.
          </div>
        ) : (
          items.map((item) => {
            const status = getMetricStatus(item);
            return (
              <div key={item.key} className="rounded-2xl border border-white/10 bg-white/[0.035] px-4 py-3">
                <div className="flex items-center justify-between gap-3">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-semibold text-white">{item.key}</div>
                    <div className="mt-1 text-xs text-white/48">{formatCount(item.count)} samples</div>
                  </div>
                  <div className="text-right">
                    <div className={`text-sm font-semibold ${status.accentClassName}`}>{formatMs(item.p95_ms)}</div>
                    <div className={`mt-1 text-xs ${status.accentClassName}`}>p50 {formatMs(item.p50_ms)}</div>
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>
    </section>
  );
}

function MetricDetailDialog(props: {
  open: boolean;
  title: string;
  description: string;
  icon: ComponentType<{ className?: string }>;
  metric?: PerfMetricSummary;
  onClose: () => void;
  children?: ReactNode;
}) {
  const { open, title, description, icon, metric, onClose, children } = props;

  if (!open) {
    return null;
  }

  return (
    <div className="fixed inset-0 z-50">
      <button
        type="button"
        aria-label="Close metric details"
        className="absolute inset-0 bg-black/82 backdrop-blur-md"
        onClick={onClose}
      />
      <div className="relative z-10 flex h-full items-center justify-center p-4 md:p-6">
        <section
          role="dialog"
          aria-modal="true"
          aria-label={`${title} details`}
          className="flex max-h-[90vh] w-full max-w-6xl flex-col overflow-hidden rounded-[32px] border border-white/12 bg-black/72 shadow-[0_24px_80px_rgba(0,0,0,0.55)] backdrop-blur-2xl"
        >
          <div className="flex items-start justify-between gap-4 border-b border-white/10 px-5 py-4 md:px-6">
            <div>
              <div className="text-[11px] font-semibold uppercase tracking-[0.28em] text-[#95b1a2]">Metric details</div>
              <h3 className="mt-2 text-2xl font-semibold tracking-tight text-white">{title}</h3>
              <p className="mt-2 max-w-3xl text-sm leading-6 text-white/62">{description}</p>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="inline-flex h-10 w-10 items-center justify-center rounded-xl border border-white/12 bg-white/[0.04] text-white/74 transition hover:bg-white/[0.08]"
              aria-label="Close metric details"
            >
              <X className="h-4 w-4" />
            </button>
          </div>

          <div className="overflow-y-auto px-5 py-5 md:px-6 md:py-6">
            <div className="space-y-6">
              <MetricCard title={title} icon={icon} metric={metric} />
              {children ? (
                children
              ) : (
                <section className="rounded-[28px] border border-dashed border-white/10 bg-white/[0.03] px-5 py-8 text-sm text-white/52">
                  No detailed panels are available for this metric yet.
                </section>
              )}
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}

function SectionLabel(props: { label: string; description: string; children: ReactNode }) {
  return (
    <div className="space-y-4 pt-4">
      <div className="flex items-center gap-4">
        <div>
          <div className="text-[11px] font-semibold uppercase tracking-[0.28em] text-[#95b1a2]">{props.label}</div>
          <p className="mt-0.5 text-sm text-white/48">{props.description}</p>
        </div>
        <div className="flex-1 border-t border-white/8" />
      </div>
      {props.children}
    </div>
  );
}

export default function AdminPerformancePage() {
  const [status, setStatus] = useState<TwfStatus | null>(null);
  const [windowValue, setWindowValue] = useState<WindowValue>("7d");
  const [deviceValue, setDeviceValue] = useState<DeviceValue>("all");
  const [loading, setLoading] = useState(true);
  const [refreshTick, setRefreshTick] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [summary, setSummary] = useState<Record<string, PerfMetricSummary>>({});
  const [frameTrend, setFrameTrend] = useState<PerfTimeseriesPoint[]>([]);
  const [loopTrend, setLoopTrend] = useState<PerfTimeseriesPoint[]>([]);
  const [firstFrameTrend, setFirstFrameTrend] = useState<PerfTimeseriesPoint[]>([]);
  const [varSwitchTrend, setVarSwitchTrend] = useState<PerfTimeseriesPoint[]>([]);
  const [tileFetchTrend, setTileFetchTrend] = useState<PerfTimeseriesPoint[]>([]);
  const [modelBreakdown, setModelBreakdown] = useState<PerfBreakdownItem[]>([]);
  const [deviceBreakdown, setDeviceBreakdown] = useState<PerfBreakdownItem[]>([]);
  const [loopModelBreakdown, setLoopModelBreakdown] = useState<PerfBreakdownItem[]>([]);
  const [firstFrameModelBreakdown, setFirstFrameModelBreakdown] = useState<PerfBreakdownItem[]>([]);
  const [firstFrameDeviceBreakdown, setFirstFrameDeviceBreakdown] = useState<PerfBreakdownItem[]>([]);
  const [scrubModelBreakdown, setScrubModelBreakdown] = useState<PerfBreakdownItem[]>([]);
  const [frameVariableBreakdown, setFrameVariableBreakdown] = useState<PerfBreakdownItem[]>([]);
  const [varSwitchModelBreakdown, setVarSwitchModelBreakdown] = useState<PerfBreakdownItem[]>([]);
  const [tileFetchModelBreakdown, setTileFetchModelBreakdown] = useState<PerfBreakdownItem[]>([]);
  const [activeMetric, setActiveMetric] = useState<MetricKey | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      setLoading(true);
      setError(null);
      try {
        const authStatus = await fetchTwfStatus();
        if (cancelled) return;
        setStatus(authStatus);
        if (!authStatus.linked || !authStatus.admin) {
          setLoading(false);
          return;
        }

        const [
          summaryData,
          frameSeries, loopSeries, firstFrameSeries, varSwitchSeries, tileFetchSeries,
          modelData, deviceData, loopModelData,
          firstFrameModelData, firstFrameDeviceData,
          scrubModelData, frameVariableData,
          varSwitchModelData, tileFetchModelData,
        ] = await Promise.all([
          fetchAdminPerfSummary({ window: windowValue, device: deviceValue }),
          fetchAdminPerfTimeseries({ metric: "frame_change", window: windowValue, device: deviceValue }),
          fetchAdminPerfTimeseries({ metric: "loop_start", window: windowValue, device: deviceValue }),
          fetchAdminPerfTimeseries({ metric: "viewer_first_frame", window: windowValue, device: deviceValue }),
          fetchAdminPerfTimeseries({ metric: "variable_switch", window: windowValue, device: deviceValue }),
          fetchAdminPerfTimeseries({ metric: "tile_fetch", window: windowValue, device: deviceValue }),
          fetchAdminPerfBreakdown({ metric: "frame_change", by: "model", window: windowValue, device: deviceValue }),
          fetchAdminPerfBreakdown({ metric: "loop_start", by: "device", window: windowValue, device: deviceValue }),
          fetchAdminPerfBreakdown({ metric: "loop_start", by: "model", window: windowValue, device: deviceValue }),
          fetchAdminPerfBreakdown({ metric: "viewer_first_frame", by: "model", window: windowValue, device: deviceValue }),
          fetchAdminPerfBreakdown({ metric: "viewer_first_frame", by: "device", window: windowValue, device: deviceValue }),
          fetchAdminPerfBreakdown({ metric: "scrub_latency", by: "model", window: windowValue, device: deviceValue }),
          fetchAdminPerfBreakdown({ metric: "frame_change", by: "variable", window: windowValue, device: deviceValue }),
          fetchAdminPerfBreakdown({ metric: "variable_switch", by: "model", window: windowValue, device: deviceValue }),
          fetchAdminPerfBreakdown({ metric: "tile_fetch", by: "model", window: windowValue, device: deviceValue }),
        ]);
        if (cancelled) return;

        setSummary(summaryData.metrics);
        setFrameTrend(frameSeries.points);
        setLoopTrend(loopSeries.points);
        setFirstFrameTrend(firstFrameSeries.points);
        setVarSwitchTrend(varSwitchSeries.points);
        setTileFetchTrend(tileFetchSeries.points);
        setModelBreakdown(modelData.items);
        setDeviceBreakdown(deviceData.items);
        setLoopModelBreakdown(loopModelData.items);
        setFirstFrameModelBreakdown(firstFrameModelData.items);
        setFirstFrameDeviceBreakdown(firstFrameDeviceData.items);
        setScrubModelBreakdown(scrubModelData.items);
        setFrameVariableBreakdown(frameVariableData.items);
        setVarSwitchModelBreakdown(varSwitchModelData.items);
        setTileFetchModelBreakdown(tileFetchModelData.items);
      } catch (nextError) {
        if (cancelled) return;
        setError(nextError instanceof Error ? nextError.message : "Failed to load admin dashboard");
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void load();
    return () => {
      cancelled = true;
    };
  }, [windowValue, deviceValue, refreshTick]);

  useEffect(() => {
    if (!activeMetric) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setActiveMetric(null);
      }
    };

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", handleKeyDown);

    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [activeMetric]);

  if (loading && status === null) {
    return (
      <div className="rounded-[28px] border border-white/12 bg-black/28 p-6 text-white shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl">
        Loading admin dashboard...
      </div>
    );
  }

  if (!status?.linked) {
    return (
      <section className="rounded-[28px] border border-white/12 bg-black/28 p-6 text-white shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl">
        <div className="flex items-center gap-2 text-lg font-semibold">
          <AlertCircle className="h-5 w-5 text-amber-200" />
          Login required
        </div>
        <p className="mt-3 max-w-xl text-sm leading-6 text-white/66">
          The admin dashboard is private. Sign in with your linked The Weather Forums account before opening this page.
        </p>
        <Link
          to="/login"
          className="mt-5 inline-flex rounded-xl border border-white/15 bg-white/[0.06] px-4 py-2 text-sm font-medium text-white hover:bg-white/[0.1]"
        >
          Open login
        </Link>
      </section>
    );
  }

  if (!status.admin) {
    return (
      <section className="rounded-[28px] border border-white/12 bg-black/28 p-6 text-white shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl">
        <div className="flex items-center gap-2 text-lg font-semibold">
          <AlertCircle className="h-5 w-5 text-amber-200" />
          Admin access required
        </div>
        <p className="mt-3 max-w-xl text-sm leading-6 text-white/66">
          Your account is linked, but it is not in the configured admin allowlist yet.
        </p>
      </section>
    );
  }

  const metricSections: Array<{
    label: string;
    description: string;
    metrics: Array<{
      key: MetricKey;
      title: string;
      description: string;
      icon: ComponentType<{ className?: string }>;
      metric?: PerfMetricSummary;
      detailContent?: ReactNode;
    }>;
  }> = [
    {
      label: "Playback & Animation",
      description: "Real-time frame rendering, loop playback, and scrub responsiveness.",
      metrics: [
        {
          key: "frame_change",
          title: "Frame Change",
          description: "How quickly the map responds to manual frame changes.",
          icon: Gauge,
          metric: summary.frame_change,
          detailContent: (
            <>
              <TrendChart
                title="Frame Change Trend"
                subtitle="How quickly the map responds to manual frame changes."
                points={frameTrend}
                lineColor="#7ec8ff"
              />
              <div className="grid gap-6 xl:grid-cols-2">
                <BreakdownList
                  title="Frame Change by Model"
                  subtitle="Most active models ordered by sample count."
                  items={modelBreakdown}
                />
                <BreakdownList
                  title="Frame Change by Variable"
                  subtitle="Frame change latency split by variable — identifies render-heavy variables."
                  items={frameVariableBreakdown}
                />
              </div>
            </>
          ),
        },
        {
          key: "loop_start",
          title: "Loop Start",
          description: "Time from play action to actual loop playback start.",
          icon: Activity,
          metric: summary.loop_start,
          detailContent: (
            <>
              <TrendChart
                title="Loop Start Trend"
                subtitle="Time from play action to actual loop playback start."
                points={loopTrend}
                lineColor="#b7e38f"
              />
              <div className="grid gap-6 xl:grid-cols-2">
                <BreakdownList
                  title="Loop Start by Model"
                  subtitle="Playback startup latency split by model."
                  items={loopModelBreakdown}
                />
                <BreakdownList
                  title="Loop Start by Device"
                  subtitle="Quick split of playback startup behavior."
                  items={deviceBreakdown}
                />
              </div>
            </>
          ),
        },
        {
          key: "scrub_latency",
          title: "Scrub Latency",
          description: "How responsive timeline scrubbing feels under real interaction.",
          icon: TimerReset,
          metric: summary.scrub_latency,
          detailContent: (
            <BreakdownList
              title="Scrub Latency by Model"
              subtitle="Scrub response time per model — reveals which datasets are cache-miss prone."
              items={scrubModelBreakdown}
            />
          ),
        },
        {
          key: "animation_stall",
          title: "Animation Stall",
          description: "Counts slow playback frames that exceed the stall threshold.",
          icon: PauseCircle,
          metric: summary.animation_stall,
        },
      ],
    },
    {
      label: "Cold Start & Navigation",
      description: "Initial viewer load time and variable switching latency.",
      metrics: [
        {
          key: "viewer_first_frame",
          title: "First Viewer Frame",
          description: "Time from viewer open to first frame being rendered.",
          icon: Zap,
          metric: summary.viewer_first_frame,
          detailContent: (
            <>
              <TrendChart
                title="First Viewer Frame Trend"
                subtitle="Time from viewer open to first frame being rendered."
                points={firstFrameTrend}
                lineColor="#f0a575"
              />
              <div className="grid gap-6 xl:grid-cols-2">
                <BreakdownList
                  title="First Viewer Frame by Model"
                  subtitle="Cold-start render latency per model — identifies slow-loading datasets."
                  items={firstFrameModelBreakdown}
                />
                <BreakdownList
                  title="First Viewer Frame by Device"
                  subtitle="Cold-start render latency split by device type."
                  items={firstFrameDeviceBreakdown}
                />
              </div>
            </>
          ),
        },
        {
          key: "variable_switch",
          title: "Variable Switch",
          description: "Time from variable selector click to first frame of the new variable.",
          icon: Layers,
          metric: summary.variable_switch,
          detailContent: (
            <>
              <TrendChart
                title="Variable Switch Trend"
                subtitle="Time from variable selector click to first frame of new variable."
                points={varSwitchTrend}
                lineColor="#c4a8f5"
              />
              <BreakdownList
                title="Variable Switch by Model"
                subtitle="Time to first frame after a variable selection, per model."
                items={varSwitchModelBreakdown}
              />
            </>
          ),
        },
      ],
    },
    {
      label: "Network / Tile Fetch",
      description: "Individual weather tile network fetch latency from the CDN.",
      metrics: [
        {
          key: "tile_fetch",
          title: "Tile Fetch",
          description: "Individual weather tile network fetch duration from the CDN.",
          icon: Globe,
          metric: summary.tile_fetch,
          detailContent: (
            <>
              <TrendChart
                title="Tile Fetch Trend"
                subtitle="Individual weather tile network fetch duration (sampled 1-in-8)."
                points={tileFetchTrend}
                lineColor="#f5c842"
              />
              <BreakdownList
                title="Tile Fetch by Model"
                subtitle="Sampled network fetch time per weather tile, split by model."
                items={tileFetchModelBreakdown}
              />
            </>
          ),
        },
      ],
    },
  ];
  const activeMetricConfig = metricSections
    .flatMap((section) => section.metrics)
    .find((metric) => metric.key === activeMetric);

  return (
    <>
      <div className="space-y-6">
        <section className="rounded-[32px] border border-white/12 bg-black/28 p-6 shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl md:p-7">
        <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="text-[11px] font-semibold uppercase tracking-[0.28em] text-[#95b1a2]">Performance</div>
            <h2 className="mt-2 text-4xl font-semibold tracking-tight text-white">Viewer telemetry</h2>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-white/62">
              Real-user viewer timing from the current frontend build. Focus on p95 for frame changes and loop start.
            </p>
            <div className="mt-4 flex flex-wrap items-center gap-2 text-[11px] font-medium uppercase tracking-[0.18em] text-white/58">
              <span className="rounded-full border border-emerald-400/25 bg-emerald-500/12 px-3 py-1 text-emerald-100">
                Green = well under target
              </span>
              <span className="rounded-full border border-amber-400/25 bg-amber-500/12 px-3 py-1 text-amber-100">
                Yellow = near target
              </span>
              <span className="rounded-full border border-rose-400/25 bg-rose-500/12 px-3 py-1 text-rose-100">
                Red = over target
              </span>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <select
              value={deviceValue}
              onChange={(event) => setDeviceValue(event.target.value as DeviceValue)}
              className="rounded-xl border border-white/12 bg-white/[0.06] px-3 py-2 text-sm text-white outline-none"
            >
              <option value="all">All devices</option>
              <option value="desktop">Desktop</option>
              <option value="mobile">Mobile</option>
            </select>

            <select
              value={windowValue}
              onChange={(event) => setWindowValue(event.target.value as WindowValue)}
              className="rounded-xl border border-white/12 bg-white/[0.06] px-3 py-2 text-sm text-white outline-none"
            >
              <option value="24h">Last 24h</option>
              <option value="7d">Last 7d</option>
              <option value="30d">Last 30d</option>
            </select>

            <button
              type="button"
              onClick={() => setRefreshTick((value) => value + 1)}
              className="inline-flex items-center gap-2 rounded-xl border border-white/12 bg-white/[0.06] px-3 py-2 text-sm font-medium text-white hover:bg-white/[0.1]"
            >
              <RefreshCcw className="h-4 w-4" />
              Refresh
            </button>
          </div>
        </div>

        {error ? (
          <div className="mt-5 rounded-2xl border border-red-400/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
            {error}
          </div>
        ) : null}
        </section>

        {metricSections.map((section) => (
          <SectionLabel key={section.label} label={section.label} description={section.description}>
            <div className={`grid gap-4 ${section.metrics.length >= 4 ? "xl:grid-cols-4" : section.metrics.length === 2 ? "xl:grid-cols-2" : "xl:grid-cols-3"}`}>
              {section.metrics.map((metric) => (
                <MetricCard
                  key={metric.key}
                  title={metric.title}
                  icon={metric.icon}
                  metric={metric.metric}
                  onClick={() => setActiveMetric(metric.key)}
                />
              ))}
            </div>
          </SectionLabel>
        ))}
      </div>

      <MetricDetailDialog
        open={activeMetricConfig !== undefined}
        title={activeMetricConfig?.title ?? ""}
        description={activeMetricConfig?.description ?? ""}
        icon={activeMetricConfig?.icon ?? Gauge}
        metric={activeMetricConfig?.metric}
        onClose={() => setActiveMetric(null)}
      >
        {activeMetricConfig?.detailContent}
      </MetricDetailDialog>
    </>
  );
}

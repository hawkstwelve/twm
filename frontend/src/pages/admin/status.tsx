import { useEffect, useMemo, useRef, useState } from "react";
import { AlertTriangle, ClipboardCheck, SearchCheck, ShieldAlert, X } from "lucide-react";

import {
  fetchAdminStatusResults,
  fetchTwfStatus,
  type TwfStatus,
  type StatusDiagnostics,
  type StatusResult,
} from "@/lib/admin-api";

type WindowValue = "24h" | "7d" | "30d";
type ViewFilter = "issues" | "artifacts" | "derived" | "all";

function formatTimestamp(value: number | null | undefined): string {
  if (!value) return "—";
  return new Date(value * 1000).toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatCoverage(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return `${(value * 100).toFixed(1)}%`;
}

function formatRange(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return value.toFixed(1);
}

function formatDiagnosticPercent(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  if (value <= 0) return "0.0%";
  if (value < 0.1) return "<0.1%";
  return `${value.toFixed(1)}%`;
}

function formatDiagnosticValue(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  if (value <= 0) return "0.0";
  if (value < 0.1) return "<0.1";
  return value.toFixed(1);
}

function issueType(result: StatusResult): "missing_artifact" | "unreadable_artifact" | "derived_warning" | "ok" {
  const artifact = result.diagnostics.artifact;
  if (artifact?.issue_type === "missing_value_grid") return "missing_artifact";
  if (artifact?.issue_type === "unreadable_value_grid") return "unreadable_artifact";
  if (result.auto_status === "warning") return "derived_warning";
  return "ok";
}

function issueLabel(result: StatusResult): string {
  const kind = issueType(result);
  if (kind === "missing_artifact") return "Missing artifact";
  if (kind === "unreadable_artifact") return "Unreadable COG";
  if (kind === "derived_warning") return "Derived-field warning";
  return "Healthy";
}

function issueTone(result: StatusResult): "pass" | "warning" | "fail" | "review" {
  const kind = issueType(result);
  if (kind === "missing_artifact" || kind === "unreadable_artifact") return "fail";
  if (kind === "derived_warning") return "warning";
  return "pass";
}

function issueSummary(result: StatusResult): string {
  if (result.warning_summary) return result.warning_summary;
  const monotonic = result.diagnostics.monotonic;
  if (monotonic && !monotonic.ok) {
    return `${formatDiagnosticPercent((monotonic.decreased_fraction ?? 0) * 100)} of valid pixels decreased; max drop ${formatDiagnosticValue(monotonic.max_decrease)}.`;
  }
  return "No automatic issues detected.";
}

function ReviewBadge(props: { tone: "pass" | "warning" | "review" | "fail"; label: string }) {
  const className =
    props.tone === "pass"
      ? "border-emerald-400/25 bg-emerald-500/12 text-emerald-100"
      : props.tone === "warning"
        ? "border-amber-400/25 bg-amber-500/12 text-amber-100"
        : props.tone === "fail"
          ? "border-rose-400/25 bg-rose-500/12 text-rose-100"
          : "border-white/10 bg-white/[0.04] text-white/70";
  return <span className={`inline-flex rounded-full border px-3 py-1 text-[11px] font-medium uppercase tracking-[0.18em] ${className}`}>{props.label}</span>;
}

function SummaryCard(props: {
  title: string;
  value: number;
  accent: string;
  icon: typeof ClipboardCheck;
  hint?: string;
  onClick?: () => void;
  active?: boolean;
}) {
  const muted = props.value === 0;
  const Icon = props.icon;
  return (
    <section
      className={[
        "rounded-[24px] border p-5 shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl",
        props.onClick ? "cursor-pointer transition-colors hover:bg-white/[0.03]" : "",
        muted ? "border-white/8 bg-black/18" : "border-white/12 bg-black/28",
        props.active ? "ring-1 ring-emerald-300/30" : "",
      ].join(" ")}
      onClick={props.onClick}
    >
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className={`text-sm font-semibold ${muted ? "text-white/72" : "text-white"}`}>{props.title}</div>
          <div className={`mt-3 text-[2.1rem] font-semibold tracking-tight ${muted ? "text-white/68" : props.accent}`}>{props.value}</div>
          {props.hint ? <div className="mt-2 text-xs uppercase tracking-[0.18em] text-white/38">{props.hint}</div> : null}
        </div>
        <div className={`rounded-2xl border p-3 ${muted ? "border-white/8 bg-white/[0.025]" : "border-white/10 bg-white/[0.05]"}`}>
          <Icon className={`h-5 w-5 ${muted ? "text-white/52" : props.accent}`} />
        </div>
      </div>
    </section>
  );
}

function selectedViewRows(rows: StatusResult[], view: ViewFilter): StatusResult[] {
  if (view === "all") return rows;
  if (view === "issues") return rows.filter((row) => row.auto_status === "warning");
  if (view === "artifacts") return rows.filter((row) => {
    const kind = issueType(row);
    return kind === "missing_artifact" || kind === "unreadable_artifact";
  });
  return rows.filter((row) => issueType(row) === "derived_warning");
}

function viewLabel(view: ViewFilter): string {
  if (view === "issues") return "Open issues";
  if (view === "artifacts") return "Artifact failures";
  if (view === "derived") return "Derived warnings";
  return "All tracked frames";
}

export default function AdminStatusPage() {
  const [status, setStatus] = useState<TwfStatus | null>(null);
  const [windowValue, setWindowValue] = useState<WindowValue>("30d");
  const [modelFilter, setModelFilter] = useState<string>("all");
  const [variableFilter, setVariableFilter] = useState<string>("all");
  const [viewFilter, setViewFilter] = useState<ViewFilter>("issues");
  const [results, setResults] = useState<StatusResult[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const topScrollRef = useRef<HTMLDivElement | null>(null);
  const tableScrollRef = useRef<HTMLDivElement | null>(null);
  const [tableScrollWidth, setTableScrollWidth] = useState(0);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const authStatus = await fetchTwfStatus();
        if (cancelled) return;
        setStatus(authStatus);
        if (!authStatus.linked || !authStatus.admin) return;

        const response = await fetchAdminStatusResults({
          window: windowValue,
          model: modelFilter,
          variable: variableFilter,
          limit: 500,
        });
        if (cancelled) return;
        setResults(response.results);
        setError(null);
      } catch (nextError) {
        if (cancelled) return;
        setError(nextError instanceof Error ? nextError.message : "Failed to load pipeline status");
      }
    }

    void load();
    return () => {
      cancelled = true;
    };
  }, [windowValue, modelFilter, variableFilter]);

  const filteredRows = useMemo(() => selectedViewRows(results, viewFilter), [results, viewFilter]);
  const selected = filteredRows.find((item) => item.id === selectedId) ?? results.find((item) => item.id === selectedId) ?? null;

  useEffect(() => {
    if (selectedId !== null && !results.some((item) => item.id === selectedId)) {
      setSelectedId(null);
    }
  }, [results, selectedId]);

  useEffect(() => {
    function updateScrollWidth() {
      if (!tableScrollRef.current) return;
      setTableScrollWidth(tableScrollRef.current.scrollWidth);
    }
    updateScrollWidth();
    window.addEventListener("resize", updateScrollWidth);
    return () => window.removeEventListener("resize", updateScrollWidth);
  }, [filteredRows]);

  function syncScroll(source: "top" | "table") {
    if (!topScrollRef.current || !tableScrollRef.current) return;
    if (source === "top") {
      tableScrollRef.current.scrollLeft = topScrollRef.current.scrollLeft;
    } else {
      topScrollRef.current.scrollLeft = tableScrollRef.current.scrollLeft;
    }
  }

  const modelOptions = Array.from(new Set(results.map((item) => item.model_id))).sort();
  const variableOptions = Array.from(new Set(results.map((item) => item.variable_id))).sort();
  const issueRows = results.filter((row) => row.auto_status === "warning");
  const artifactRows = results.filter((row) => {
    const kind = issueType(row);
    return kind === "missing_artifact" || kind === "unreadable_artifact";
  });
  const derivedWarningRows = results.filter((row) => issueType(row) === "derived_warning");
  const emptyStateMessage =
    results.length === 0
      ? "No pipeline status rows found yet for the retained runs on disk."
      : viewFilter === "issues"
        ? "No open issues in the current retained runs."
        : viewFilter === "artifacts"
          ? "No missing or unreadable artifacts in the current retained runs."
          : viewFilter === "derived"
            ? "No derived-field warnings in the current retained runs."
            : "No rows match the current filters.";

  if (!status?.linked || !status.admin) {
    return (
      <section className="rounded-[28px] border border-white/12 bg-black/28 p-6 text-white shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl">
        Admin pipeline status appears here after admin access is available.
      </section>
    );
  }

  return (
    <div className="space-y-6">
      <section className="rounded-[32px] border border-white/12 bg-black/28 p-6 text-white shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl">
        <div className="flex items-start gap-3">
          <div className="rounded-2xl border border-white/12 bg-white/[0.05] p-3">
            <ClipboardCheck className="h-5 w-5 text-[#9dd5bf]" />
          </div>
          <div>
            <div className="text-2xl font-semibold tracking-tight">Pipeline Status</div>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-white/62">
              Operational health for the retained published runs. This view tracks missing artifacts, unreadable value grids, and derived-field warnings across the current pipeline output.
            </p>
          </div>
        </div>

        {error ? (
          <div className="mt-5 rounded-2xl border border-red-400/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
            {error}
          </div>
        ) : null}

        <div className="mt-6 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <SummaryCard
            title="Tracked frames"
            value={results.length}
            accent="text-white"
            icon={SearchCheck}
            hint="click for all"
            onClick={() => setViewFilter("all")}
            active={viewFilter === "all"}
          />
          <SummaryCard
            title="Open issues"
            value={issueRows.length}
            accent="text-amber-300"
            icon={ShieldAlert}
            hint="click to inspect"
            onClick={() => setViewFilter("issues")}
            active={viewFilter === "issues"}
          />
          <SummaryCard
            title="Artifact failures"
            value={artifactRows.length}
            accent="text-rose-300"
            icon={AlertTriangle}
            hint="missing or unreadable"
            onClick={() => setViewFilter("artifacts")}
            active={viewFilter === "artifacts"}
          />
          <SummaryCard
            title="Derived warnings"
            value={derivedWarningRows.length}
            accent="text-amber-300"
            icon={ClipboardCheck}
            hint="cumulative or content"
            onClick={() => setViewFilter("derived")}
            active={viewFilter === "derived"}
          />
        </div>

        <div className="mt-6 grid gap-3 md:grid-cols-4">
          <label className="space-y-2 text-sm">
            <span className="text-white/62">Window</span>
            <select
              value={windowValue}
              onChange={(event) => setWindowValue(event.target.value as WindowValue)}
              className="w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-white outline-none"
            >
              <option value="24h">24 hours</option>
              <option value="7d">7 days</option>
              <option value="30d">30 days</option>
            </select>
          </label>
          <label className="space-y-2 text-sm">
            <span className="text-white/62">Model</span>
            <select
              value={modelFilter}
              onChange={(event) => setModelFilter(event.target.value)}
              className="w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-white outline-none"
            >
              <option value="all">All models</option>
              {modelOptions.map((modelId) => (
                <option key={modelId} value={modelId}>
                  {modelId}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-2 text-sm">
            <span className="text-white/62">Variable</span>
            <select
              value={variableFilter}
              onChange={(event) => setVariableFilter(event.target.value)}
              className="w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-white outline-none"
            >
              <option value="all">All variables</option>
              {variableOptions.map((variableId) => (
                <option key={variableId} value={variableId}>
                  {variableId}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-2 text-sm">
            <span className="text-white/62">View</span>
            <select
              value={viewFilter}
              onChange={(event) => setViewFilter(event.target.value as ViewFilter)}
              className="w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-white outline-none"
            >
              <option value="issues">Open issues</option>
              <option value="artifacts">Artifact failures</option>
              <option value="derived">Derived warnings</option>
              <option value="all">All tracked frames</option>
            </select>
          </label>
        </div>

        <div className="mt-4 text-sm text-white/48">
          Current signals include <span className="text-white/72">missing artifacts</span>, <span className="text-white/72">unreadable value grids</span>, and <span className="text-white/72">derived-field warnings</span>. This page only tracks the latest four retained published runs per model.
        </div>
      </section>

      <section className="rounded-[32px] border border-white/12 bg-black/28 p-4 text-white shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl">
        <div className="mb-3 flex items-center justify-between gap-3 px-2">
          <div>
            <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-[#95b1a2]">Current View</div>
            <div className="mt-1 text-sm text-white/58">
              Showing <span className="text-white">{viewLabel(viewFilter)}</span> for the retained runs matching the current filters.
            </div>
          </div>
          <div className="rounded-full border border-white/10 bg-white/[0.04] px-3 py-1 text-xs font-medium text-white/60">
            {filteredRows.length} rows loaded
          </div>
        </div>

        <div className="mb-3 px-2 text-xs text-white/42">
          Click a row to inspect the issue. Use the top scrollbar to reach the right-side columns while staying at the top of the table.
        </div>

        <div ref={topScrollRef} onScroll={() => syncScroll("top")} className="mb-3 overflow-x-auto px-2">
          <div className="h-3 rounded-full bg-white/[0.04]" style={{ width: tableScrollWidth > 0 ? `${tableScrollWidth}px` : "100%" }} />
        </div>

        <div ref={tableScrollRef} onScroll={() => syncScroll("table")} className="overflow-x-auto pb-2">
          <table className="w-max min-w-[1240px] border-separate border-spacing-y-2 text-left text-sm">
            <thead className="text-white/48">
              <tr>
                <th className="px-3 py-2 font-medium">Model</th>
                <th className="px-3 py-2 font-medium">Variable</th>
                <th className="px-3 py-2 font-medium">Run</th>
                <th className="px-3 py-2 font-medium">FH</th>
                <th className="px-3 py-2 font-medium">Status</th>
                <th className="px-3 py-2 font-medium">Issue type</th>
                <th className="px-3 py-2 font-medium">Issue</th>
                <th className="px-3 py-2 font-medium">Coverage</th>
                <th className="px-3 py-2 font-medium">Updated</th>
              </tr>
            </thead>
            <tbody>
              {filteredRows.length === 0 ? (
                <tr>
                  <td colSpan={9} className="rounded-2xl border border-dashed border-white/10 bg-white/[0.03] px-4 py-8 text-center text-white/48">
                    {emptyStateMessage}
                  </td>
                </tr>
              ) : (
                filteredRows.map((item) => (
                  <tr
                    key={item.id}
                    onClick={() => setSelectedId(item.id)}
                    className={[
                      "cursor-pointer rounded-2xl border transition-colors",
                      item.id === selectedId ? "bg-emerald-500/10 text-white" : "bg-white/[0.03] text-white/84 hover:bg-white/[0.05]",
                    ].join(" ")}
                  >
                    <td className="rounded-l-2xl border-y border-l border-white/10 px-3 py-3 font-semibold">{item.model_id}</td>
                    <td className="border-y border-white/10 px-3 py-3">{item.variable_id}</td>
                    <td className="border-y border-white/10 px-3 py-3">{item.run_id}</td>
                    <td className="border-y border-white/10 px-3 py-3">f{item.forecast_hour}</td>
                    <td className="border-y border-white/10 px-3 py-3">
                      <ReviewBadge tone={issueTone(item)} label={item.auto_status === "pass" ? "ok" : item.severity} />
                    </td>
                    <td className="border-y border-white/10 px-3 py-3">
                      <ReviewBadge tone={issueTone(item)} label={issueLabel(item)} />
                    </td>
                    <td className="max-w-[420px] border-y border-white/10 px-3 py-3 text-white/68">
                      <div className="line-clamp-2">{issueSummary(item)}</div>
                    </td>
                    <td className="border-y border-white/10 px-3 py-3">{formatCoverage(item.coverage_fraction)}</td>
                    <td className="rounded-r-2xl border-y border-r border-white/10 px-3 py-3 text-white/58">{formatTimestamp(item.updated_at)}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      {selected ? (
        <>
          <button type="button" aria-label="Close status details" className="fixed inset-0 z-30 bg-black/45 backdrop-blur-[2px]" onClick={() => setSelectedId(null)} />
          <section className="fixed inset-y-4 right-4 z-40 w-[min(520px,calc(100vw-2rem))] overflow-y-auto rounded-[32px] border border-white/12 bg-[#030711]/95 p-5 text-white shadow-[0_24px_80px_rgba(0,0,0,0.5)] backdrop-blur-xl">
            <div className="space-y-5">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <div className="text-[11px] font-semibold uppercase tracking-[0.26em] text-[#95b1a2]">Issue Details</div>
                  <h2 className="mt-2 text-2xl font-semibold tracking-tight">
                    {selected.model_id} · {selected.variable_id} · f{selected.forecast_hour}
                  </h2>
                  <p className="mt-1 text-sm text-white/58">
                    {selected.run_id} · updated {formatTimestamp(selected.updated_at)}
                  </p>
                </div>
                <button
                  type="button"
                  onClick={() => setSelectedId(null)}
                  className="rounded-full border border-white/10 bg-white/[0.04] p-2 text-white/72 transition hover:bg-white/[0.08] hover:text-white"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>

              <div className="grid gap-3 sm:grid-cols-2">
                <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                  <div className="text-xs uppercase tracking-[0.22em] text-white/42">Issue type</div>
                  <div className="mt-3"><ReviewBadge tone={issueTone(selected)} label={issueLabel(selected)} /></div>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                  <div className="text-xs uppercase tracking-[0.22em] text-white/42">Severity</div>
                  <div className="mt-3"><ReviewBadge tone={issueTone(selected)} label={selected.auto_status === "pass" ? "ok" : selected.severity} /></div>
                </div>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                <div className="text-xs uppercase tracking-[0.22em] text-white/42">Summary</div>
                <div className="mt-3 text-sm leading-6 text-white/78">{issueSummary(selected)}</div>
              </div>

              {selected.diagnostics.artifact ? (
                <div className="rounded-2xl border border-rose-400/18 bg-rose-500/8 p-4">
                  <div className="text-xs uppercase tracking-[0.22em] text-rose-100/70">Artifact diagnostics</div>
                  <div className="mt-3 space-y-2 text-sm text-rose-50/86">
                    <div>Value grid: <span className="font-medium">{selected.diagnostics.artifact.value_grid_exists ? "Present" : "Missing"}</span></div>
                    <div className="break-all text-rose-100/70">{selected.diagnostics.artifact.value_grid_path}</div>
                    <div>Sidecar: <span className="font-medium">{selected.diagnostics.artifact.sidecar_exists ? "Present" : "Missing"}</span></div>
                    <div className="break-all text-rose-100/70">{selected.diagnostics.artifact.sidecar_path}</div>
                    {selected.diagnostics.artifact.read_error ? (
                      <div className="rounded-xl border border-rose-400/20 bg-black/20 px-3 py-2 text-rose-100/74">
                        Read error: {selected.diagnostics.artifact.read_error}
                      </div>
                    ) : null}
                  </div>
                </div>
              ) : null}

              {selected.diagnostics.monotonic && selected.auto_status === "warning" ? (
                <div className="grid gap-3 sm:grid-cols-2">
                  <div className="rounded-2xl border border-amber-400/18 bg-amber-500/8 p-4">
                    <div className="text-xs uppercase tracking-[0.22em] text-amber-100/70">Pixels decreased</div>
                    <div className="mt-2 text-2xl font-semibold text-amber-100">
                      {formatDiagnosticPercent((selected.diagnostics.monotonic.decreased_fraction ?? 0) * 100)}
                    </div>
                    <div className="mt-1 text-sm text-amber-100/70">of valid pixels versus the previous hour</div>
                  </div>
                  <div className="rounded-2xl border border-amber-400/18 bg-amber-500/8 p-4">
                    <div className="text-xs uppercase tracking-[0.22em] text-amber-100/70">Largest drop</div>
                    <div className="mt-2 text-2xl font-semibold text-amber-100">
                      {formatDiagnosticValue(selected.diagnostics.monotonic.max_decrease)}
                    </div>
                    <div className="mt-1 text-sm text-amber-100/70">
                      {selected.diagnostics.monotonic.max_decrease_lat != null && selected.diagnostics.monotonic.max_decrease_lon != null
                        ? `Near ${selected.diagnostics.monotonic.max_decrease_lat}, ${selected.diagnostics.monotonic.max_decrease_lon}`
                        : "Location unavailable"}
                    </div>
                  </div>
                </div>
              ) : null}

              <div className="grid gap-3 sm:grid-cols-3">
                <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                  <div className="text-xs uppercase tracking-[0.22em] text-white/42">Coverage</div>
                  <div className="mt-2 text-xl font-semibold text-[#9dd5bf]">{formatCoverage(selected.coverage_fraction)}</div>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                  <div className="text-xs uppercase tracking-[0.22em] text-white/42">Value range</div>
                  <div className="mt-2 text-xl font-semibold text-white">{formatRange(selected.range_min)} to {formatRange(selected.range_max)}</div>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                  <div className="text-xs uppercase tracking-[0.22em] text-white/42">Valid pixels</div>
                  <div className="mt-2 text-xl font-semibold text-white">{selected.valid_pixel_count.toLocaleString("en-US")}</div>
                </div>
              </div>
            </div>
          </section>
        </>
      ) : null}
    </div>
  );
}

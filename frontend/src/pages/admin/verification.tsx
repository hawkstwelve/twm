import { useEffect, useState } from "react";
import { ClipboardCheck, Flag, SearchCheck, ShieldAlert } from "lucide-react";

import {
  fetchAdminVerificationResults,
  fetchAdminVerificationSummary,
  fetchTwfStatus,
  type TwfStatus,
  type VerificationDiagnostics,
  type VerificationResult,
  type VerificationSummaryResponse,
  updateAdminVerificationReview,
} from "@/lib/admin-api";

type WindowValue = "24h" | "7d" | "30d";
type QueueFilter = "review" | "flagged" | "all" | "pass" | "fail";

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

function formatNumber(value: number | null | undefined, digits = 1): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  return value.toFixed(digits);
}

function severityTone(value: string): "pass" | "warning" | "review" | "fail" {
  if (value === "high") return "fail";
  if (value === "medium" || value === "low") return "warning";
  if (value === "none") return "pass";
  return "review";
}

function monotonicSummary(diagnostics: VerificationDiagnostics): string {
  const monotonic = diagnostics.monotonic;
  if (!monotonic) return "No previous-hour comparison";
  if (monotonic.ok) return "No cumulative drop detected";
  if (monotonic.reason === "shape_mismatch") return "Grid shape changed vs previous hour";
  const fraction = typeof monotonic.decreased_fraction === "number" ? `${(monotonic.decreased_fraction * 100).toFixed(1)}%` : "—";
  const drop = formatNumber(monotonic.max_decrease, 1);
  return `${fraction} of valid pixels decreased; max drop ${drop}`;
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
  const { title, value, accent, icon: Icon } = props;
  const muted = value === 0;
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
          <div className={`text-sm font-semibold ${muted ? "text-white/72" : "text-white"}`}>{title}</div>
          <div className={`mt-3 text-[2.1rem] font-semibold tracking-tight ${muted ? "text-white/68" : accent}`}>{value}</div>
          {props.hint ? <div className="mt-2 text-xs uppercase tracking-[0.18em] text-white/38">{props.hint}</div> : null}
        </div>
        <div className={`rounded-2xl border p-3 ${muted ? "border-white/8 bg-white/[0.025]" : "border-white/10 bg-white/[0.05]"}`}>
          <Icon className={`h-5 w-5 ${muted ? "text-white/52" : accent}`} />
        </div>
      </div>
    </section>
  );
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

export default function AdminVerificationPage() {
  const [status, setStatus] = useState<TwfStatus | null>(null);
  const [windowValue, setWindowValue] = useState<WindowValue>("30d");
  const [modelFilter, setModelFilter] = useState<string>("all");
  const [variableFilter, setVariableFilter] = useState<string>("all");
  const [queueFilter, setQueueFilter] = useState<QueueFilter>("review");
  const [summary, setSummary] = useState<VerificationSummaryResponse | null>(null);
  const [results, setResults] = useState<VerificationResult[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [benchmarkSite, setBenchmarkSite] = useState("");
  const [notes, setNotes] = useState("");
  const [manualStatus, setManualStatus] = useState<"review" | "pass" | "fail">("review");
  const [error, setError] = useState<string | null>(null);
  const [saveState, setSaveState] = useState<"idle" | "saving">("idle");

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const authStatus = await fetchTwfStatus();
        if (cancelled) return;
        setStatus(authStatus);
        if (!authStatus.linked || !authStatus.admin) {
          return;
        }

        const [nextSummary, nextResults] = await Promise.all([
          fetchAdminVerificationSummary({
            window: windowValue,
            model: modelFilter,
            variable: variableFilter,
          }),
          fetchAdminVerificationResults({
            window: windowValue,
            model: modelFilter,
            variable: variableFilter,
            manualStatus: queueFilter === "review" || queueFilter === "pass" || queueFilter === "fail" ? queueFilter : "all",
            flaggedOnly: queueFilter === "flagged",
            limit: 250,
          }),
        ]);

        if (cancelled) return;
        setSummary(nextSummary);
        setResults(nextResults.results);
        setError(null);
      } catch (nextError) {
        if (cancelled) return;
        setError(nextError instanceof Error ? nextError.message : "Failed to load verification results");
      }
    }

    void load();
    return () => {
      cancelled = true;
    };
  }, [modelFilter, queueFilter, variableFilter, windowValue]);

  useEffect(() => {
    if (results.length === 0) {
      setSelectedId(null);
      return;
    }
    if (selectedId === null || !results.some((item) => item.id === selectedId)) {
      setSelectedId(results[0].id);
    }
  }, [results, selectedId]);

  const selected = results.find((item) => item.id === selectedId) ?? null;

  useEffect(() => {
    if (!selected) {
      setBenchmarkSite("");
      setNotes("");
      setManualStatus("review");
      return;
    }
    setBenchmarkSite(selected.benchmark_site ?? "");
    setNotes(selected.notes ?? "");
    setManualStatus(selected.manual_status);
  }, [selected]);

  const modelOptions = Array.from(new Set(results.map((item) => item.model_id))).sort();
  const variableOptions = Array.from(new Set(results.map((item) => item.variable_id))).sort();
  const hasAnyRows = (summary?.total_rows ?? 0) > 0;
  const queueLabel =
    queueFilter === "review"
      ? "Needs Review"
      : queueFilter === "flagged"
        ? "Flagged"
        : queueFilter === "pass"
          ? "Manual PASS"
          : queueFilter === "fail"
            ? "Manual FAIL"
            : "All Recent";
  const emptyStateMessage = hasAnyRows
    ? queueFilter === "flagged"
      ? "No flagged rows in this window. Click All Recent or Needs Review to work through the queue."
      : "No rows match this view yet. Try widening the filters or switching to All Recent."
    : "No published verification candidates found yet. This page tracks tmp2m, precip_total, snowfall_total, and snowfall_kuchera_total.";

  async function saveReview() {
    if (!selected) return;
    try {
      setSaveState("saving");
      const updated = await updateAdminVerificationReview(selected.id, {
        manual_status: manualStatus,
        benchmark_site: benchmarkSite.trim(),
        notes: notes.trim(),
      });
      setResults((current) => current.map((item) => (item.id === updated.id ? updated : item)));
      setError(null);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Failed to save verification review");
    } finally {
      setSaveState("idle");
    }
  }

  if (!status?.linked || !status.admin) {
    return (
      <section className="rounded-[28px] border border-white/12 bg-black/28 p-6 text-white shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl">
        Verification review will appear here after admin access is available and published runs have been scanned.
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
            <div className="text-2xl font-semibold tracking-tight">Verification Review</div>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-white/62">
              Lightweight QA for published maps. Internal checks flag obvious regressions, and manual review tracks parity notes against your benchmark site.
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
            title="Rows in window"
            value={summary?.total_rows ?? 0}
            accent="text-white"
            icon={SearchCheck}
            hint="click for all recent"
            onClick={() => setQueueFilter("all")}
            active={queueFilter === "all"}
          />
          <SummaryCard
            title="Auto checks passing"
            value={summary?.auto_pass_rows ?? 0}
            accent="text-emerald-300"
            icon={ClipboardCheck}
            hint="window total"
          />
          <SummaryCard
            title="Needs manual review"
            value={summary?.manual_review_rows ?? 0}
            accent="text-amber-300"
            icon={ShieldAlert}
            hint="click to open queue"
            onClick={() => setQueueFilter("review")}
            active={queueFilter === "review"}
          />
          <SummaryCard
            title="Flagged"
            value={summary?.flagged_rows ?? 0}
            accent="text-rose-300"
            icon={Flag}
            hint="click to inspect"
            onClick={() => setQueueFilter("flagged")}
            active={queueFilter === "flagged"}
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
            <span className="text-white/62">Table view</span>
            <select
              value={queueFilter}
              onChange={(event) => setQueueFilter(event.target.value as QueueFilter)}
              className="w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-white outline-none"
            >
              <option value="review">Needs review</option>
              <option value="flagged">Flagged</option>
              <option value="all">All recent</option>
              <option value="pass">Manual PASS</option>
              <option value="fail">Manual FAIL</option>
            </select>
          </label>
        </div>

        <div className="mt-4 text-sm text-white/48">
          Tracking <span className="text-white/72">tmp2m</span>, <span className="text-white/72">precip_total</span>, <span className="text-white/72">snowfall_total</span>, and <span className="text-white/72">snowfall_kuchera_total</span>. Use <span className="text-white/72">Needs review</span> to work the queue, click a row, then mark it <span className="text-white/72">PASS</span> or <span className="text-white/72">FAIL</span> in the panel on the right.
        </div>
      </section>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.3fr)_minmax(320px,0.9fr)]">
        <section className="rounded-[32px] border border-white/12 bg-black/28 p-4 text-white shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl">
          <div className="mb-3 flex items-center justify-between gap-3 px-2">
            <div>
              <div className="text-[11px] font-semibold uppercase tracking-[0.24em] text-[#95b1a2]">Queue</div>
              <div className="mt-1 text-sm text-white/58">
                Showing <span className="text-white">{queueLabel}</span> for the selected window and filters.
              </div>
            </div>
            <div className="rounded-full border border-white/10 bg-white/[0.04] px-3 py-1 text-xs font-medium text-white/60">
              {results.length} rows loaded
            </div>
          </div>

          <div className="overflow-x-auto pb-2">
            <table className="w-max min-w-[1220px] border-separate border-spacing-y-2 text-left text-sm">
              <thead className="text-white/48">
                <tr>
                  <th className="px-3 py-2 font-medium">Model</th>
                  <th className="px-3 py-2 font-medium">Variable</th>
                  <th className="px-3 py-2 font-medium">Run</th>
                  <th className="px-3 py-2 font-medium">FH</th>
                  <th className="px-3 py-2 font-medium">Severity</th>
                  <th className="px-3 py-2 font-medium">Auto</th>
                  <th className="px-3 py-2 font-medium">Manual</th>
                  <th className="px-3 py-2 font-medium">Issue</th>
                  <th className="px-3 py-2 font-medium">Coverage</th>
                  <th className="px-3 py-2 font-medium">Updated</th>
                </tr>
              </thead>
              <tbody>
                {results.length === 0 ? (
                  <tr>
                    <td colSpan={10} className="rounded-2xl border border-dashed border-white/10 bg-white/[0.03] px-4 py-8 text-center text-white/48">
                      {emptyStateMessage}
                    </td>
                  </tr>
                ) : (
                  results.map((item) => (
                    <tr
                      key={item.id}
                      onClick={() => setSelectedId(item.id)}
                      className={[
                        "cursor-pointer rounded-2xl border transition-colors",
                        item.id === selectedId
                          ? "bg-emerald-500/10 text-white"
                          : "bg-white/[0.03] text-white/84 hover:bg-white/[0.05]",
                      ].join(" ")}
                    >
                      <td className="rounded-l-2xl border-y border-l border-white/10 px-3 py-3 font-semibold">{item.model_id}</td>
                      <td className="border-y border-white/10 px-3 py-3">{item.variable_id}</td>
                      <td className="border-y border-white/10 px-3 py-3">{item.run_id}</td>
                      <td className="border-y border-white/10 px-3 py-3">f{item.forecast_hour}</td>
                      <td className="border-y border-white/10 px-3 py-3">
                        <ReviewBadge tone={severityTone(item.severity)} label={item.severity} />
                      </td>
                      <td className="border-y border-white/10 px-3 py-3">
                        <ReviewBadge tone={item.auto_status === "pass" ? "pass" : "warning"} label={item.auto_status} />
                      </td>
                      <td className="border-y border-white/10 px-3 py-3">
                        <ReviewBadge tone={item.manual_status} label={item.manual_status} />
                      </td>
                      <td className="max-w-[340px] border-y border-white/10 px-3 py-3 text-white/68">
                        <div className="line-clamp-2">
                          {item.warning_summary ?? (item.auto_status === "pass" ? "No automatic issues detected." : monotonicSummary(item.diagnostics))}
                        </div>
                      </td>
                      <td className="border-y border-white/10 px-3 py-3">{formatCoverage(item.coverage_fraction)}</td>
                      <td className="rounded-r-2xl border-y border-r border-white/10 px-3 py-3 text-white/58">
                        {formatTimestamp(item.updated_at)}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </section>

        <section className="rounded-[32px] border border-white/12 bg-black/28 p-5 text-white shadow-[0_16px_42px_rgba(0,0,0,0.3)] backdrop-blur-xl">
          {!selected ? (
            <div className="rounded-2xl border border-dashed border-white/10 bg-white/[0.03] px-4 py-10 text-sm text-white/48">
              Choose a row from the left to review it. The fastest workflow is: keep <span className="text-white/72">Needs review</span> selected, open the top row, compare it to your benchmark site, then save <span className="text-white/72">PASS</span> or <span className="text-white/72">FAIL</span>.
            </div>
          ) : (
            <div className="space-y-5">
              <div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.26em] text-[#95b1a2]">Selected Frame</div>
                <h2 className="mt-2 text-2xl font-semibold tracking-tight">
                  {selected.model_id} · {selected.variable_id} · f{selected.forecast_hour}
                </h2>
                <p className="mt-1 text-sm text-white/58">
                  {selected.run_id} · checked {formatTimestamp(selected.last_checked_at)}
                </p>
              </div>

              <div className="grid gap-3 sm:grid-cols-2">
                <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                  <div className="text-xs uppercase tracking-[0.22em] text-white/42">Auto status</div>
                  <div className="mt-3"><ReviewBadge tone={selected.auto_status === "pass" ? "pass" : "warning"} label={selected.auto_status} /></div>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                  <div className="text-xs uppercase tracking-[0.22em] text-white/42">Severity</div>
                  <div className="mt-3"><ReviewBadge tone={severityTone(selected.severity)} label={selected.severity} /></div>
                </div>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                <div className="text-xs uppercase tracking-[0.22em] text-white/42">Diagnostic summary</div>
                <div className="mt-3 text-sm leading-6 text-white/78">
                  {selected.warning_summary ?? "No automatic issues detected for this frame."}
                </div>
              </div>

              {selected.diagnostics.monotonic && selected.auto_checks.monotonic === false ? (
                <div className="grid gap-3 sm:grid-cols-2">
                  <div className="rounded-2xl border border-amber-400/18 bg-amber-500/8 p-4">
                    <div className="text-xs uppercase tracking-[0.22em] text-amber-100/70">Pixels decreased</div>
                    <div className="mt-2 text-2xl font-semibold text-amber-100">
                      {formatNumber((selected.diagnostics.monotonic.decreased_fraction ?? 0) * 100, 1)}%
                    </div>
                    <div className="mt-1 text-sm text-amber-100/70">of valid pixels versus the previous hour</div>
                  </div>
                  <div className="rounded-2xl border border-amber-400/18 bg-amber-500/8 p-4">
                    <div className="text-xs uppercase tracking-[0.22em] text-amber-100/70">Largest drop</div>
                    <div className="mt-2 text-2xl font-semibold text-amber-100">
                      {formatNumber(selected.diagnostics.monotonic.max_decrease, 1)}
                    </div>
                    <div className="mt-1 text-sm text-amber-100/70">
                      {selected.diagnostics.monotonic.max_decrease_lat != null && selected.diagnostics.monotonic.max_decrease_lon != null
                        ? `Near ${selected.diagnostics.monotonic.max_decrease_lat}, ${selected.diagnostics.monotonic.max_decrease_lon}`
                        : "Location unavailable"}
                    </div>
                  </div>
                </div>
              ) : null}

              <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                <div className="text-xs uppercase tracking-[0.22em] text-white/42">Internal checks</div>
                <div className="mt-4 grid gap-2 text-sm">
                  <div className="flex items-center justify-between gap-3">
                    <span>Has valid pixels</span>
                    <ReviewBadge tone={selected.auto_checks.has_valid_pixels ? "pass" : "warning"} label={selected.auto_checks.has_valid_pixels ? "pass" : "warning"} />
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span>Range present</span>
                    <ReviewBadge tone={selected.auto_checks.range_present ? "pass" : "warning"} label={selected.auto_checks.range_present ? "pass" : "warning"} />
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span>Coverage present</span>
                    <ReviewBadge tone={selected.auto_checks.coverage_present ? "pass" : "warning"} label={selected.auto_checks.coverage_present ? "pass" : "warning"} />
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span>Monotonic</span>
                    {selected.auto_checks.monotonic === null || selected.auto_checks.monotonic === undefined ? (
                      <ReviewBadge tone="review" label="n/a" />
                    ) : (
                      <ReviewBadge tone={selected.auto_checks.monotonic ? "pass" : "warning"} label={selected.auto_checks.monotonic ? "pass" : "warning"} />
                    )}
                  </div>
                </div>
              </div>

              <div className="grid gap-3 sm:grid-cols-4">
                <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                  <div className="text-xs uppercase tracking-[0.22em] text-white/42">Coverage</div>
                  <div className="mt-2 text-xl font-semibold text-[#9dd5bf]">{formatCoverage(selected.coverage_fraction)}</div>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                  <div className="text-xs uppercase tracking-[0.22em] text-white/42">Value range</div>
                  <div className="mt-2 text-xl font-semibold text-white">
                    {formatRange(selected.range_min)} to {formatRange(selected.range_max)}
                  </div>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                  <div className="text-xs uppercase tracking-[0.22em] text-white/42">Valid pixels</div>
                  <div className="mt-2 text-xl font-semibold text-white">{selected.valid_pixel_count.toLocaleString("en-US")}</div>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                  <div className="text-xs uppercase tracking-[0.22em] text-white/42">Manual status</div>
                  <div className="mt-2"><ReviewBadge tone={manualStatus} label={manualStatus} /></div>
                </div>
              </div>

              <div className="space-y-3 rounded-2xl border border-white/10 bg-white/[0.04] p-4">
                <label className="block space-y-2 text-sm">
                  <span className="text-white/62">Manual status</span>
                  <select
                    value={manualStatus}
                    onChange={(event) => setManualStatus(event.target.value as "review" | "pass" | "fail")}
                    className="w-full rounded-2xl border border-white/10 bg-black/20 px-4 py-3 text-white outline-none"
                  >
                    <option value="review">Review</option>
                    <option value="pass">Pass</option>
                    <option value="fail">Fail</option>
                  </select>
                </label>

                <label className="block space-y-2 text-sm">
                  <span className="text-white/62">Benchmark site</span>
                  <input
                    value={benchmarkSite}
                    onChange={(event) => setBenchmarkSite(event.target.value)}
                    placeholder="Example: Tropical Tidbits"
                    className="w-full rounded-2xl border border-white/10 bg-black/20 px-4 py-3 text-white outline-none placeholder:text-white/28"
                  />
                </label>

                <label className="block space-y-2 text-sm">
                  <span className="text-white/62">Notes</span>
                  <textarea
                    value={notes}
                    onChange={(event) => setNotes(event.target.value)}
                    rows={5}
                    placeholder="Manual parity notes"
                    className="w-full rounded-2xl border border-white/10 bg-black/20 px-4 py-3 text-white outline-none placeholder:text-white/28"
                  />
                </label>

                <div className="flex items-center justify-between gap-3">
                  <div className="text-xs text-white/46">
                    {selected.reviewer_name ? `Last review by ${selected.reviewer_name}` : "No manual review saved yet."}
                  </div>
                  <button
                    type="button"
                    onClick={() => void saveReview()}
                    disabled={saveState === "saving"}
                    className="rounded-full border border-emerald-400/25 bg-emerald-500/12 px-4 py-2 text-sm font-medium text-emerald-100 transition hover:bg-emerald-500/18 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {saveState === "saving" ? "Saving..." : "Save review"}
                  </button>
                </div>
              </div>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}

import { useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import { fetchCapabilities } from "@/lib/api";

function GlassCard({
  title,
  desc,
  children,
  right,
}: {
  title: string;
  desc?: string;
  children?: React.ReactNode;
  right?: React.ReactNode;
}) {
  return (
    <div className="rounded-2xl border border-white/10 bg-black/25 backdrop-blur-xl shadow-[0_10px_30px_rgba(0,0,0,0.35)]">
      <div className="p-5">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-sm font-semibold text-white">{title}</div>
            {desc ? <div className="mt-1 text-sm text-white/65">{desc}</div> : null}
          </div>
          {right ? <div className="shrink-0">{right}</div> : null}
        </div>
        {children ? <div className="mt-4">{children}</div> : null}
      </div>
    </div>
  );
}

function Pill({
  children,
  tone = "neutral",
}: {
  children: React.ReactNode;
  tone?: "good" | "warn" | "bad" | "neutral";
}) {
  const cls =
    tone === "good"
      ? "border-emerald-400/20 bg-emerald-400/10 text-emerald-100"
      : tone === "warn"
      ? "border-amber-400/20 bg-amber-400/10 text-amber-100"
      : tone === "bad"
      ? "border-rose-400/20 bg-rose-400/10 text-rose-100"
      : "border-white/10 bg-white/5 text-white/70";

  return (
    <div className={`rounded-lg border px-3 py-2 text-xs ${cls}`}>{children}</div>
  );
}

function formatRunLabel(runId?: string): string {
  if (!runId) return "—";
  const normalized = runId.trim();
  if (!normalized) return "—";
  if (normalized.toLowerCase() === "latest") return "Latest";

  const runMatch = normalized.match(/^(\d{4})(\d{2})(\d{2})_(\d{2})z$/i);
  if (runMatch) {
    const [, year, month, day, hour] = runMatch;
    const runDate = new Date(Date.UTC(Number(year), Number(month) - 1, Number(day), Number(hour), 0, 0));
    const dateLabel = new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "numeric",
      timeZone: "UTC",
    }).format(runDate);
    return `${hour}Z (${dateLabel})`;
  }

  const hourMatch = normalized.match(/_(\d{2})z$/i);
  if (hourMatch) return `${hourMatch[1]}Z`;
  return normalized;
}

function pct(n: number) {
  if (!Number.isFinite(n)) return "—";
  return `${Math.max(0, Math.min(100, Math.round(n)))}%`;
}

type Capabilities = any; // keep loose; you already have this shape in your project

type Manifest = {
  model: string;
  run: string;
  variables?: Record<
    string,
    {
      expected_frames?: number;
      available_frames?: number;
      frames?: any[];
    }
  >;
};

async function fetchRunManifest(model: string, run: string, signal?: AbortSignal): Promise<Manifest> {
  const r = await fetch(`/api/v4/${model}/${run}/manifest`, { signal });
  if (!r.ok) throw new Error(`manifest ${model} ${run} failed: ${r.status}`);
  return (await r.json()) as Manifest;
}

function classifyOverall(args: {
  latestReady?: boolean;
  readyVars?: number;
  totalVars?: number;
  frameRatio?: number; // 0..1
  availableFrames?: number;
}) {
  const { latestReady, readyVars, totalVars, frameRatio, availableFrames } = args;

  // If backend says latest run ready, that's the state. Don't block on var totals.
  if (latestReady === true) return { label: "Ready", tone: "good" as const };

  if (Number.isFinite(frameRatio) && (frameRatio as number) >= 0.999) {
  return { label: "Ready", tone: "good" as const };
}

  const tv = Number.isFinite(totalVars) ? (totalVars as number) : 0;
  const rv = Number.isFinite(readyVars) ? (readyVars as number) : 0;
  const fr = Number.isFinite(frameRatio) ? (frameRatio as number) : 0;

  // If we have *some* evidence of progress, call it ingesting.
  if (rv > 0) return { label: "Ingesting", tone: "warn" as const };
  if (fr > 0) return { label: "Ingesting", tone: "warn" as const };
  if ((availableFrames ?? 0) > 0) return { label: "Ingesting", tone: "warn" as const };

  // If nothing is available and not ready, it's not ready (not unknown).
  if (tv > 0) return { label: "Not ready", tone: "bad" as const };

  // Truly unknown only if we cannot infer anything.
  return { label: "Unknown", tone: "neutral" as const };
}

export default function Status() {
  const [caps, setCaps] = useState<Capabilities | null>(null);
  const [manifests, setManifests] = useState<Record<string, Manifest>>({});
  const [lastUpdatedIso, setLastUpdatedIso] = useState<string | null>(null);
  const [apiOk, setApiOk] = useState<boolean | null>(null);

  // Basic rate: capabilities + manifests poll
  const POLL_MS = 30_000;

  const inFlight = useRef<AbortController | null>(null);

  useEffect(() => {
    function stop() {
      inFlight.current?.abort();
      inFlight.current = null;
    }

    async function refresh() {
      stop();
      const controller = new AbortController();
      inFlight.current = controller;

      try {
        const c = await fetchCapabilities({ signal: controller.signal });
        setCaps(c);
        setApiOk(true);
        setLastUpdatedIso(new Date().toISOString());

        // Build manifest fetch list for latest runs we know about
        const availability = c?.availability ?? {};
        const models = Object.keys(availability).map((m) => m.toLowerCase());

        // Fetch manifests for each model's latest_run (if present)
        const nextManifests: Record<string, Manifest> = {};
        await Promise.all(
          models.map(async (modelId) => {
            const latestRun = availability?.[modelId]?.latest_run ?? availability?.[modelId.toUpperCase()]?.latest_run;
            // Availability keys can be mixed-case depending on your implementation; keep defensive.
            const run = typeof latestRun === "string" ? latestRun : null;
            if (!run || run.toLowerCase() === "latest") return;

            try {
              const m = await fetchRunManifest(modelId, run, controller.signal);
              nextManifests[modelId] = m;
            } catch {
              // Manifest missing is still useful signal (ingest down / not written yet)
            }
          })
        );

        setManifests(nextManifests);
      } catch {
        setApiOk(false);
      }
    }

    refresh();
    const t = setInterval(refresh, POLL_MS);
    return () => {
      clearInterval(t);
      stop();
    };
  }, []);

  const modelRows = useMemo(() => {
    const availability = caps?.availability ?? {};
    const modelIds = Object.keys(availability).map((k) => k.toLowerCase());

    return modelIds.map((modelId) => {
      const a = availability?.[modelId] ?? availability?.[modelId.toUpperCase()] ?? {};
      const latestRun: string | undefined = a?.latest_run;

      const latestReady: boolean | undefined = a?.latest_run_ready;
      const readyVars: number | undefined = a?.latest_run_ready_vars;
      const readyFrames: number | undefined = a?.latest_run_ready_frame_count;

      const manifest = latestRun && latestRun !== "latest" ? manifests[modelId] : undefined;

      // Compute totals from manifest if present; otherwise show best-effort.
      const vars = manifest?.variables ?? {};
      const varKeys = Object.keys(vars);

      const expectedTotal = varKeys.reduce((acc, k) => acc + (vars[k]?.expected_frames ?? 0), 0);
      const availableTotal = varKeys.reduce((acc, k) => acc + (vars[k]?.available_frames ?? 0), 0);
      const frameRatio = expectedTotal > 0 ? availableTotal / expectedTotal : (latestReady ? 1 : 0);

      const totalVars = Number.isFinite(readyVars) ? Math.max(readyVars ?? 0, varKeys.length) : varKeys.length;

      const state = classifyOverall({
      latestReady,
      readyVars,
      totalVars: totalVars || undefined,
      frameRatio,
      availableFrames:
      expectedTotal > 0
        ? availableTotal
        : Number.isFinite(readyFrames)
        ? readyFrames
        : 0,
      });

      return {
        modelId,
        latestRun,
        latestReady,
        readyVars,
        readyFrames,
        expectedTotal,
        availableTotal,
        frameRatio,
        state,
      };
    });
  }, [caps, manifests]);

  const ingestingVars = useMemo(() => {
    // Flatten variables that are not fully available
    const out: {
      modelId: string;
      run: string;
      variable: string;
      available: number;
      expected: number;
    }[] = [];

    for (const row of modelRows) {
      const run = row.latestRun;
      if (!run || run.toLowerCase() === "latest") continue;

      const m = manifests[row.modelId];
      const vars = m?.variables ?? {};
      for (const [varId, v] of Object.entries(vars)) {
        const expected = v.expected_frames ?? 0;
        const available = v.available_frames ?? 0;
        if (expected > 0 && available < expected) {
          out.push({ modelId: row.modelId, run, variable: varId, available, expected });
        }
      }
    }

    // Sort “most incomplete” first
    out.sort((a, b) => (a.available / a.expected) - (b.available / b.expected));
    return out.slice(0, 12);
  }, [modelRows, manifests]);

  const overall = useMemo(() => {
    if (apiOk === false) return { label: "Degraded", tone: "bad" as const };
    if (apiOk === true) return { label: "Operational", tone: "good" as const };
    return { label: "Checking…", tone: "neutral" as const };
  }, [apiOk]);

  return (
    <div className="space-y-14">
      {/* HERO */}
      <section className="pt-6 md:pt-10">
        <div className="max-w-3xl">
          <h1 className="text-5xl md:text-6xl font-semibold tracking-tight leading-[1.02]">
            Status,
            <br />
            <span className="text-[#577361]">Run Readiness.</span>
          </h1>

          <p className="mt-4 text-base md:text-lg text-white/70">
            Live readiness for the latest model runs using existing manifests + capabilities.
          </p>

          <div className="mt-7 flex flex-wrap gap-3">
            <Link
              to="/viewer"
              className="rounded-lg bg-[linear-gradient(to_top_right,#1f342f_0%,#526d5c_100%)] px-4 py-2.5 text-sm font-medium text-white border border-white/20 shadow-[0_8px_18px_rgba(0,0,0,0.28)] transition-all duration-150 hover:brightness-110"
            >
              Launch Viewer
            </Link>
            <Link
              to="/models"
              className="rounded-lg bg-black/20 px-4 py-2.5 text-sm font-medium text-white hover:bg-black/30 border border-white/15"
            >
              Models
            </Link>
          </div>
        </div>
      </section>

      {/* TOP HEALTH STRIP */}
      <section className="grid gap-4 md:grid-cols-3">
        <GlassCard
          title="Overall"
          desc="High-level platform status"
          right={<Pill tone={overall.tone}>{overall.label}</Pill>}
        >
          <div className="text-sm text-white/75">
            {lastUpdatedIso ? `Last checked: ${lastUpdatedIso.replace("T", " ").slice(0, 19)}Z` : "Last checked: —"}
          </div>
        </GlassCard>

        <GlassCard title="API" desc="Capabilities + manifest fetch">
          <div className="flex flex-wrap gap-2">
            <Pill tone={apiOk === true ? "good" : apiOk === false ? "bad" : "neutral"}>
              {apiOk === true ? "Reachable" : apiOk === false ? "Unreachable" : "Checking…"}
            </Pill>
            <Pill>Poll: {Math.round(POLL_MS / 1000)}s</Pill>
          </div>
          <div className="mt-3 text-xs text-white/55">
            This page uses /api/v4/capabilities and /api/v4/&lt;model&gt;/&lt;run&gt;/manifest.
          </div>
        </GlassCard>

        <GlassCard title="Schedulers" desc="Ingest is scheduler-driven">
          <div className="text-sm text-white/75 space-y-1">
            <div>twm-hrrr-scheduler</div>
            <div>twm-gfs-scheduler</div>
            <div>twm-nam-scheduler</div>
            <div>twm-nbm-scheduler</div>
          </div>
          <div className="mt-3 text-xs text-white/55">
            Service health isn’t probed directly here; readiness is inferred from manifests.
          </div>
        </GlassCard>
      </section>

      {/* MODEL TABLE */}
      <section className="space-y-4">
        <div className="flex items-end justify-between">
          <div>
            <div className="text-xs uppercase tracking-wider text-white/60">Latest runs</div>
            <h2 className="mt-2 text-2xl md:text-3xl font-semibold tracking-tight text-white">Run readiness</h2>
          </div>
        </div>

        <div className="rounded-2xl border border-white/10 bg-black/25 backdrop-blur-xl overflow-hidden">
          <div className="grid grid-cols-6 gap-0 text-xs text-white/60 border-b border-white/10 bg-white/5">
            <div className="px-4 py-3">Model</div>
            <div className="px-4 py-3">Latest Run</div>
            <div className="px-4 py-3">State</div>
            <div className="px-4 py-3">Vars Ready</div>
            <div className="px-4 py-3">Frames</div>
            <div className="px-4 py-3">Progress</div>
          </div>

          {modelRows.length === 0 ? (
            <div className="px-4 py-4 text-sm text-white/70">No models found in capabilities.</div>
          ) : (
            modelRows.map((r) => {
              const tone = r.state.tone;
              const varsLabel =
                Number.isFinite(r.readyVars) ? `${r.readyVars}` : r.expectedTotal > 0 ? "—" : "—";

              const framesLabel =
                r.expectedTotal > 0 ? `${r.availableTotal}/${r.expectedTotal}` : Number.isFinite(r.readyFrames) ? `${r.readyFrames}` : "—";

              return (
                <div
                  key={r.modelId}
                  className="grid grid-cols-6 text-sm text-white/80 border-b border-white/5 last:border-b-0"
                >
                  <div className="px-4 py-3 font-medium uppercase">{r.modelId}</div>
                  <div className="px-4 py-3 text-white/70">{formatRunLabel(r.latestRun)}</div>
                  <div className="px-4 py-3">
                    <Pill tone={tone}>{r.state.label}</Pill>
                  </div>
                  <div className="px-4 py-3 text-white/70">{varsLabel}</div>
                  <div className="px-4 py-3 text-white/70">{framesLabel}</div>
                  <div className="px-4 py-3 text-white/70">{pct(r.frameRatio * 100)}</div>
                </div>
              );
            })
          )}
        </div>

        <div className="text-xs text-white/55">
          Progress is computed from manifest totals when available; otherwise uses readiness fields from capabilities.
        </div>
      </section>

      {/* CURRENTLY INGESTING */}
      <section className="space-y-4">
        <div className="flex items-end justify-between">
          <div>
            <div className="text-xs uppercase tracking-wider text-white/60">In progress</div>
            <h2 className="mt-2 text-2xl md:text-3xl font-semibold tracking-tight text-white">
              Currently ingesting
            </h2>
          </div>
        </div>

        <GlassCard
          title="Partial variables"
          desc="Top items still building (available_frames < expected_frames)"
          right={<Pill tone={ingestingVars.length ? "warn" : "good"}>{ingestingVars.length ? "Active" : "Idle"}</Pill>}
        >
          {ingestingVars.length === 0 ? (
            <div className="text-sm text-white/70">No partial variables detected for latest runs.</div>
          ) : (
            <div className="space-y-2">
              {ingestingVars.map((v) => {
                const p = v.expected > 0 ? (v.available / v.expected) * 100 : 0;
                return (
                  <div
                    key={`${v.modelId}-${v.run}-${v.variable}`}
                    className="flex items-center justify-between gap-3 rounded-xl border border-white/10 bg-white/5 px-3 py-2"
                  >
                    <div className="min-w-0">
                      <div className="text-sm text-white/85 truncate">
                        <span className="uppercase font-medium">{v.modelId}</span>{" "}
                        <span className="text-white/60">•</span>{" "}
                        <span className="text-white/75">{formatRunLabel(v.run)}</span>{" "}
                        <span className="text-white/60">•</span>{" "}
                        <span className="text-white/80">{v.variable}</span>
                      </div>
                      <div className="text-xs text-white/55">
                        {v.available}/{v.expected} frames
                      </div>
                    </div>

                    <div className="flex items-center gap-2 shrink-0">
                      <div className="w-28 h-2 rounded-full bg-white/10 overflow-hidden">
                        <div className="h-full bg-white/40" style={{ width: `${Math.max(0, Math.min(100, p))}%` }} />
                      </div>
                      <div className="text-xs text-white/65 w-12 text-right">{pct(p)}</div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </GlassCard>
      </section>

      {/* FOOTER */}
      <section className="pt-2">
        <div className="flex flex-wrap items-center gap-6 text-xs text-white/55">
          <span>Manifest-driven progress</span>
          <span>•</span>
          <span>Latest run readiness</span>
          <span>•</span>
          <span>30s polling</span>
        </div>
      </section>
    </div>
  );
}
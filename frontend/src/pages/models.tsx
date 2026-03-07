import { useEffect, useMemo, useState } from "react";
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

function Pill({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-xs text-white/70">
      {children}
    </div>
  );
}

function formatRunLabel(runId?: string): string {
  if (!runId) return "Loading...";
  const normalized = runId.trim();
  if (!normalized) return "Loading...";
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

type ModelDef = {
  id: string;
  name: string;
  oneLiner: string;
  pills: string[];
  bestFor: string[];
  limitations: string[];
  notes?: string[];
  specs?: { k: string; v: string }[];
};

function Section({
  label,
  items,
}: {
  label: string;
  items: string[];
}) {
  return (
    <div className="space-y-2">
      <div className="text-[11px] uppercase tracking-wider text-white/55">{label}</div>
      <ul className="space-y-1.5 text-sm text-white/80">
        {items.map((t) => (
          <li key={t} className="flex gap-2">
            <span className="mt-[7px] h-1.5 w-1.5 rounded-full bg-white/35" />
            <span>{t}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

export default function Models() {
  const [latestRunsByModel, setLatestRunsByModel] = useState<Record<string, string>>({});
  const [openId, setOpenId] = useState<string>("hrrr"); // default open one model

  useEffect(() => {
    const controller = new AbortController();

    fetchCapabilities({ signal: controller.signal })
      .then((capabilities) => {
        const nextRuns: Record<string, string> = {};
        for (const [modelId, availability] of Object.entries(capabilities.availability ?? {})) {
          if (availability?.latest_run) nextRuns[modelId.toLowerCase()] = availability.latest_run;
        }
        setLatestRunsByModel(nextRuns);
      })
      .catch(() => {});

    return () => controller.abort();
  }, []);

  const models: ModelDef[] = useMemo(
    () => [
      {
        id: "hrrr",
        name: "HRRR",
        oneLiner: "Convection-permitting short range. Best for storms, wind, and mesoscale detail.",
        pills: ["CONUS", "Hourly", "3 km"],
        bestFor: [
          "Convective evolution and storm mode hints",
          "Wind maxima/gust potential and tight gradients",
          "Banding structure (snow / deformation zones) at short lead",
          "Rapidly evolving mesoscale features",
        ],
        limitations: [
          "Short horizon compared to global guidance",
          "Can be noisy beyond ~12–15h depending on regime",
          "Boundary-layer biases can show up during strong mixing",
        ],
        notes: [
          "Latest-run availability reflects what’s ingested and ready in this system.",
        ],
        specs: [
          { k: "Domain", v: "CONUS" },
          { k: "Cadence", v: "Hourly cycles" },
          { k: "Horizon", v: "Short range (product-dependent)" },
        ],
      },
      {
        id: "gfs",
        name: "GFS",
        oneLiner: "Global guidance for synoptic trends and longer lead time. Great context setter.",
        pills: ["Global", "3h to 240h, then 6h", "~25 km"],
        bestFor: [
          "Pattern recognition (ridges/troughs and timing)",
          "Longer-range temperature trends",
          "Broad QPF signals and large-scale forcing",
          "Setting the baseline for ensembles/other guidance",
        ],
        limitations: [
          "Under-resolves storm-scale detail",
          "Convective placement/coverage often benefits from higher-res guidance",
        ],
        notes: [
          "Later forecast hours typically step down in temporal granularity (product-dependent).",
        ],
        specs: [
          { k: "Domain", v: "Global" },
          { k: "Cadence", v: "00/06/12/18Z" },
          { k: "Horizon", v: "Long range" },
        ],
      },
      {
        id: "nam",
        name: "NAM",
        oneLiner: "Mesoscale guidance with regional strength. Useful bridge between global and convection-permitting.",
        pills: ["CONUS", "Every 6 hours", "~12 km"],
        bestFor: [
          "Synoptic-to-mesoscale structure and fronts",
          "Thermal gradients / baroclinic setups",
          "Broader precip placement vs storm-scale details",
        ],
        limitations: [
          "Not convection-permitting at ~12 km",
          "Use HRRR/high-res guidance for storm-scale evolution",
        ],
        notes: ["Included as a mid-resolution option for context and continuity."],
        specs: [
          { k: "Domain", v: "CONUS" },
          { k: "Cadence", v: "Every 6 hours" },
          { k: "Horizon", v: "Short-to-mid range" },
        ],
      },
      {
        id: "nbm",
        name: "NBM",
        oneLiner: "National Blend guidance for calibrated temperature, wind, precip, and snowfall context.",
        pills: ["CONUS + PNW", "Every 3 hours", "~13 km"],
        bestFor: [
          "Blended baseline for sensible weather expectations",
          "Temperature, precip, snowfall, and wind overview without model-to-model noise",
          "Quick consensus checks before diving into deterministic detail",
        ],
        limitations: [
          "Not designed for storm-scale structure or convective evolution",
          "Smoother blend can mute sharp mesoscale gradients",
          "Shorter horizon than true long-range global guidance",
        ],
        notes: [
          "Current frontend rollout mirrors the system's initial NBM catalog and forecast-hour availability.",
        ],
        specs: [
          { k: "Domain", v: "CONUS, PNW" },
          { k: "Cadence", v: "Every 3 hours" },
          { k: "Horizon", v: "0-120h in 6h steps" },
        ],
      },
    ],
    []
  );

  return (
    <div className="space-y-14">
      {/* HERO */}
      <section className="pt-6 md:pt-10">
        <div className="max-w-3xl">
          <h1 className="text-5xl md:text-6xl font-semibold tracking-tight leading-[1.02]">
            Models,
            <br />
            <span className="text-[#577361]">Clearly Defined.</span>
          </h1>

          <p className="mt-4 text-base md:text-lg text-white/70">
            A technical catalog of supported guidance: cadence, coverage, and what each model is best at.
            Built for fast inspection, smooth animation, and correct rendering.
          </p>

          <div className="mt-7 flex flex-wrap gap-3">
            <Link
              to="/viewer"
              className="rounded-lg bg-[linear-gradient(to_top_right,#1f342f_0%,#526d5c_100%)] px-4 py-2.5 text-sm font-medium text-white border border-white/20 shadow-[0_8px_18px_rgba(0,0,0,0.28)] transition-all duration-150 hover:brightness-110"
            >
              Launch Viewer
            </Link>
            <Link
              to="/status"
              className="rounded-lg bg-black/20 px-4 py-2.5 text-sm font-medium text-white hover:bg-black/30 border border-white/15"
            >
              System Status
            </Link>
          </div>
        </div>
      </section>

      {/* MODEL LIST (clean) */}
      <section className="space-y-4">
        <div className="flex items-end justify-between">
          <div>
            <div className="text-xs uppercase tracking-wider text-white/60">Current lineup</div>
            <h2 className="mt-2 text-2xl md:text-3xl font-semibold tracking-tight text-white">
              Models
            </h2>
          </div>
        </div>

        <div className="space-y-4">
          {models.map((m) => {
            const isOpen = openId === m.id;

            return (
              <GlassCard
                key={m.id}
                title={m.name}
                desc={m.oneLiner}
                right={
                  <div className="text-xs text-white/55">
                    Latest:{" "}
                    <span className="text-white/75">
                      {formatRunLabel(latestRunsByModel[m.id])}
                    </span>
                  </div>
                }
              >
                {/* pills */}
                <div className="flex flex-wrap gap-2">
                  {m.pills.map((p) => (
                    <Pill key={p}>{p}</Pill>
                  ))}
                </div>

                {/* accordion toggle */}
                <div className="mt-4 flex items-center justify-between gap-3">
                  <button
                    type="button"
                    onClick={() => setOpenId((prev) => (prev === m.id ? "" : m.id))}
                    className="rounded-lg border border-white/10 bg-white/5 px-3 py-2 text-xs font-medium text-white/80 hover:bg-white/10 transition-colors"
                    aria-expanded={isOpen}
                    aria-controls={`model-${m.id}-details`}
                  >
                    {isOpen ? "Hide details" : "Show details"}
                  </button>

                  {m.specs?.length ? (
                    <div className="hidden md:flex items-center gap-3 text-xs text-white/55">
                      {m.specs.slice(0, 3).map((s) => (
                        <span key={s.k}>
                          {s.k}: <span className="text-white/75">{s.v}</span>
                        </span>
                      ))}
                    </div>
                  ) : null}
                </div>

                {/* details */}
                {isOpen ? (
                  <div
                    id={`model-${m.id}-details`}
                    className="mt-4 rounded-xl border border-white/10 bg-white/5 p-4"
                  >
                    <div className="grid gap-6 md:grid-cols-2">
                      <Section label="Best for" items={m.bestFor} />
                      <Section label="Limitations" items={m.limitations} />
                    </div>

                    {m.notes?.length ? (
                      <>
                        <div className="my-4 h-px bg-white/10" />
                        <Section label="Notes" items={m.notes} />
                      </>
                    ) : null}
                  </div>
                ) : null}
              </GlassCard>
            );
          })}
        </div>
      </section>

      {/* ROADMAP (simple) */}
      <section className="space-y-4">
        <GlassCard
          title="Roadmap"
          desc="More models and domains are planned as the catalog expands."
        >
          <div className="space-y-3 text-sm text-white/75">
            <div className="text-[11px] uppercase tracking-wider text-white/55">Candidates</div>
            <div className="text-white/80">
              RAP, additional high-res domains, regional subsets, more ensemble context.
            </div>

            <div className="my-2 h-px bg-white/10" />

            <div className="text-[11px] uppercase tracking-wider text-white/55">Principle</div>
            <div className="text-white/80">
              Add models only when we can deliver correct rendering + smooth interactive performance.
            </div>
          </div>
        </GlassCard>
      </section>

      {/* FOOTER TRUST ROW */}
      <section className="pt-2">
        <div className="flex flex-wrap items-center gap-6 text-xs text-white/55">
          <span>Built for fast model map sharing</span>
          <span>•</span>
          <span>Optimized for smooth scrubbing</span>
          <span>•</span>
          <span>Correct resampling by field type</span>
        </div>
      </section>
    </div>
  );
}

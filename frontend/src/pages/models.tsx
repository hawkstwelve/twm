import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { fetchCapabilities } from "@/lib/api";

function GlassCard({
  title,
  desc,
  children,
}: {
  title: string;
  desc?: string;
  children?: React.ReactNode;
}) {
  return (
    <div className="rounded-2xl border border-white/10 bg-black/25 backdrop-blur-xl shadow-[0_10px_30px_rgba(0,0,0,0.35)]">
      <div className="p-5">
        <div className="text-sm font-semibold text-white">{title}</div>
        {desc ? <div className="mt-1 text-sm text-white/65">{desc}</div> : null}
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
    const runDate = new Date(
      Date.UTC(Number(year), Number(month) - 1, Number(day), Number(hour), 0, 0)
    );
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

type ModelRow = {
  key: string;
  modelId: string;
  domain: string;
  update: string;
  resolution: string;
  forecast: string;
  notes: string;
};

export default function Models() {
  const [latestRunsByModel, setLatestRunsByModel] = useState<Record<string, string>>({});

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
      .catch(() => {
        // keep fallback labels on transient errors
      });

    return () => controller.abort();
  }, []);

  const rows: ModelRow[] = useMemo(
    () => [
      {
        key: "HRRR",
        modelId: "hrrr",
        domain: "CONUS",
        update: "Hourly",
        resolution: "3 km",
        forecast: "0–18h*",
        notes: "Storm-scale / mesoscale",
      },
      {
        key: "NAM",
        modelId: "nam",
        domain: "CONUS",
        update: "Every 6 hours",
        resolution: "~12 km",
        forecast: "0–60h",
        notes: "Mesoscale (synoptic + regional)",
      },
      {
        key: "GFS",
        modelId: "gfs",
        domain: "Global",
        update: "Every 6 hours",
        resolution: "~25 km",
        forecast: "0–384h",
        notes: "Global trends / ensembles context",
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
            Model Specs,
            <br />
            <span className="text-[#577361]">No Guesswork.</span>
          </h1>

          <p className="mt-4 text-base md:text-lg text-white/70">
            A technical catalog of supported guidance: cadence, coverage, and what each model is
            best at. Rendering is optimized for correctness and speed—continuous fields stay smooth,
            categorical fields stay crisp.
          </p>

          <div className="mt-7 flex flex-wrap gap-3">
            <Link
              to="/viewer"
              className="rounded-lg bg-[linear-gradient(to_top_right,#1f342f_0%,#526d5c_100%)] px-4 py-2.5 text-sm font-medium text-white border border-white/20 shadow-[0_8px_18px_rgba(0,0,0,0.28)] transition-all duration-150 hover:brightness-110"
            >
              Launch Viewer
            </Link>
            <Link
              to="/variables"
              className="rounded-lg bg-black/20 px-4 py-2.5 text-sm font-medium text-white hover:bg-black/30 border border-white/15"
            >
              Browse Variables
            </Link>
          </div>
        </div>

        {/* Feature pills */}
        <div className="mt-10 grid gap-4 md:grid-cols-3">
          <GlassCard
            title="Transparent cadence"
            desc="Run times, update frequency, and forecast length—no ambiguity."
          />
          <GlassCard
            title="Performance-first rendering"
            desc="Fast frame animation, clean legends, and correct resampling by field type."
          />
          <GlassCard
            title="Status-aware workflow"
            desc="Know what’s ingesting, what’s ready, and what’s delayed."
          />
        </div>
      </section>

      {/* MODEL CARDS */}
      <section className="space-y-4">
        <div className="flex items-end justify-between">
          <div>
            <div className="text-xs uppercase tracking-wider text-white/60">Current lineup</div>
            <h2 className="mt-2 text-2xl md:text-3xl font-semibold tracking-tight text-white">
              Models
            </h2>
          </div>
          <Link to="/status" className="text-sm text-white/70 hover:text-white">
            System status →
          </Link>
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <GlassCard
            title="HRRR"
            desc="Convection-permitting short range. Best for storms, wind, and mesoscale detail."
          >
            <div className="grid grid-cols-3 gap-3">
              <Pill>CONUS</Pill>
              <Pill>Hourly</Pill>
              <Pill>3 km</Pill>
            </div>

            <div className="mt-4 space-y-3 text-sm text-white/75">
              <div className="grid gap-2 md:grid-cols-2">
                <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                  <div className="text-xs uppercase tracking-wider text-white/55">Best for</div>
                  <div className="mt-2 text-white/80">
                    Convective evolution, wind maxima/gusts, snow bands, rapid mesoscale changes.
                  </div>
                </div>
                <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                  <div className="text-xs uppercase tracking-wider text-white/55">Limitations</div>
                  <div className="mt-2 text-white/80">
                    Short horizon; can be noisy beyond ~12–15h; boundary-layer bias during strong mixing.
                  </div>
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                <div className="text-xs uppercase tracking-wider text-white/55">Notes</div>
                <div className="mt-2 text-white/80">
                  Typical availability is ~45–75 minutes after initialization (pipeline + upstream availability can vary).
                </div>
              </div>
            </div>
          </GlassCard>

          <GlassCard
            title="GFS"
            desc="Global guidance for synoptic trends and longer lead time. Great context setter."
          >
            <div className="grid grid-cols-3 gap-3">
              <Pill>Global</Pill>
              <Pill>Every 6 hours</Pill>
              <Pill>~25 km</Pill>
            </div>

            <div className="mt-4 space-y-3 text-sm text-white/75">
              <div className="grid gap-2 md:grid-cols-2">
                <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                  <div className="text-xs uppercase tracking-wider text-white/55">Best for</div>
                  <div className="mt-2 text-white/80">
                    Pattern recognition, trough/ridge timing, long-range temperature trends, broad QPF signals.
                  </div>
                </div>
                <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                  <div className="text-xs uppercase tracking-wider text-white/55">Limitations</div>
                  <div className="mt-2 text-white/80">
                    Under-resolves storm-scale detail; convective placement/coverage often better handled by high-res guidance.
                  </div>
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                <div className="text-xs uppercase tracking-wider text-white/55">Notes</div>
                <div className="mt-2 text-white/80">
                  Forecast length to 384h. Hourly output typically ends earlier with coarser time steps later (varies by product).
                </div>
              </div>
            </div>
          </GlassCard>

          <GlassCard
            title="NAM"
            desc="Mesoscale guidance with regional strength. Useful bridge between global and convection-permitting."
          >
            <div className="grid grid-cols-3 gap-3">
              <Pill>CONUS</Pill>
              <Pill>Every 6 hours</Pill>
              <Pill>~12 km</Pill>
            </div>

            <div className="mt-4 space-y-3 text-sm text-white/75">
              <div className="grid gap-2 md:grid-cols-2">
                <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                  <div className="text-xs uppercase tracking-wider text-white/55">Best for</div>
                  <div className="mt-2 text-white/80">
                    Synoptic-to-mesoscale structure, thermal gradients, fronts, and broader precipitation placement.
                  </div>
                </div>
                <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                  <div className="text-xs uppercase tracking-wider text-white/55">Limitations</div>
                  <div className="mt-2 text-white/80">
                    Not convection-permitting at ~12 km; use HRRR/other high-res models for storm-scale detail.
                  </div>
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                <div className="text-xs uppercase tracking-wider text-white/55">Notes</div>
                <div className="mt-2 text-white/80">
                  Included as a mesoscale option for users who want a middle-ground view between GFS and HRRR.
                </div>
              </div>
            </div>
          </GlassCard>

          <GlassCard
            title="Roadmap"
            desc="More models and domains are planned as the catalog expands."
          >
            <div className="grid gap-3 md:grid-cols-2">
              <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                <div className="text-xs uppercase tracking-wider text-white/55">Candidates</div>
                <div className="mt-2 text-sm text-white/80">
                  RAP, NAM 3km (CONUS), regional subsets, additional ensemble context.
                </div>
              </div>
              <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                <div className="text-xs uppercase tracking-wider text-white/55">Principle</div>
                <div className="mt-2 text-sm text-white/80">
                  Add models only when we can deliver correct rendering + smooth interactive performance.
                </div>
              </div>
            </div>
          </GlassCard>
        </div>
      </section>

      {/* AT-A-GLANCE TABLE */}
      <section className="space-y-4">
        <h3 className="text-lg font-semibold text-white">At-a-glance</h3>

        <div className="rounded-2xl border border-white/10 bg-black/25 backdrop-blur-xl overflow-hidden">
          <div className="grid grid-cols-7 gap-0 text-xs text-white/60 border-b border-white/10 bg-white/5">
            <div className="px-4 py-3">Model</div>
            <div className="px-4 py-3">Domain</div>
            <div className="px-4 py-3">Update</div>
            <div className="px-4 py-3">Latest Run</div>
            <div className="px-4 py-3">Resolution</div>
            <div className="px-4 py-3">Forecast</div>
            <div className="px-4 py-3">Notes</div>
          </div>

          {rows.map((row) => (
            <div
              key={row.key}
              className="grid grid-cols-7 text-sm text-white/80 border-b border-white/5 last:border-b-0"
            >
              <div className="px-4 py-3 font-medium">{row.key}</div>
              <div className="px-4 py-3 text-white/70">{row.domain}</div>
              <div className="px-4 py-3 text-white/70">{row.update}</div>
              <div className="px-4 py-3 text-white/70">{formatRunLabel(latestRunsByModel[row.modelId])}</div>
              <div className="px-4 py-3 text-white/70">{row.resolution}</div>
              <div className="px-4 py-3 text-white/70">{row.forecast}</div>
              <div className="px-4 py-3 text-white/70">{row.notes}</div>
            </div>
          ))}
        </div>

        <div className="text-xs text-white/55">
          *HRRR horizon can vary by cycle/product availability; the viewer displays what is currently ingested and ready.
        </div>
      </section>

      {/* PIPELINE + RENDERING */}
      <section className="space-y-4">
        <div className="grid gap-4 md:grid-cols-2">
          <GlassCard
            title="Processing pipeline"
            desc="What happens between a model run and what you see in the viewer."
          >
            <ol className="space-y-2 text-sm text-white/75 list-decimal pl-5">
              <li>Detect run availability and select run metadata</li>
              <li>Fetch GRIB subsets required for supported variables</li>
              <li>Decode/extract fields; apply unit conversions where appropriate</li>
              <li>Reproject to web map tiling space</li>
              <li>Render RGBA frames (variable-specific color mapping + alpha policy)</li>
              <li>Write Cloud-Optimized GeoTIFFs (COGs) + generate tile artifacts</li>
              <li>Publish tiles and frame metadata; CDN cache warms automatically</li>
            </ol>

            <div className="mt-4 rounded-xl border border-white/10 bg-white/5 p-3 text-sm text-white/75">
              Status and ingest timing are exposed on the <Link className="text-white hover:text-white/90 underline underline-offset-4" to="/status">Status</Link> page.
            </div>
          </GlassCard>

          <GlassCard
            title="Rendering policy"
            desc="Correct resampling and crisp categorical boundaries."
          >
            <div className="space-y-3 text-sm text-white/75">
              <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                <div className="text-xs uppercase tracking-wider text-white/55">Continuous fields</div>
                <div className="mt-2 text-white/80">
                  Bilinear resampling for smooth gradients (temperature, wind, pressure, etc.).
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                <div className="text-xs uppercase tracking-wider text-white/55">Categorical / indexed fields</div>
                <div className="mt-2 text-white/80">
                  Nearest-neighbor resampling to preserve discrete classes (ptype, categorical hazards, etc.).
                </div>
              </div>

              <div className="rounded-xl border border-white/10 bg-white/5 p-3">
                <div className="text-xs uppercase tracking-wider text-white/55">Overlay readability</div>
                <div className="mt-2 text-white/80">
                  Alpha and color mapping are tuned per variable to keep basemap context visible without destroying signal.
                </div>
              </div>

              <div className="text-xs text-white/55">
                Variable-specific notes (units, caveats, and interpretation) live on the Variables page.
              </div>
            </div>
          </GlassCard>
        </div>
      </section>

      {/* FOOTER TRUST ROW */}
      <section className="pt-4">
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
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

export default function Home() {
  const [latestRunsByModel, setLatestRunsByModel] = useState<Record<string, string>>({});

  useEffect(() => {
    const controller = new AbortController();

    fetchCapabilities({ signal: controller.signal })
      .then((capabilities) => {
        const nextRuns: Record<string, string> = {};
        for (const [modelId, availability] of Object.entries(capabilities.availability ?? {})) {
          if (availability?.latest_run) {
            nextRuns[modelId.toLowerCase()] = availability.latest_run;
          }
        }
        setLatestRunsByModel(nextRuns);
      })
      .catch(() => {
        // Keep fallback labels on transient errors.
      });

    return () => controller.abort();
  }, []);

  const atAGlanceRows = useMemo(
    () => [
      {
        key: "HRRR",
        modelId: "hrrr",
        update: "Hourly",
        forecast: "0-18h",
        notes: "Storm-scale",
      },
      {
        key: "GFS",
        modelId: "gfs",
        update: "Every 6 hours",
        forecast: "0-384h",
        notes: "Global trends",
      },
    ],
    []
  );

  function formatRunLabel(runId?: string): string {
    if (!runId) {
      return "Loading...";
    }
    const normalized = runId.trim();
    if (!normalized) {
      return "Loading...";
    }
    if (normalized.toLowerCase() === "latest") {
      return "Latest";
    }
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
    if (hourMatch) {
      return `${hourMatch[1]}Z`;
    }
    return normalized;
  }

  return (
    <div className="space-y-14">
      {/* HERO */}
      <section className="pt-6 md:pt-10">
        <div className="max-w-2xl">
          <h1 className="text-5xl md:text-6xl font-semibold tracking-tight leading-[1.02]">
            Advanced weather models,
            <br />
            simplified.
          </h1>

          <p className="mt-4 text-base md:text-lg text-white/70">
            Access the latest high-resolution weather models in an easy-to-use interactive map viewer.
            Smooth frame animation, fast scrubbing, and clean legends.
          </p>

          <div className="mt-7 flex flex-wrap gap-3">
            <Link
              to="/viewer"
              className="rounded-lg bg-white/15 px-4 py-2.5 text-sm font-medium text-white backdrop-blur hover:bg-white/20 border border-white/10"
            >
              Launch Viewer
            </Link>
            <Link
              to="/models"
              className="rounded-lg bg-black/20 px-4 py-2.5 text-sm font-medium text-white hover:bg-black/30 border border-white/15"
            >
              Explore Models
            </Link>
          </div>
        </div>

        {/* Feature pills */}
        <div className="mt-10 grid gap-4 md:grid-cols-3">
          <GlassCard title="Smooth animation" desc="Buffer frames to avoid stutter and keep loops clean." />
          <GlassCard title="Model catalog" desc="Know cadence, resolution, and coverage at a glance." />
          <GlassCard title="Variable library" desc="Units, notes, and examples for each product." />
        </div>
      </section>

      {/* MODELS SECTION */}
      <section className="space-y-4">
        <div className="flex items-end justify-between">
          <div>
            <div className="text-xs uppercase tracking-wider text-white/60">Access the most trusted models</div>
            <h2 className="mt-2 text-2xl md:text-3xl font-semibold tracking-tight text-white">
              Model lineup
            </h2>
          </div>
          <Link to="/models" className="text-sm text-white/70 hover:text-white">
            View all →
          </Link>
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <GlassCard title="HRRR" desc="High-res short range. Best for storms, wind, and mesoscale detail.">
            <div className="grid grid-cols-3 gap-3 text-xs text-white/65">
              <div className="rounded-lg border border-white/10 bg-white/5 px-3 py-2">CONUS</div>
              <div className="rounded-lg border border-white/10 bg-white/5 px-3 py-2">Hourly</div>
              <div className="rounded-lg border border-white/10 bg-white/5 px-3 py-2">3km</div>
            </div>
          </GlassCard>

          <GlassCard title="GFS" desc="Global guidance. Great for synoptic trends and longer lead time.">
            <div className="grid grid-cols-3 gap-3 text-xs text-white/65">
              <div className="rounded-lg border border-white/10 bg-white/5 px-3 py-2">Global</div>
              <div className="rounded-lg border border-white/10 bg-white/5 px-3 py-2">Every 6 hours</div>
              <div className="rounded-lg border border-white/10 bg-white/5 px-3 py-2">~25km</div>
            </div>
          </GlassCard>
        </div>
      </section>

      {/* SIMPLE TABLE (MOCKUP-LIKE) */}
      <section className="space-y-4">
        <h3 className="text-lg font-semibold text-white">At-a-glance</h3>

        <div className="rounded-2xl border border-white/10 bg-black/25 backdrop-blur-xl overflow-hidden">
          <div className="grid grid-cols-5 gap-0 text-xs text-white/60 border-b border-white/10 bg-white/5">
            <div className="px-4 py-3">Model</div>
            <div className="px-4 py-3">Update</div>
            <div className="px-4 py-3">Latest Run</div>
            <div className="px-4 py-3">Forecast</div>
            <div className="px-4 py-3">Notes</div>
          </div>

          {atAGlanceRows.map((row) => (
            <div key={row.key} className="grid grid-cols-5 text-sm text-white/80 border-b border-white/5 last:border-b-0">
              <div className="px-4 py-3 font-medium">{row.key}</div>
              <div className="px-4 py-3 text-white/70">{row.update}</div>
              <div className="px-4 py-3 text-white/70">
                {formatRunLabel(latestRunsByModel[row.modelId])}
              </div>
              <div className="px-4 py-3 text-white/70">{row.forecast}</div>
              <div className="px-4 py-3 text-white/70">{row.notes}</div>
            </div>
          ))}
        </div>
      </section>

      {/* FOOTER TRUST ROW */}
      <section className="pt-4">
        <div className="flex flex-wrap items-center gap-6 text-xs text-white/55">
          <span>Built for fast model map sharing</span>
          <span>•</span>
          <span>Optimized for smooth scrubbing</span>
          <span>•</span>
          <span>Forum tie-in coming soon</span>
        </div>
      </section>
    </div>
  );
}

import { Link } from "react-router-dom";

export default function Home() {
  return (
    <div className="space-y-10 text-white">
      <section className="space-y-4 pt-6">
        <h1 className="text-4xl md:text-5xl font-semibold tracking-tight">
          Advanced weather models, simplified.
        </h1>
        <p className="max-w-2xl text-white/70">
          Fast interactive maps for model runs, variables, and smooth frame animation.
        </p>

        <div className="flex flex-wrap gap-3">
          <Link
            to="/viewer"
            className="rounded-lg bg-white/15 px-4 py-2 text-sm font-medium text-white backdrop-blur hover:bg-white/20"
          >
            Launch Viewer
          </Link>
          <Link
            to="/models"
            className="rounded-lg border border-white/15 bg-black/20 px-4 py-2 text-sm font-medium text-white hover:bg-black/30"
          >
            Explore Models
          </Link>
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-3">
        {[
          ["Smooth animation", "Buffer frames to avoid stutter."],
          ["Model catalog", "Know cadence, resolution, and coverage."],
          ["Variable library", "Units, notes, and examples."],
        ].map(([title, desc]) => (
          <div key={title} className="rounded-2xl border border-white/10 bg-black/25 p-4 backdrop-blur-xl">
            <div className="font-medium">{title}</div>
            <div className="text-sm text-white/65">{desc}</div>
          </div>
        ))}
      </section>

      <section className="space-y-3">
        <div className="text-sm font-semibold text-white/80">Models</div>
        <div className="grid gap-4 md:grid-cols-2">
          {[
            ["HRRR", "High-res short range."],
            ["GFS", "Global guidance."],
          ].map(([m, d]) => (
            <div key={m} className="rounded-2xl border border-white/10 bg-black/25 p-4 backdrop-blur-xl">
              <div className="font-medium">{m}</div>
              <div className="text-sm text-white/65">{d}</div>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
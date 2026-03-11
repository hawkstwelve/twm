import type { ComponentType } from "react";
import { Activity, BarChart3, ChevronRight, ClipboardCheck } from "lucide-react";
import { NavLink, Outlet } from "react-router-dom";

function AdminNavItem(props: { to: string; label: string; icon: ComponentType<{ className?: string }> }) {
  const { to, label, icon: Icon } = props;
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        [
          "flex items-center justify-between gap-3 rounded-2xl border px-4 py-3 text-sm transition-all duration-150",
          isActive
            ? "border-emerald-300/20 bg-emerald-400/10 text-white shadow-[0_10px_30px_rgba(0,0,0,0.28)]"
            : "border-white/10 bg-black/20 text-white/74 hover:bg-white/[0.06] hover:text-white",
        ].join(" ")
      }
    >
      <span className="flex items-center gap-3">
        <Icon className="h-4 w-4" />
        {label}
      </span>
      <ChevronRight className="h-4 w-4 opacity-55" />
    </NavLink>
  );
}

export default function AdminLayout() {
  return (
    <div className="relative min-h-[calc(100vh-3.5rem)] overflow-hidden bg-[#05070c] text-white">
      <div
        aria-hidden="true"
        className="absolute inset-0"
        style={{
          backgroundImage: `
            radial-gradient(1200px 720px at 15% 15%, rgba(115,160,255,0.14), transparent 56%),
            radial-gradient(1000px 700px at 82% 22%, rgba(100,210,175,0.10), transparent 58%),
            linear-gradient(to bottom, rgba(3,6,12,0.72), rgba(3,6,12,0.96)),
            url(/assets/hero-space.webp)
          `,
          backgroundSize: "cover",
          backgroundPosition: "center",
        }}
      />
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_center,transparent_34%,rgba(0,0,0,0.48)_100%)]" />

      <div className="relative mx-auto grid min-h-[calc(100vh-3.5rem)] max-w-[1500px] grid-cols-1 gap-6 px-4 py-5 md:grid-cols-[260px_minmax(0,1fr)] md:px-5">
        <aside className="glass h-fit rounded-[28px] border border-white/12 p-4 shadow-[0_20px_60px_rgba(0,0,0,0.38)]">
          <div className="px-2 pb-4">
            <div className="text-[11px] font-semibold uppercase tracking-[0.28em] text-[#95b1a2]">
              CartoSky Admin
            </div>
            <h1 className="mt-2 text-2xl font-semibold tracking-tight text-white">Command Center</h1>
            <p className="mt-2 text-sm leading-6 text-white/62">
              Private performance and usage visibility for the viewer.
            </p>
          </div>

          <nav className="space-y-2">
            <AdminNavItem to="/admin/performance" label="Performance" icon={Activity} />
            <AdminNavItem to="/admin/status" label="Pipeline Status" icon={ClipboardCheck} />
            <AdminNavItem to="/admin/usage" label="Usage" icon={BarChart3} />
          </nav>
        </aside>

        <main className="min-w-0">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

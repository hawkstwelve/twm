import { NavLink } from "react-router-dom";

function NavItem({ to, label }: { to: string; label: string }) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        [
          "text-sm font-medium transition px-3 py-1.5 rounded-md",
          isActive
            ? "text-white bg-white/10"
            : "text-white/70 hover:text-white hover:bg-white/10",
        ].join(" ")
      }
    >
      {label}
    </NavLink>
  );
}

export default function SiteHeader({ variant }: { variant: "marketing" | "app" }) {
  return (
    <header className="sticky top-0 z-50 border-b border-white/10 bg-black/35 backdrop-blur-2xl">
      <div className="mx-auto flex h-16 max-w-6xl items-center gap-6 px-5 md:px-8">
        <NavLink to="/" className="font-semibold tracking-tight text-white">
          The Weather Models
        </NavLink>

        <nav className="hidden md:flex items-center gap-1">
          <NavItem to="/viewer" label="Viewer" />
          <NavItem to="/models" label="Models" />
          <NavItem to="/variables" label="Variables" />
          <NavItem to="/changelog" label="Changelog" />
          <NavItem to="/status" label="Status" />
        </nav>

        <div className="ml-auto flex items-center gap-3">
          <NavLink
            to="/login"
            className="rounded-md border border-white/15 bg-white/5 px-3 py-2 text-sm text-white hover:bg-white/10"
          >
            Login
          </NavLink>
          <NavLink
            to="/login"
            className="rounded-md bg-emerald-400/20 border border-emerald-300/25 px-3 py-2 text-sm font-medium text-emerald-50 hover:bg-emerald-400/25"
          >
            Sign Up
          </NavLink>
        </div>
      </div>
    </header>
  );
}
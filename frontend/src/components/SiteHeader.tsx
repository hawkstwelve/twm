import { NavLink } from "react-router-dom";

function LinkItem({ to, label }: { to: string; label: string }) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        [
          "text-sm font-medium transition",
          isActive ? "text-foreground" : "text-muted-foreground hover:text-foreground",
        ].join(" ")
      }
    >
      {label}
    </NavLink>
  );
}

export default function SiteHeader({ variant }: { variant: "marketing" | "app" }) {
  return (
    <header className="sticky top-0 z-50 border-b border-white/10 bg-black/35 backdrop-blur-xl">
      <div className="mx-auto flex h-14 max-w-6xl items-center gap-6 px-4">
        <NavLink to="/" className="font-semibold tracking-tight text-white">
          The Weather Models
        </NavLink>

        <nav className="hidden md:flex items-center gap-4">
          <LinkItem to="/viewer" label="Viewer" />
          <LinkItem to="/models" label="Models" />
          <LinkItem to="/variables" label="Variables" />
          <LinkItem to="/changelog" label="Changelog" />
          <LinkItem to="/status" label="Status" />
        </nav>

        <div className="ml-auto flex items-center gap-3">
          <NavLink
            to="/login"
            className="rounded-md border border-white/15 bg-white/10 px-3 py-1.5 text-sm text-white hover:bg-white/15"
          >
            Login
          </NavLink>
        </div>
      </div>
    </header>
  );
}
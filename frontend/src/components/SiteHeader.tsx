import { useEffect, useRef, useState } from "react";
import { NavLink, useLocation } from "react-router-dom";

type NavItemProps = {
  to: string;
  label: string;
  onClick?: () => void;
  className?: string;
};

function NavItem({ to, label, onClick, className }: NavItemProps) {
  return (
    <NavLink
      to={to}
      onClick={onClick}
      className={({ isActive }) =>
        [
          "text-sm font-medium transition px-3 py-1.5 rounded-md",
          isActive
            ? "text-white bg-white/10"
            : "text-white/70 hover:text-white hover:bg-white/10",
          className ?? "",
        ].join(" ")
      }
    >
      {label}
    </NavLink>
  );
}

export default function SiteHeader({ variant }: { variant: "marketing" | "app" }) {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const location = useLocation();
  const menuRef = useRef<HTMLDivElement | null>(null);
  const isAppVariant = variant === "app";
  const isMarketingVariant = variant === "marketing";
  const hideInlineAuthOnMobile = variant === "marketing" && location.pathname === "/";

  useEffect(() => {
    setMobileMenuOpen(false);
  }, [location.pathname]);

  useEffect(() => {
    if (!mobileMenuOpen) {
      return;
    }

    function onPointerDown(event: MouseEvent | TouchEvent) {
      const target = event.target;
      if (!(target instanceof Node)) {
        return;
      }
      if (menuRef.current?.contains(target)) {
        return;
      }
      setMobileMenuOpen(false);
    }

    function onKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setMobileMenuOpen(false);
      }
    }

    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("touchstart", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("touchstart", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [mobileMenuOpen]);

  return (
    <header className="sticky top-0 z-[60] border-b border-white/10 bg-black/35 backdrop-blur-2xl">
      <div
        className={
          isAppVariant
            ? "flex h-14 items-center gap-3 px-4 md:px-5"
            : "mx-auto flex h-16 max-w-6xl items-center gap-3 md:gap-6 px-5 md:px-8"
        }
      >
        <NavLink to="/" className="font-semibold tracking-tight text-white">
          The Weather Models
        </NavLink>

        {isMarketingVariant ? (
          <nav className="ml-auto hidden items-center gap-1 md:flex">
            <NavItem to="/viewer" label="Viewer" />
            <NavItem to="/models" label="Models" />
            <NavItem to="/variables" label="Variables" />
            <NavItem to="/changelog" label="Changelog" />
            <NavItem to="/status" label="Status" />
            <NavLink
              to="/login"
              className="ml-1 rounded-md border border-white/20 bg-[linear-gradient(to_top_right,#1f342f_0%,#526d5c_100%)] px-3 py-2 text-sm text-white shadow-[0_8px_18px_rgba(0,0,0,0.28)] transition-all duration-150 hover:brightness-110"
            >
              Login
            </NavLink>
          </nav>
        ) : null}

        {isMarketingVariant ? (
          <div className="ml-auto flex items-center gap-2 md:hidden" ref={menuRef}>
          <button
            type="button"
            className="inline-flex h-10 w-10 items-center justify-center rounded-md border border-white/15 bg-white/5 text-white hover:bg-white/10"
            aria-label="Open menu"
            aria-expanded={mobileMenuOpen}
            aria-controls="mobile-site-nav"
            onClick={() => setMobileMenuOpen((open) => !open)}
          >
            <span className="sr-only">{mobileMenuOpen ? "Close menu" : "Open menu"}</span>
            <span className="flex w-4 flex-col gap-1.5">
              <span className="block h-0.5 w-4 rounded bg-current" />
              <span className="block h-0.5 w-4 rounded bg-current" />
              <span className="block h-0.5 w-4 rounded bg-current" />
            </span>
          </button>

          {variant === "marketing" ? (
            <>
              <NavLink
                to="/login"
                className={[
                  "rounded-md border border-white/20 bg-[linear-gradient(to_top_right,#1f342f_0%,#526d5c_100%)] px-3 py-2 text-sm text-white shadow-[0_8px_18px_rgba(0,0,0,0.28)] transition-all duration-150 hover:brightness-110",
                  hideInlineAuthOnMobile ? "hidden md:inline-flex" : "",
                ].join(" ")}
              >
                Login
              </NavLink>
              <NavLink
                to="/login"
                className={[
                  "rounded-md bg-emerald-400/20 border border-emerald-300/25 px-3 py-2 text-sm font-medium text-emerald-50 hover:bg-emerald-400/25",
                  hideInlineAuthOnMobile ? "hidden md:inline-flex" : "",
                ].join(" ")}
              >
                Sign Up
              </NavLink>
            </>
          ) : null}

          {mobileMenuOpen ? (
            <nav
              id="mobile-site-nav"
              className="absolute right-0 top-[calc(100%+0.5rem)] z-[70] w-[min(92vw,360px)] rounded-2xl border border-white/15 bg-black/90 p-2.5 text-white shadow-[0_20px_52px_rgba(0,0,0,0.72)] backdrop-blur-xl"
              aria-label="Site navigation"
            >
              <div className="flex flex-col gap-1">
                <NavItem
                  to="/viewer"
                  label="Viewer"
                  onClick={() => setMobileMenuOpen(false)}
                  className="text-white/90 hover:text-white"
                />
                <NavItem
                  to="/models"
                  label="Models"
                  onClick={() => setMobileMenuOpen(false)}
                  className="text-white/90 hover:text-white"
                />
                <NavItem
                  to="/variables"
                  label="Variables"
                  onClick={() => setMobileMenuOpen(false)}
                  className="text-white/90 hover:text-white"
                />
                <NavItem
                  to="/changelog"
                  label="Changelog"
                  onClick={() => setMobileMenuOpen(false)}
                  className="text-white/90 hover:text-white"
                />
                <NavItem
                  to="/status"
                  label="Status"
                  onClick={() => setMobileMenuOpen(false)}
                  className="text-white/90 hover:text-white"
                />
                <div className="my-1 h-px bg-white/10" />
                <NavItem
                  to="/login"
                  label="Login"
                  onClick={() => setMobileMenuOpen(false)}
                  className="text-white/90 hover:text-white"
                />
                <NavItem
                  to="/login"
                  label="Sign Up"
                  onClick={() => setMobileMenuOpen(false)}
                  className="text-white/90 hover:text-white"
                />
              </div>
            </nav>
          ) : null}
          </div>
        ) : null}
      </div>
    </header>
  );
}

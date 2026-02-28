import { Outlet } from "react-router-dom";
import SiteHeader from "../components/SiteHeader";
import SiteFooter from "../components/SiteFooter";

export default function MarketingLayout() {
  return (
    <div
      className="min-h-svh"
      style={{
        backgroundImage:
          "linear-gradient(to bottom, rgba(8,10,16,0.82), rgba(8,10,16,0.92)), url(/assets/hero-space.webp)",
        backgroundSize: "cover",
        backgroundPosition: "center",
      }}
    >
      <SiteHeader variant="marketing" />
      <main className="mx-auto max-w-6xl px-4 py-10">
        <Outlet />
      </main>
      <SiteFooter />
    </div>
  );
}
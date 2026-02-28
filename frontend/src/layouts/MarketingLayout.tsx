import { Outlet } from "react-router-dom";
import SiteHeader from "../components/SiteHeader";
import SiteFooter from "../components/SiteFooter";

export default function MarketingLayout() {
  return (
    <div className="relative min-h-svh text-white overflow-hidden">
      {/* Background */}
      <div
        aria-hidden="true"
        className="absolute md:fixed inset-0 -z-10"
        style={{
          backgroundImage: `
            radial-gradient(1200px 700px at 20% 20%, rgba(120,160,255,0.14), transparent 55%),
            radial-gradient(900px 600px at 80% 30%, rgba(120,255,210,0.08), transparent 60%),
            linear-gradient(to bottom, rgba(8,10,16,0.74), rgba(8,10,16,0.92)),
            url(/assets/hero-space.webp)
          `,
          backgroundSize: "cover",
          backgroundPosition: "center",
        }}
      />
      {/* Vignette */}
      <div
        aria-hidden="true"
        className="absolute md:fixed inset-0 -z-10 pointer-events-none bg-[radial-gradient(ellipse_at_center,transparent_45%,rgba(0,0,0,0.55)_100%)]"
      />
      {/* Optional subtle noise (add /public/assets/noise.png if you want) */}
      {/* <div aria-hidden="true" className="absolute md:fixed inset-0 -z-10 pointer-events-none opacity-[0.06] mix-blend-overlay bg-[url(/assets/noise.png)]" /> */}

      <SiteHeader variant="marketing" />

      <main className="mx-auto max-w-6xl px-5 md:px-8 py-12 md:py-16">
        <Outlet />
      </main>

      <SiteFooter />
    </div>
  );
}
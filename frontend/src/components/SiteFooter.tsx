export default function SiteFooter() {
  return (
    <footer className="border-t border-white/10">
      <div className="mx-auto max-w-6xl px-4 py-8 text-sm text-white/60">
        © {new Date().getFullYear()} CartoSky
      </div>
    </footer>
  );
}
import { useEffect, useMemo, useState } from "react";

type TwfStatus =
  | { linked: false }
  | { linked: true; member_id: number; display_name: string };

type ShareResult = {
  id?: number;
  url?: string;
  [k: string]: unknown;
};

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

function getApiBase(): string {
  // Prefer a configurable base for dev/staging; fall back to production.
  const fromEnv = (import.meta as any)?.env?.VITE_API_BASE as string | undefined;
  const base = (fromEnv ?? "https://api.theweathermodels.com").trim();
  return base.replace(/\/$/, "");
}

export default function Login() {
  const apiBase = useMemo(() => getApiBase(), []);

  const [status, setStatus] = useState<TwfStatus>({ linked: false });
  const [loadingStatus, setLoadingStatus] = useState(true);
  const [statusError, setStatusError] = useState<string | null>(null);

  const [shareForumId, setShareForumId] = useState<string>("");
  const [shareTitle, setShareTitle] = useState<string>("Map share from The Weather Models");
  const [shareContent, setShareContent] = useState<string>(
    "Sharing a map from The Weather Models.\n\n(Replace this with your map link, GIF, or details.)"
  );
  const [shareBusy, setShareBusy] = useState(false);
  const [shareError, setShareError] = useState<string | null>(null);
  const [shareResult, setShareResult] = useState<ShareResult | null>(null);

  useEffect(() => {
    const controller = new AbortController();

    setLoadingStatus(true);
    setStatusError(null);

    fetch(`${apiBase}/auth/twf/status`, {
      method: "GET",
      credentials: "include",
      signal: controller.signal,
    })
      .then(async (r) => {
        if (!r.ok) {
          const text = await r.text().catch(() => "");
          throw new Error(text || `Status request failed (${r.status})`);
        }
        return (await r.json()) as TwfStatus;
      })
      .then((s) => setStatus(s))
      .catch((e: unknown) => {
        if ((e as any)?.name === "AbortError") return;
        setStatus({ linked: false });
        setStatusError((e as Error).message || "Failed to load status");
      })
      .finally(() => setLoadingStatus(false));

    return () => controller.abort();
  }, [apiBase]);

  function startTwfLogin() {
    // Full-page redirect is required for OAuth.
    window.location.href = `${apiBase}/auth/twf/start`;
  }

  async function disconnectTwf() {
    setStatusError(null);
    try {
      const r = await fetch(`${apiBase}/auth/twf/disconnect`, {
        method: "POST",
        credentials: "include",
      });
      if (!r.ok) {
        const text = await r.text().catch(() => "");
        throw new Error(text || `Disconnect failed (${r.status})`);
      }
      setStatus({ linked: false });
      setShareResult(null);
    } catch (e: unknown) {
      setStatusError((e as Error).message || "Disconnect failed");
    }
  }

  async function shareTopic() {
    setShareError(null);
    setShareResult(null);

    const forumIdNum = Number(shareForumId);
    if (!Number.isFinite(forumIdNum) || forumIdNum <= 0) {
      setShareError("Enter a valid forum_id (numeric)");
      return;
    }
    if (!shareTitle.trim()) {
      setShareError("Title is required");
      return;
    }
    if (!shareContent.trim()) {
      setShareError("Content is required");
      return;
    }

    setShareBusy(true);
    try {
      const r = await fetch(`${apiBase}/twf/share/topic`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          forum_id: forumIdNum,
          title: shareTitle.trim(),
          content: shareContent.trim(),
        }),
      });

      if (!r.ok) {
        const text = await r.text().catch(() => "");
        throw new Error(text || `Share failed (${r.status})`);
      }

      const data = (await r.json()) as ShareResult;
      setShareResult(data);
    } catch (e: unknown) {
      setShareError((e as Error).message || "Share failed");
    } finally {
      setShareBusy(false);
    }
  }

  const connected = status.linked === true;

  return (
    <div className="space-y-10">
      <section className="pt-6 md:pt-10">
        <div className="max-w-2xl">
          <h1 className="text-4xl md:text-5xl font-semibold tracking-tight leading-[1.04]">
            The Weather Forums
            <br />
            <span className="text-[#577361]">account link</span>
          </h1>
          <p className="mt-4 text-base md:text-lg text-white/70">
            Link your Weather Forums account to share maps and post directly from The Weather Models.
          </p>
        </div>

        <div className="mt-10 grid gap-4 md:grid-cols-3">
          <GlassCard title="OAuth login" desc="Secure sign-in via the forums. No passwords handled by this site." />
          <GlassCard title="Post as you" desc="Topics and replies appear under your forum account." />
          <GlassCard title="Fast sharing" desc="Generate a map link/GIF and publish it in one click." />
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-2">
        <GlassCard
          title="Connection"
          desc="Connect once, then share maps anytime. You can revoke access from your forum settings as well."
        >
          {loadingStatus ? (
            <div className="text-sm text-white/65">Checking status…</div>
          ) : connected ? (
            <div className="space-y-4">
              <div className="rounded-lg border border-white/10 bg-white/5 px-4 py-3">
                <div className="text-xs uppercase tracking-wider text-white/60">Connected as</div>
                <div className="mt-1 text-sm font-medium text-white">{status.display_name}</div>
                <div className="mt-1 text-xs text-white/60">Member ID: {status.member_id}</div>
              </div>

              <div className="flex flex-wrap gap-3">
                <button
                  type="button"
                  onClick={disconnectTwf}
                  className="rounded-lg bg-black/20 px-4 py-2.5 text-sm font-medium text-white hover:bg-black/30 border border-white/15"
                >
                  Disconnect
                </button>
              </div>
            </div>
          ) : (
            <div className="space-y-4">
              <div className="text-sm text-white/70">
                Not connected. Click below to sign in with your Weather Forums account.
              </div>

              <div className="flex flex-wrap gap-3">
                <button
                  type="button"
                  onClick={startTwfLogin}
                  className="rounded-lg bg-[linear-gradient(to_top_right,#1f342f_0%,#526d5c_100%)] px-4 py-2.5 text-sm font-medium text-white border border-white/20 shadow-[0_8px_18px_rgba(0,0,0,0.28)] transition-all duration-150 hover:brightness-110"
                >
                  Login with The Weather Forums
                </button>
              </div>

              <div className="text-xs text-white/55">
                You’ll be redirected to the forums to authorize The Weather Models, then returned here.
              </div>
            </div>
          )}

          {statusError ? (
            <div className="mt-4 rounded-lg border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
              {statusError}
            </div>
          ) : null}
        </GlassCard>

        <GlassCard
          title="Test share"
          desc="Use this to validate your backend wiring before you hook it into the viewer export workflow."
        >
          {!connected ? (
            <div className="text-sm text-white/65">Connect your account to enable posting.</div>
          ) : (
            <div className="space-y-4">
              <div className="grid gap-3">
                <div>
                  <label className="block text-xs uppercase tracking-wider text-white/60">Forum ID</label>
                  <input
                    value={shareForumId}
                    onChange={(e) => setShareForumId(e.target.value)}
                    placeholder="e.g. 12"
                    className="mt-2 w-full rounded-lg border border-white/10 bg-black/20 px-3 py-2 text-sm text-white placeholder:text-white/35 outline-none focus:border-white/20"
                  />
                  <div className="mt-1 text-xs text-white/55">
                    Tip: call <span className="text-white/70">{apiBase}/twf/forums</span> to list forums and IDs.
                  </div>
                </div>

                <div>
                  <label className="block text-xs uppercase tracking-wider text-white/60">Title</label>
                  <input
                    value={shareTitle}
                    onChange={(e) => setShareTitle(e.target.value)}
                    className="mt-2 w-full rounded-lg border border-white/10 bg-black/20 px-3 py-2 text-sm text-white outline-none focus:border-white/20"
                  />
                </div>

                <div>
                  <label className="block text-xs uppercase tracking-wider text-white/60">Content</label>
                  <textarea
                    value={shareContent}
                    onChange={(e) => setShareContent(e.target.value)}
                    rows={6}
                    className="mt-2 w-full resize-none rounded-lg border border-white/10 bg-black/20 px-3 py-2 text-sm text-white outline-none focus:border-white/20"
                  />
                </div>
              </div>

              <div className="flex flex-wrap gap-3">
                <button
                  type="button"
                  onClick={shareTopic}
                  disabled={shareBusy}
                  className="rounded-lg bg-[linear-gradient(to_top_right,#1f342f_0%,#526d5c_100%)] px-4 py-2.5 text-sm font-medium text-white border border-white/20 shadow-[0_8px_18px_rgba(0,0,0,0.28)] transition-all duration-150 hover:brightness-110 disabled:opacity-60 disabled:hover:brightness-100"
                >
                  {shareBusy ? "Posting…" : "Create topic"}
                </button>
              </div>

              {shareError ? (
                <div className="rounded-lg border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                  {shareError}
                </div>
              ) : null}

              {shareResult ? (
                <div className="rounded-lg border border-white/10 bg-white/5 px-4 py-3 text-sm text-white/80">
                  <div className="text-xs uppercase tracking-wider text-white/60">Result</div>
                  <div className="mt-2 break-words text-xs text-white/70">
                    <pre className="whitespace-pre-wrap">{JSON.stringify(shareResult, null, 2)}</pre>
                  </div>
                </div>
              ) : null}
            </div>
          )}
        </GlassCard>
      </section>

      <section className="pt-2">
        <div className="flex flex-wrap items-center gap-6 text-xs text-white/55">
          <span>OAuth: forums authorize → token exchange → server-side session</span>
          <span>•</span>
          <span>Cookies: Secure + SameSite=None + credentials include</span>
        </div>
      </section>
    </div>
  );
}
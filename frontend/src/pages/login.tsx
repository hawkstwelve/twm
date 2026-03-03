import { useEffect, useMemo, useState } from "react";

type TwfStatus =
  | { linked: false }
  | { linked: true; member_id: number; display_name: string; photo_url?: string | null };

type ShareResult = {
  topicId: number;
  topicUrl: string;
  forumId: number;
  title: string;
};

type ReplyResult = {
  postId: number;
  postUrl: string;
  topicId: number;
};

type TwfForum = {
  id: number;
  name: string;
  path?: string;
  url?: string;
};

type ApiErrorInfo = {
  code?: string;
  message: string;
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

async function readApiError(response: Response): Promise<ApiErrorInfo | null> {
  try {
    const body = (await response.json()) as any;
    const err = body?.error;
    if (err && typeof err === "object" && typeof err.message === "string" && err.message.trim()) {
      const code = typeof err.code === "string" && err.code.trim() ? err.code : undefined;
      return { code, message: err.message };
    }
  } catch {
    return null;
  }
  return null;
}

export default function Login() {
  const apiBase = useMemo(() => getApiBase(), []);

  const [status, setStatus] = useState<TwfStatus>({ linked: false });
  const [loadingStatus, setLoadingStatus] = useState(true);
  const [statusError, setStatusError] = useState<string | null>(null);

  const [shareForumId, setShareForumId] = useState<string>("");

  const [forums, setForums] = useState<TwfForum[]>([]);
  const [forumsLoading, setForumsLoading] = useState(false);
  const [forumsError, setForumsError] = useState<string | null>(null);

  const [shareTitle, setShareTitle] = useState<string>("Map share from The Weather Models");
  const [shareContent, setShareContent] = useState<string>(
    "Sharing a map from The Weather Models.\n\n(Replace this with your map link, GIF, or details.)"
  );
  const [shareBusy, setShareBusy] = useState(false);
  const [shareError, setShareError] = useState<ApiErrorInfo | null>(null);
  const [shareResult, setShareResult] = useState<ShareResult | null>(null);
  const [replyTopicId, setReplyTopicId] = useState<string>("");
  const [replyContent, setReplyContent] = useState<string>("Reply from The Weather Models.");
  const [replyBusy, setReplyBusy] = useState(false);
  const [replyError, setReplyError] = useState<ApiErrorInfo | null>(null);
  const [replyResult, setReplyResult] = useState<ReplyResult | null>(null);

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

  const connected = status.linked === true;

  useEffect(() => {
    if (!connected) {
      setForums([]);
      setForumsError(null);
      setForumsLoading(false);
      return;
    }

    const controller = new AbortController();
    setForumsLoading(true);
    setForumsError(null);

    fetch(`${apiBase}/twf/forums`, {
      method: "GET",
      credentials: "include",
      signal: controller.signal,
    })
      .then(async (r) => {
        if (!r.ok) {
          const err = await readApiError(r);
          throw new Error(err?.message || `Forums request failed (${r.status})`);
        }
        return (await r.json()) as any;
      })
      .then((data: any) => {
        const list: any[] = Array.isArray(data)
          ? data
          : Array.isArray(data?.results)
          ? data.results
          : Array.isArray(data?.forums)
          ? data.forums
          : [];

        const normalized: TwfForum[] = list
          .map((f: any) => ({
            id: Number(f?.id),
            name: String(f?.name ?? ""),
            path: typeof f?.path === "string" ? f.path : undefined,
            url: typeof f?.url === "string" ? f.url : undefined,
          }))
          .filter((f) => Number.isFinite(f.id) && f.id > 0 && f.name.trim().length > 0)
          .sort((a, b) => (a.path ?? a.name).localeCompare(b.path ?? b.name));

        setForums(normalized);

        // Auto-select the first forum if none chosen yet.
        if (!shareForumId && normalized.length > 0) {
          setShareForumId(String(normalized[0].id));
        }
      })
      .catch((e: unknown) => {
        if ((e as any)?.name === "AbortError") return;
        setForums([]);
        setForumsError((e as Error).message || "Failed to load forums");
      })
      .finally(() => setForumsLoading(false));

    return () => controller.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [apiBase, connected]);

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
      setShareError({ message: "Enter a valid forum_id (numeric)" });
      return;
    }
    if (!shareTitle.trim()) {
      setShareError({ message: "Title is required" });
      return;
    }
    if (!shareContent.trim()) {
      setShareError({ message: "Content is required" });
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
        setShareError((await readApiError(r)) ?? { message: "Request failed. Please try again." });
        return;
      }

      const data = (await r.json()) as ShareResult;
      setShareResult(data);
    } catch {
      setShareError({ message: "Request failed. Please try again." });
    } finally {
      setShareBusy(false);
    }
  }

  async function shareReply() {
    setReplyError(null);
    setReplyResult(null);

    const topicIdNum = Number(replyTopicId);
    if (!Number.isFinite(topicIdNum) || topicIdNum <= 0) {
      setReplyError({ message: "Enter a valid topic_id (numeric)" });
      return;
    }
    if (!replyContent.trim()) {
      setReplyError({ message: "Reply content is required" });
      return;
    }

    setReplyBusy(true);
    try {
      const r = await fetch(`${apiBase}/twf/share/post`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          topic_id: topicIdNum,
          content: replyContent.trim(),
        }),
      });

      if (!r.ok) {
        setReplyError((await readApiError(r)) ?? { message: "Request failed. Please try again." });
        return;
      }

      const data = (await r.json()) as ReplyResult;
      setReplyResult(data);
    } catch {
      setReplyError({ message: "Request failed. Please try again." });
    } finally {
      setReplyBusy(false);
    }
  }

  return (
    <div className="space-y-10">
      <section className="pt-6 md:pt-10">
        <div className="max-w-2xl">
          <h1 className="text-4xl md:text-5xl font-semibold tracking-tight leading-[1.04]">
            The Weather Forums,
            <br />
            <span className="text-[#577361]">Integrated.</span>
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
                  <label className="block text-xs uppercase tracking-wider text-white/60">Forum</label>

                  {forumsLoading ? (
                    <div className="mt-2 text-sm text-white/65">Loading forums…</div>
                  ) : forumsError ? (
                    <div className="mt-2 rounded-lg border border-red-500/20 bg-red-500/10 px-3 py-2 text-sm text-red-100">
                      {forumsError}
                    </div>
                  ) : (
                    <select
                      value={shareForumId}
                      onChange={(e) => {
                        setShareForumId(e.target.value);
                        setShareResult(null);
                      }}
                      className="mt-2 w-full rounded-lg border border-white/10 bg-black/20 px-3 py-2 text-sm text-white outline-none focus:border-white/20"
                    >
                      {forums.length === 0 ? (
                        <option value="">No forums found</option>
                      ) : (
                        forums.map((f) => (
                          <option key={f.id} value={String(f.id)}>
                            {(f.path ?? f.name) + ` (ID: ${f.id})`}
                          </option>
                        ))
                      )}
                    </select>
                  )}

                  <div className="mt-1 text-xs text-white/55">
                    Source: <span className="text-white/70">{apiBase}/twf/forums</span>
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
                  <div>{shareError.message}</div>
                  {shareError.code ? <div className="mt-1 text-xs text-red-100/80">{shareError.code}</div> : null}
                </div>
              ) : null}

              {shareResult ? (
                <div className="rounded-lg border border-white/10 bg-white/5 px-4 py-3 text-sm text-white/80 space-y-2">
                  <div className="text-xs uppercase tracking-wider text-white/60">Topic created</div>
                  <div className="text-sm text-white">
                    {shareResult.title}
                  </div>
                  <div className="text-xs text-white/60">
                    Forum ID: {shareResult.forumId} • Topic ID: {shareResult.topicId}
                  </div>
                  <a
                    href={shareResult.topicUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-block mt-1 text-sm text-[#8fb3a7] hover:underline"
                  >
                    Open topic →
                  </a>
                </div>
              ) : null}

              <div className="my-1 h-px bg-white/10" />

              <div className="grid gap-3">
                <div className="text-xs uppercase tracking-wider text-white/60">Reply to topic</div>

                <div>
                  <label className="block text-xs uppercase tracking-wider text-white/60">Topic ID</label>
                  <input
                    type="number"
                    min={1}
                    value={replyTopicId}
                    onChange={(e) => setReplyTopicId(e.target.value)}
                    className="mt-2 w-full rounded-lg border border-white/10 bg-black/20 px-3 py-2 text-sm text-white outline-none focus:border-white/20"
                  />
                </div>

                <div>
                  <label className="block text-xs uppercase tracking-wider text-white/60">Reply content</label>
                  <textarea
                    value={replyContent}
                    onChange={(e) => setReplyContent(e.target.value)}
                    rows={4}
                    className="mt-2 w-full resize-none rounded-lg border border-white/10 bg-black/20 px-3 py-2 text-sm text-white outline-none focus:border-white/20"
                  />
                </div>
              </div>

              <div className="flex flex-wrap gap-3">
                <button
                  type="button"
                  onClick={shareReply}
                  disabled={replyBusy}
                  className="rounded-lg bg-[linear-gradient(to_top_right,#1f342f_0%,#526d5c_100%)] px-4 py-2.5 text-sm font-medium text-white border border-white/20 shadow-[0_8px_18px_rgba(0,0,0,0.28)] transition-all duration-150 hover:brightness-110 disabled:opacity-60 disabled:hover:brightness-100"
                >
                  {replyBusy ? "Posting…" : "Reply"}
                </button>
              </div>

              {replyError ? (
                <div className="rounded-lg border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                  <div>{replyError.message}</div>
                  {replyError.code ? <div className="mt-1 text-xs text-red-100/80">{replyError.code}</div> : null}
                </div>
              ) : null}

              {replyResult ? (
                <div className="rounded-lg border border-white/10 bg-white/5 px-4 py-3 text-sm text-white/80 space-y-2">
                  <div className="text-xs uppercase tracking-wider text-white/60">Reply posted</div>
                  <div className="text-xs text-white/60">
                    Topic ID: {replyResult.topicId} • Post ID: {replyResult.postId}
                  </div>
                  <a
                    href={replyResult.postUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-block mt-1 text-sm text-[#8fb3a7] hover:underline"
                  >
                    Open post →
                  </a>
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

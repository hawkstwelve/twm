import { useEffect, useMemo, useState } from "react";
import { CheckCircle2, ExternalLink, Loader2 } from "lucide-react";
import { Link, useSearchParams } from "react-router-dom";

type TwfStatus =
  | { linked: false }
  | { linked: true; member_id: number; display_name: string; photo_url?: string | null };

type ApiErrorInfo = {
  code?: string;
  message: string;
};

function getApiBase(): string {
  const fromEnv = (import.meta as ImportMeta & { env?: { VITE_API_BASE?: string } }).env?.VITE_API_BASE;
  const base = (fromEnv ?? "https://api.cartosky.com").trim();
  return base.replace(/\/$/, "");
}

async function readApiError(response: Response): Promise<ApiErrorInfo | null> {
  try {
    const body = (await response.json()) as unknown;
    if (!body || typeof body !== "object" || Array.isArray(body)) {
      return null;
    }
    const err = (body as { error?: unknown }).error;
    if (!err || typeof err !== "object" || Array.isArray(err)) {
      return null;
    }
    const message = typeof (err as { message?: unknown }).message === "string" ? (err as { message: string }).message.trim() : "";
    if (!message) {
      return null;
    }
    const code = typeof (err as { code?: unknown }).code === "string" ? (err as { code: string }).code.trim() : "";
    return { code: code || undefined, message };
  } catch {
    return null;
  }
}

function profileInitial(name: string): string {
  const trimmed = name.trim();
  return trimmed ? trimmed.charAt(0).toUpperCase() : "?";
}

export default function Login() {
  const apiBase = useMemo(() => getApiBase(), []);
  const [searchParams] = useSearchParams();

  const [status, setStatus] = useState<TwfStatus>({ linked: false });
  const [loadingStatus, setLoadingStatus] = useState(true);
  const [statusError, setStatusError] = useState<string | null>(null);
  const [logoutBusy, setLogoutBusy] = useState(false);
  const authState = searchParams.get("twf");
  const authMessage = searchParams.get("twf_message")?.trim() ?? "";
  const authSuccess = authState === "linked";
  const authFailure = authState === "error";

  useEffect(() => {
    const controller = new AbortController();

    setLoadingStatus(true);
    setStatusError(null);

    fetch(`${apiBase}/auth/twf/status`, {
      method: "GET",
      credentials: "include",
      signal: controller.signal,
    })
      .then(async (response) => {
        if (!response.ok) {
          const apiError = await readApiError(response);
          throw new Error(apiError?.message || `Status request failed (${response.status})`);
        }
        return (await response.json()) as TwfStatus;
      })
      .then((nextStatus) => setStatus(nextStatus))
      .catch((error: unknown) => {
        if ((error as { name?: string } | undefined)?.name === "AbortError") {
          return;
        }
        setStatus({ linked: false });
        setStatusError((error as Error).message || "Failed to load status");
      })
      .finally(() => setLoadingStatus(false));

    return () => controller.abort();
  }, [apiBase]);

  const connected = status.linked === true;

  function startTwfLogin() {
    window.location.href = `${apiBase}/auth/twf/start?return_to=${encodeURIComponent("/login")}`;
  }

  async function disconnectTwf() {
    setStatusError(null);
    setLogoutBusy(true);
    try {
      const response = await fetch(`${apiBase}/auth/twf/disconnect`, {
        method: "POST",
        credentials: "include",
      });
      if (!response.ok) {
        const apiError = await readApiError(response);
        throw new Error(apiError?.message || `Disconnect failed (${response.status})`);
      }
      setStatus({ linked: false });
    } catch (error: unknown) {
      setStatusError((error as Error).message || "Log out failed");
    } finally {
      setLogoutBusy(false);
    }
  }

  return (
    <div className="relative min-h-[calc(100vh-9rem)] overflow-hidden px-4 py-10 md:px-6 md:py-16">
      <div className="pointer-events-none absolute inset-0">
        <div className="absolute left-1/2 top-0 h-[32rem] w-[32rem] -translate-x-1/2 rounded-full bg-[#294137]/25 blur-3xl" />
        <div className="absolute bottom-0 left-1/2 h-[24rem] w-[24rem] -translate-x-1/2 rounded-full bg-[#7da08f]/10 blur-3xl" />
      </div>

      <div className="relative mx-auto flex min-h-[calc(100vh-13rem)] max-w-md items-center justify-center">
        <section className="w-full rounded-[28px] border border-white/12 bg-black/35 p-6 shadow-[0_20px_80px_rgba(0,0,0,0.48)] backdrop-blur-2xl md:p-8">
          <div className="space-y-3 text-center">
            <div className="text-[11px] font-semibold uppercase tracking-[0.28em] text-[#9bb4a8]">
              The Weather Forums
            </div>
            <h1 className="text-3xl font-semibold tracking-tight text-white md:text-4xl">
              Connect your account
            </h1>
            <p className="mx-auto max-w-sm text-sm leading-6 text-white/68">
              Sign in with The Weather Forums to connect your account with The Weather Models.
            </p>
          </div>

          <div className="mt-8 rounded-[24px] border border-white/10 bg-white/[0.045] p-5">
            {authSuccess ? (
              <div className="mb-4 flex items-center gap-2 rounded-xl border border-emerald-300/20 bg-emerald-400/10 px-4 py-3 text-sm text-emerald-50">
                <CheckCircle2 className="h-4 w-4" />
                Login complete.
              </div>
            ) : null}

            {authFailure ? (
              <div className="mb-4 rounded-xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                {authMessage || "Login failed. Please try again."}
              </div>
            ) : null}

            {loadingStatus ? (
              <div className="flex items-center justify-center gap-2 py-10 text-sm text-white/68">
                <Loader2 className="h-4 w-4 animate-spin" />
                Checking connection status...
              </div>
            ) : connected ? (
              <div className="space-y-5">
                <div className="inline-flex items-center gap-2 rounded-full border border-emerald-300/20 bg-emerald-400/10 px-3 py-1 text-[11px] font-medium text-emerald-100">
                  <CheckCircle2 className="h-3.5 w-3.5" />
                  Logged in with The Weather Forums
                </div>

                <div className="flex items-center gap-4 rounded-2xl border border-white/10 bg-black/25 p-4">
                  {status.photo_url ? (
                    <img
                      src={status.photo_url}
                      alt={`${status.display_name} profile`}
                      className="h-14 w-14 rounded-full border border-white/10 object-cover"
                    />
                  ) : (
                    <div className="flex h-14 w-14 items-center justify-center rounded-full border border-white/10 bg-white/10 text-lg font-semibold text-white">
                      {profileInitial(status.display_name)}
                    </div>
                  )}

                  <div className="min-w-0">
                    <div className="text-xs uppercase tracking-[0.22em] text-white/45">Account</div>
                    <div className="mt-1 truncate text-lg font-medium text-white">{status.display_name}</div>
                  </div>
                </div>

                <div className="flex flex-wrap items-center justify-between gap-3 pt-1">
                  <Link
                    to="/viewer"
                    className="inline-flex items-center gap-1.5 rounded-lg border border-white/15 bg-white/[0.06] px-3 py-2 text-sm font-medium text-white hover:bg-white/[0.1]"
                  >
                    Back to viewer
                    <ExternalLink className="h-3.5 w-3.5" />
                  </Link>

                  <button
                    type="button"
                    onClick={() => {
                      void disconnectTwf();
                    }}
                    disabled={logoutBusy}
                    className="inline-flex items-center rounded-lg border border-white/12 bg-transparent px-3 py-2 text-sm text-white/74 hover:bg-white/[0.06] hover:text-white disabled:opacity-60"
                  >
                    {logoutBusy ? "Logging out..." : "Log out"}
                  </button>
                </div>
              </div>
            ) : (
              <div className="space-y-5">
                <div className="rounded-2xl border border-white/10 bg-black/25 p-4 text-sm leading-6 text-white/72">
                  You’ll be redirected to The Weather Forums to authorize The Weather Models, then returned here.
                </div>

                <button
                  type="button"
                  onClick={startTwfLogin}
                  className="w-full rounded-xl border border-white/20 bg-[linear-gradient(135deg,#1f342f_0%,#526d5c_100%)] px-4 py-3 text-sm font-semibold text-white shadow-[0_10px_24px_rgba(0,0,0,0.28)] transition-all duration-150 hover:brightness-110"
                >
                  Log in with The Weather Forums
                </button>
              </div>
            )}

            {statusError ? (
              <div className="mt-4 rounded-xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                {statusError}
              </div>
            ) : null}
          </div>
        </section>
      </div>
    </div>
  );
}

import { useEffect, useMemo, useState } from "react";
import { CheckCircle2, Copy, ExternalLink, Loader2, Send, X } from "lucide-react";

import { API_ORIGIN } from "@/lib/config";
import { getSharePrefs, setSharePrefs, type SharePrefs } from "@/lib/share_prefs";

export type SharePayload = {
  permalink: string;
  summary: string;
  detailsSummary?: string;
};

type TwfStatus =
  | { linked: false }
  | { linked: true; member_id: number; display_name: string; photo_url?: string | null };

type TwfForum = {
  id: number;
  name: string;
  path?: string;
};

type TwfTopic = {
  id: number;
  title: string;
  url: string;
  pinned: boolean;
  updated?: string;
  starter?: string;
};

type ApiErrorInfo = {
  code?: string;
  message: string;
};

type ShareTarget = {
  id: string;
  label: string;
};

type SharePostResult = {
  postId: number;
  postUrl: string;
  topicId: number;
};

type TwfShareModalProps = {
  open: boolean;
  onClose: () => void;
  payload: SharePayload;
};

const QUICK_FORUMS: Array<{ id: number; label: string }> = [
  { id: 4, label: "West" },
  { id: 9, label: "East" },
];

function isObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function normalizeTwfStatus(value: unknown): TwfStatus {
  if (!isObject(value) || value.linked !== true) {
    return { linked: false };
  }
  const memberId = Number(value.member_id);
  const displayName = typeof value.display_name === "string" ? value.display_name.trim() : "";
  if (!Number.isFinite(memberId) || memberId <= 0 || !displayName) {
    return { linked: false };
  }
  const photoUrl = typeof value.photo_url === "string" && value.photo_url.trim() ? value.photo_url.trim() : undefined;
  return {
    linked: true,
    member_id: memberId,
    display_name: displayName,
    photo_url: photoUrl,
  };
}

async function readApiError(response: Response): Promise<ApiErrorInfo | null> {
  try {
    const body = (await response.json()) as unknown;
    if (!isObject(body)) {
      return null;
    }
    const err = body.error;
    if (!isObject(err)) {
      return null;
    }
    const message = typeof err.message === "string" ? err.message.trim() : "";
    if (!message) {
      return null;
    }
    const code = typeof err.code === "string" && err.code.trim() ? err.code.trim() : undefined;
    return { code, message };
  } catch {
    return null;
  }
}

function normalizeForums(value: unknown): TwfForum[] {
  const list = Array.isArray(value)
    ? value
    : isObject(value) && Array.isArray(value.results)
    ? value.results
    : isObject(value) && Array.isArray(value.forums)
    ? value.forums
    : [];

  const normalized: TwfForum[] = [];
  for (const entry of list) {
    if (!isObject(entry)) {
      continue;
    }
    const id = Number(entry.id);
    const name = typeof entry.name === "string" ? entry.name.trim() : "";
    if (!Number.isFinite(id) || id <= 0 || !name) {
      continue;
    }
    const path = typeof entry.path === "string" && entry.path.trim() ? entry.path.trim() : undefined;
    if (path) {
      normalized.push({ id, name, path });
      continue;
    }
    normalized.push({ id, name });
  }

  normalized.sort((a, b) => (a.path ?? a.name).localeCompare(b.path ?? b.name));
  return normalized;
}

function normalizeTopics(value: unknown): TwfTopic[] {
  if (!isObject(value) || !Array.isArray(value.results)) {
    return [];
  }
  const normalized: TwfTopic[] = [];
  for (const entry of value.results) {
    if (!isObject(entry)) {
      continue;
    }
    const id = Number(entry.id);
    const title = typeof entry.title === "string" ? entry.title.trim() : "";
    const url = typeof entry.url === "string" ? entry.url.trim() : "";
    if (!Number.isFinite(id) || id <= 0 || !title || !url) {
      continue;
    }
    const updated = typeof entry.updated === "string" && entry.updated.trim() ? entry.updated.trim() : undefined;
    const starter = typeof entry.starter === "string" && entry.starter.trim() ? entry.starter.trim() : undefined;
    const topic: TwfTopic = {
      id,
      title,
      url,
      pinned: entry.pinned === true,
    };
    if (updated) {
      topic.updated = updated;
    }
    if (starter) {
      topic.starter = starter;
    }
    normalized.push(topic);
  }
  return normalized;
}

function isQuickForumId(forumId: number): boolean {
  return QUICK_FORUMS.some((entry) => entry.id === forumId);
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function resolveMonthlyTopicId(topics: TwfTopic[]): number | null {
  if (topics.length === 0) {
    return null;
  }
  const formatter = new Intl.DateTimeFormat("en-US", { month: "long" });
  const monthsToTry = [0, 1].map((offset) => {
    const date = new Date();
    date.setDate(1);
    date.setMonth(date.getMonth() - offset);
    return {
      monthName: formatter.format(date),
      year: date.getFullYear(),
    };
  });

  for (const candidate of monthsToTry) {
    const rx = new RegExp(`^\\s*${escapeRegex(candidate.monthName)}\\s+${candidate.year}\\b`, "i");
    const match = topics.find((topic) => rx.test(topic.title.replace(/\s+/g, " ")));
    if (match) {
      return match.id;
    }
  }

  const firstPinned = topics.find((topic) => topic.pinned);
  if (firstPinned) {
    return firstPinned.id;
  }
  return topics[0]?.id ?? null;
}

function parseTopicIdFromUrl(rawValue: string): number | null {
  const value = rawValue.trim();
  if (!value) {
    return null;
  }
  if (/^\d+$/.test(value)) {
    const topicId = Number(value);
    return Number.isFinite(topicId) && topicId > 0 ? topicId : null;
  }
  const topicPath = value.match(/\/topic\/(\d+)(?:[/-]|$)/i);
  const showTopic = value.match(/[?&](?:showtopic|topic|t)=(\d+)/i);
  const resolved = topicPath?.[1] ?? showTopic?.[1];
  if (!resolved) {
    return null;
  }
  const topicId = Number(resolved);
  if (!Number.isFinite(topicId) || topicId <= 0) {
    return null;
  }
  return topicId;
}

function forumIdFromPrefs(prefs: SharePrefs): number {
  if (Number.isFinite(prefs.forumId) && Number(prefs.forumId) > 0) {
    return Number(prefs.forumId);
  }
  return prefs.forumMode === "east" ? QUICK_FORUMS[1].id : QUICK_FORUMS[0].id;
}

function forumModeFromSelection(
  selectedForumId: number,
  showOtherForums: boolean
): SharePrefs["forumMode"] {
  if (showOtherForums || !isQuickForumId(selectedForumId)) {
    return "other";
  }
  return selectedForumId === QUICK_FORUMS[1].id ? "east" : "west";
}

async function writeClipboard(text: string): Promise<boolean> {
  if (typeof navigator === "undefined" || !navigator.clipboard?.writeText) {
    return false;
  }
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    return false;
  }
}

export function TwfShareModal({ open, onClose, payload }: TwfShareModalProps) {
  const initialSharePrefs = useMemo(() => getSharePrefs(), []);
  const targets = useMemo<ShareTarget[]>(
    () => [{ id: "twf", label: "The Weather Forums" }],
    []
  );
  const [activeTargetId, setActiveTargetId] = useState<string>(targets[0].id);
  const [twfStatus, setTwfStatus] = useState<TwfStatus>({ linked: false });
  const [statusLoading, setStatusLoading] = useState(false);
  const [statusError, setStatusError] = useState<string | null>(null);

  const [selectedForumId, setSelectedForumId] = useState<number>(() => forumIdFromPrefs(initialSharePrefs));
  const [showOtherForums, setShowOtherForums] = useState(
    () => initialSharePrefs.forumMode === "other" || !isQuickForumId(forumIdFromPrefs(initialSharePrefs))
  );
  const [forums, setForums] = useState<TwfForum[]>([]);
  const [forumsLoading, setForumsLoading] = useState(false);
  const [forumsError, setForumsError] = useState<string | null>(null);

  const [topics, setTopics] = useState<TwfTopic[]>([]);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [topicsError, setTopicsError] = useState<string | null>(null);
  const [topicSearch, setTopicSearch] = useState("");
  const [selectedTopicId, setSelectedTopicId] = useState<number | null>(initialSharePrefs.topicId ?? null);
  const [pastedTopicUrl, setPastedTopicUrl] = useState("");

  const [content, setContent] = useState("");
  const [submitBusy, setSubmitBusy] = useState(false);
  const [submitError, setSubmitError] = useState<ApiErrorInfo | null>(null);
  const [retryAfterSeconds, setRetryAfterSeconds] = useState<number | null>(null);
  const [submitSuccess, setSubmitSuccess] = useState<SharePostResult | null>(null);
  const [clipboardStatus, setClipboardStatus] = useState<string | null>(null);
  const [showDetailsSummary, setShowDetailsSummary] = useState(false);

  const parsedTopicIdFromUrl = useMemo(() => parseTopicIdFromUrl(pastedTopicUrl), [pastedTopicUrl]);
  const pastedTopicUrlHasValue = pastedTopicUrl.trim().length > 0;
  const pastedTopicUrlError =
    pastedTopicUrlHasValue && parsedTopicIdFromUrl === null
      ? "Could not parse a numeric topic ID from that URL."
      : null;
  const effectiveTopicId = parsedTopicIdFromUrl ?? selectedTopicId;

  const topicOptions = useMemo(() => {
    const search = topicSearch.trim().toLowerCase();
    if (!search) {
      return topics;
    }
    return topics.filter((topic) => topic.title.toLowerCase().includes(search));
  }, [topics, topicSearch]);

  const defaultContent = useMemo(() => {
    return `Link to Viewer: ${payload.permalink}\nSummary: ${payload.summary}`;
  }, [payload.permalink, payload.summary]);

  useEffect(() => {
    if (!open) {
      return;
    }
    const prefs = getSharePrefs();
    const persistedForumId = forumIdFromPrefs(prefs);
    setSelectedForumId(persistedForumId);
    setShowOtherForums(prefs.forumMode === "other" || !isQuickForumId(persistedForumId));
    setSelectedTopicId(prefs.topicId ?? null);
    setContent(defaultContent);
    setSubmitError(null);
    setSubmitSuccess(null);
    setRetryAfterSeconds(null);
    setClipboardStatus(null);
    setShowDetailsSummary(false);
    setPastedTopicUrl("");
    setTopicSearch("");
  }, [open, defaultContent]);

  useEffect(() => {
    if (!open) {
      return;
    }
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
      document.body.style.overflow = previousOverflow;
    };
  }, [open, onClose]);

  useEffect(() => {
    if (!open || activeTargetId !== "twf") {
      return;
    }

    const controller = new AbortController();
    setStatusLoading(true);
    setStatusError(null);

    fetch(`${API_ORIGIN}/auth/twf/status`, {
      method: "GET",
      credentials: "include",
      signal: controller.signal,
    })
      .then(async (response) => {
        if (!response.ok) {
          const apiError = await readApiError(response);
          throw new Error(apiError?.message || `Status request failed (${response.status})`);
        }
        return response.json() as Promise<unknown>;
      })
      .then((value) => setTwfStatus(normalizeTwfStatus(value)))
      .catch((error: unknown) => {
        if ((error as { name?: string } | undefined)?.name === "AbortError") {
          return;
        }
        setTwfStatus({ linked: false });
        setStatusError((error as Error).message || "Failed to load TWF account status.");
      })
      .finally(() => setStatusLoading(false));

    return () => controller.abort();
  }, [open, activeTargetId]);

  useEffect(() => {
    setSharePrefs({
      forumMode: forumModeFromSelection(selectedForumId, showOtherForums),
      forumId: selectedForumId > 0 ? selectedForumId : undefined,
      topicId: selectedTopicId ?? undefined,
    });
  }, [selectedForumId, showOtherForums, selectedTopicId]);

  useEffect(() => {
    if (!open || twfStatus.linked !== true || !showOtherForums) {
      return;
    }

    const controller = new AbortController();
    setForumsLoading(true);
    setForumsError(null);

    fetch(`${API_ORIGIN}/twf/forums`, {
      method: "GET",
      credentials: "include",
      signal: controller.signal,
    })
      .then(async (response) => {
        if (!response.ok) {
          const apiError = await readApiError(response);
          throw new Error(apiError?.message || `Forum request failed (${response.status})`);
        }
        return response.json() as Promise<unknown>;
      })
      .then((value) => {
        const normalized = normalizeForums(value);
        setForums(normalized);
        if (!isQuickForumId(selectedForumId) && !normalized.some((forum) => forum.id === selectedForumId)) {
          const fallbackId = normalized[0]?.id ?? QUICK_FORUMS[0].id;
          setSelectedForumId(fallbackId);
        }
      })
      .catch((error: unknown) => {
        if ((error as { name?: string } | undefined)?.name === "AbortError") {
          return;
        }
        setForums([]);
        setForumsError((error as Error).message || "Failed to load forums.");
      })
      .finally(() => setForumsLoading(false));

    return () => controller.abort();
  }, [open, twfStatus, showOtherForums, selectedForumId]);

  useEffect(() => {
    if (!open || twfStatus.linked !== true || selectedForumId <= 0) {
      setTopics([]);
      setSelectedTopicId(null);
      setTopicsError(null);
      setTopicsLoading(false);
      return;
    }

    const controller = new AbortController();
    const params = new URLSearchParams({
      forum_id: String(selectedForumId),
      limit: "15",
    });
    setTopicsLoading(true);
    setTopicsError(null);
    setSubmitSuccess(null);

    fetch(`${API_ORIGIN}/twf/topics?${params.toString()}`, {
      method: "GET",
      credentials: "include",
      signal: controller.signal,
    })
      .then(async (response) => {
        if (!response.ok) {
          const apiError = await readApiError(response);
          throw new Error(apiError?.message || `Topics request failed (${response.status})`);
        }
        return response.json() as Promise<unknown>;
      })
      .then((value) => {
        const normalized = normalizeTopics(value);
        setTopics(normalized);
        const savedTopicId = getSharePrefs().topicId;
        if (savedTopicId && normalized.some((topic) => topic.id === savedTopicId)) {
          setSelectedTopicId(savedTopicId);
          return;
        }
        setSelectedTopicId(resolveMonthlyTopicId(normalized));
      })
      .catch((error: unknown) => {
        if ((error as { name?: string } | undefined)?.name === "AbortError") {
          return;
        }
        setTopics([]);
        setSelectedTopicId(null);
        setTopicsError((error as Error).message || "Failed to load topics.");
      })
      .finally(() => setTopicsLoading(false));

    return () => controller.abort();
  }, [open, twfStatus, selectedForumId]);

  const handleCopy = async (kind: "link" | "summary") => {
    const text = kind === "link" ? payload.permalink : payload.summary;
    const ok = await writeClipboard(text);
    setClipboardStatus(ok ? `${kind === "link" ? "Link" : "Summary"} copied` : "Clipboard unavailable");
  };

  const handleSubmitPost = async () => {
    setSubmitError(null);
    setSubmitSuccess(null);
    setRetryAfterSeconds(null);

    if (twfStatus.linked !== true) {
      setSubmitError({ message: "Connect your TWF account before posting." });
      return;
    }
    if (!Number.isFinite(effectiveTopicId) || Number(effectiveTopicId) <= 0) {
      setSubmitError({ message: "Select a topic or paste a valid topic URL." });
      return;
    }
    const trimmedContent = content.trim();
    if (!trimmedContent) {
      setSubmitError({ message: "Post content is required." });
      return;
    }

    setSubmitBusy(true);
    try {
      const response = await fetch(`${API_ORIGIN}/twf/share/post`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          topic_id: Number(effectiveTopicId),
          content: trimmedContent,
        }),
      });

      if (!response.ok) {
        const apiError = await readApiError(response);
        if (response.status === 429) {
          const retryAfter = Number(response.headers.get("Retry-After"));
          if (Number.isFinite(retryAfter) && retryAfter > 0) {
            setRetryAfterSeconds(Math.max(1, Math.floor(retryAfter)));
          }
        }
        setSubmitError(apiError ?? { message: "Request failed. Please try again." });
        return;
      }

      const result = (await response.json()) as SharePostResult;
      if (!Number.isFinite(Number(result.postId)) || typeof result.postUrl !== "string") {
        setSubmitError({ message: "Unexpected response from server." });
        return;
      }
      setSubmitSuccess(result);
    } catch {
      setSubmitError({ message: "Request failed. Please try again." });
    } finally {
      setSubmitBusy(false);
    }
  };

  if (!open) {
    return null;
  }

  return (
    <div
      className="fixed inset-0 z-[80] flex items-center justify-center bg-black/65 p-4 backdrop-blur-sm"
      role="dialog"
      aria-modal="true"
      aria-label="Share"
      onClick={onClose}
    >
      <div
        className="w-full max-w-3xl rounded-2xl border border-white/15 bg-black/85 shadow-[0_20px_52px_rgba(0,0,0,0.72)]"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-center justify-between border-b border-white/10 px-4 py-3">
          <div>
            <div className="text-sm font-semibold text-white">Share</div>
            <div className="text-xs text-white/60">Copy permalink/summary or post directly to a target.</div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-white/15 bg-white/5 text-white/80 hover:bg-white/10"
            aria-label="Close share modal"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="space-y-4 px-4 py-4">
          <div className="rounded-xl border border-white/10 bg-white/[0.04] p-3">
            <div className="mb-2 text-xs font-semibold uppercase tracking-wider text-white/65">Payload</div>
            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                onClick={() => {
                  void handleCopy("link");
                }}
                className="inline-flex h-8 items-center gap-1.5 rounded-md border border-white/15 bg-black/25 px-2.5 text-xs font-medium text-white hover:bg-black/35"
              >
                <Copy className="h-3.5 w-3.5" />
                Copy link
              </button>
              <button
                type="button"
                onClick={() => {
                  void handleCopy("summary");
                }}
                className="inline-flex h-8 items-center gap-1.5 rounded-md border border-white/15 bg-black/25 px-2.5 text-xs font-medium text-white hover:bg-black/35"
              >
                <Copy className="h-3.5 w-3.5" />
                Copy summary
              </button>
              {clipboardStatus ? <span className="text-xs text-emerald-200/90">{clipboardStatus}</span> : null}
            </div>
            <div className="mt-2 text-xs text-white/65">
              <div className="truncate">Link to Viewer: {payload.permalink}</div>
              <div className={showDetailsSummary ? "" : "line-clamp-2"}>Summary: {payload.summary}</div>
              {payload.detailsSummary ? (
                <>
                  <button
                    type="button"
                    onClick={() => setShowDetailsSummary((current) => !current)}
                    className="mt-1 text-[11px] font-medium text-emerald-200/90 hover:text-emerald-100"
                  >
                    {showDetailsSummary ? "Hide details" : "Show details"}
                  </button>
                  {showDetailsSummary ? (
                    <div className="mt-1 text-[11px] text-white/60">{payload.detailsSummary}</div>
                  ) : null}
                </>
              ) : null}
            </div>
          </div>

          <div className="rounded-xl border border-white/10 bg-white/[0.04] p-3">
            <div className="mb-2 text-xs font-semibold uppercase tracking-wider text-white/65">Target</div>
            <div className="flex flex-wrap items-center gap-2">
              {targets.map((target) => (
                <button
                  key={target.id}
                  type="button"
                  onClick={() => setActiveTargetId(target.id)}
                  className={[
                    "inline-flex h-8 items-center rounded-md border px-2.5 text-xs font-medium",
                    activeTargetId === target.id
                      ? "border-emerald-300/35 bg-emerald-400/20 text-emerald-50"
                      : "border-white/15 bg-black/25 text-white/80 hover:bg-black/35",
                  ].join(" ")}
                >
                  {target.label}
                </button>
              ))}
            </div>
          </div>

          {activeTargetId === "twf" ? (
            <div className="rounded-xl border border-white/10 bg-white/[0.04] p-3">
              <div className="mb-3 text-sm font-semibold text-white">Post to The Weather Forums</div>

              {statusLoading ? (
                <div className="flex items-center gap-2 text-sm text-white/70">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Checking TWF connection...
                </div>
              ) : twfStatus.linked !== true ? (
                <div className="space-y-3">
                  <div className="text-sm text-white/70">
                    Connect your TWF account to post directly. You can still copy the permalink and summary above.
                  </div>
                  <div className="flex items-center gap-2">
                    <a
                      href={`${API_ORIGIN}/auth/twf/start`}
                      className="inline-flex h-8 items-center rounded-md border border-emerald-300/25 bg-[linear-gradient(to_top_right,#1f342f_0%,#526d5c_100%)] px-2.5 text-xs font-semibold text-emerald-50 hover:brightness-110"
                    >
                      Connect TWF
                    </a>
                    {statusError ? <span className="text-xs text-red-200">{statusError}</span> : null}
                  </div>
                </div>
              ) : submitSuccess ? (
                <div className="space-y-3">
                  <div className="flex items-center gap-2 rounded-lg border border-emerald-300/20 bg-emerald-400/10 px-3 py-2 text-sm text-emerald-50">
                    <CheckCircle2 className="h-4 w-4" />
                    Post created successfully.
                  </div>
                  <div className="text-xs text-white/70">
                    Topic ID: {submitSuccess.topicId} - Post ID: {submitSuccess.postId}
                  </div>
                  <div className="flex flex-wrap items-center gap-2">
                    <a
                      href={submitSuccess.postUrl}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex h-8 items-center gap-1.5 rounded-md border border-white/15 bg-black/25 px-2.5 text-xs font-medium text-white hover:bg-black/35"
                    >
                      <ExternalLink className="h-3.5 w-3.5" />
                      Open post
                    </a>
                    <button
                      type="button"
                      onClick={onClose}
                      className="inline-flex h-8 items-center rounded-md border border-white/15 bg-black/25 px-2.5 text-xs font-medium text-white hover:bg-black/35"
                    >
                      Close
                    </button>
                  </div>
                </div>
              ) : (
                <div className="space-y-4">
                  <div className="grid gap-2">
                    <div>
                      <div className="mb-1 text-xs uppercase tracking-wider text-white/60">Forum</div>
                      <div className="flex flex-wrap items-center gap-2">
                        {QUICK_FORUMS.map((forum) => (
                          <button
                            key={forum.id}
                            type="button"
                            onClick={() => {
                              setSelectedForumId(forum.id);
                              setShowOtherForums(false);
                            }}
                            className={[
                              "inline-flex h-8 items-center rounded-md border px-2.5 text-xs font-medium",
                              selectedForumId === forum.id && !showOtherForums
                                ? "border-emerald-300/35 bg-emerald-400/20 text-emerald-50"
                                : "border-white/15 bg-black/25 text-white/80 hover:bg-black/35",
                            ].join(" ")}
                          >
                            {forum.label}
                          </button>
                        ))}
                        <button
                          type="button"
                          onClick={() => setShowOtherForums((current) => !current)}
                          className={[
                            "inline-flex h-8 items-center rounded-md border px-2.5 text-xs font-medium",
                            showOtherForums
                              ? "border-emerald-300/35 bg-emerald-400/20 text-emerald-50"
                              : "border-white/15 bg-black/25 text-white/80 hover:bg-black/35",
                          ].join(" ")}
                        >
                          Other forum...
                        </button>
                      </div>
                      {showOtherForums ? (
                        <div className="mt-2 space-y-1">
                          {forumsLoading ? (
                            <div className="text-xs text-white/65">Loading forums...</div>
                          ) : forums.length > 0 ? (
                            <select
                              value={String(selectedForumId)}
                              onChange={(event) => setSelectedForumId(Number(event.target.value))}
                              className="h-8 w-full rounded-md border border-white/15 bg-black/35 px-2 text-xs text-white outline-none focus:border-emerald-300/40"
                            >
                              {forums.map((forum) => (
                                <option key={forum.id} value={String(forum.id)}>
                                  {(forum.path ?? forum.name) + ` (ID ${forum.id})`}
                                </option>
                              ))}
                            </select>
                          ) : (
                            <div className="text-xs text-white/65">No accessible forums found.</div>
                          )}
                          {forumsError ? <div className="text-xs text-red-200">{forumsError}</div> : null}
                        </div>
                      ) : null}
                    </div>

                    <div>
                      <div className="mb-1 text-xs uppercase tracking-wider text-white/60">Topic</div>
                      <input
                        value={topicSearch}
                        onChange={(event) => setTopicSearch(event.target.value)}
                        placeholder="Search loaded topics"
                        className="mb-2 h-8 w-full rounded-md border border-white/15 bg-black/35 px-2 text-xs text-white outline-none placeholder:text-white/40 focus:border-emerald-300/40"
                      />
                      {topicsLoading ? (
                        <div className="flex items-center gap-2 text-xs text-white/70">
                          <Loader2 className="h-3.5 w-3.5 animate-spin" />
                          Loading topics...
                        </div>
                      ) : topicOptions.length > 0 ? (
                        <select
                          value={selectedTopicId !== null ? String(selectedTopicId) : ""}
                          onChange={(event) => setSelectedTopicId(Number(event.target.value))}
                          className="h-8 w-full rounded-md border border-white/15 bg-black/35 px-2 text-xs text-white outline-none focus:border-emerald-300/40"
                        >
                          {topicOptions.map((topic) => (
                            <option key={topic.id} value={String(topic.id)}>
                              {(topic.pinned ? "[PIN] " : "") + topic.title}
                            </option>
                          ))}
                        </select>
                      ) : (
                        <div className="text-xs text-white/65">No topics loaded for this forum.</div>
                      )}
                      {topicsError ? <div className="mt-1 text-xs text-red-200">{topicsError}</div> : null}
                    </div>

                    <div>
                      <div className="mb-1 text-xs uppercase tracking-wider text-white/60">Paste topic URL (optional)</div>
                      <input
                        value={pastedTopicUrl}
                        onChange={(event) => setPastedTopicUrl(event.target.value)}
                        placeholder="https://www.theweatherforums.com/topic/123..."
                        className="h-8 w-full rounded-md border border-white/15 bg-black/35 px-2 text-xs text-white outline-none placeholder:text-white/40 focus:border-emerald-300/40"
                      />
                      {pastedTopicUrlError ? <div className="mt-1 text-xs text-red-200">{pastedTopicUrlError}</div> : null}
                      {parsedTopicIdFromUrl ? (
                        <div className="mt-1 text-xs text-emerald-200/90">Using topic ID {parsedTopicIdFromUrl} from URL.</div>
                      ) : null}
                    </div>

                    <div>
                      <div className="mb-1 text-xs uppercase tracking-wider text-white/60">Post content</div>
                      <textarea
                        value={content}
                        onChange={(event) => setContent(event.target.value)}
                        rows={6}
                        className="w-full rounded-md border border-white/15 bg-black/35 px-2 py-2 text-xs text-white outline-none focus:border-emerald-300/40"
                      />
                    </div>
                  </div>

                  {submitError ? (
                    <div className="rounded-lg border border-red-400/25 bg-red-500/10 px-3 py-2 text-xs text-red-100">
                      <div>{submitError.message}</div>
                      {submitError.code ? <div className="mt-0.5 text-[11px] opacity-90">Code: {submitError.code}</div> : null}
                      {retryAfterSeconds ? <div className="mt-0.5 text-[11px] opacity-90">Try again in {retryAfterSeconds}s.</div> : null}
                    </div>
                  ) : null}

                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="text-xs text-white/60">
                      {effectiveTopicId ? `Posting to topic ID ${effectiveTopicId}` : "Select or paste a topic to post"}
                    </div>
                    <button
                      type="button"
                      onClick={() => {
                        void handleSubmitPost();
                      }}
                      disabled={submitBusy}
                      className="inline-flex h-8 items-center gap-1.5 rounded-md border border-emerald-300/25 bg-[linear-gradient(to_top_right,#1f342f_0%,#526d5c_100%)] px-2.5 text-xs font-semibold text-emerald-50 hover:brightness-110 disabled:opacity-60 disabled:hover:brightness-100"
                    >
                      {submitBusy ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Send className="h-3.5 w-3.5" />}
                      {submitBusy ? "Posting..." : "Post to TWF"}
                    </button>
                  </div>
                </div>
              )}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

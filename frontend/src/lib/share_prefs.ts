export type SharePrefs = {
  forumMode: "west" | "east" | "other";
  forumId?: number;
  topicId?: number;
};

const SHARE_PREFS_STORAGE_KEY = "twm.share_prefs.v1";

function sanitizePositiveInt(value: unknown): number | undefined {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return undefined;
  }
  return Math.floor(parsed);
}

function sanitizeForumMode(value: unknown): SharePrefs["forumMode"] {
  if (value === "east" || value === "other") {
    return value;
  }
  return "west";
}

function sanitizeSharePrefs(value: unknown): SharePrefs {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return { forumMode: "west" };
  }
  const record = value as Record<string, unknown>;
  const prefs: SharePrefs = {
    forumMode: sanitizeForumMode(record.forumMode),
  };
  const forumId = sanitizePositiveInt(record.forumId);
  if (forumId !== undefined) {
    prefs.forumId = forumId;
  }
  const topicId = sanitizePositiveInt(record.topicId);
  if (topicId !== undefined) {
    prefs.topicId = topicId;
  }
  return prefs;
}

export function getSharePrefs(): SharePrefs {
  if (typeof window === "undefined") {
    return { forumMode: "west" };
  }
  try {
    const raw = window.localStorage.getItem(SHARE_PREFS_STORAGE_KEY);
    if (!raw) {
      return { forumMode: "west" };
    }
    const parsed = JSON.parse(raw) as unknown;
    return sanitizeSharePrefs(parsed);
  } catch {
    return { forumMode: "west" };
  }
}

export function setSharePrefs(next: SharePrefs): void {
  if (typeof window === "undefined") {
    return;
  }
  try {
    const sanitized = sanitizeSharePrefs(next);
    // TODO: Back this abstraction with server-side TWM account prefs for cross-device sync;
    // keep consumers (modal UI) unchanged.
    window.localStorage.setItem(SHARE_PREFS_STORAGE_KEY, JSON.stringify(sanitized));
  } catch {
    // Ignore storage write errors.
  }
}

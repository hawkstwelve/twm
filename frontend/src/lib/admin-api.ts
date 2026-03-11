import { API_ORIGIN } from "@/lib/config";

export type TwfStatus = {
  linked: boolean;
  admin: boolean;
  member_id?: number;
  display_name?: string;
  photo_url?: string | null;
};

export type PerfMetricSummary = {
  count: number;
  avg_ms: number | null;
  min_ms: number | null;
  max_ms: number | null;
  p50_ms: number | null;
  p95_ms: number | null;
  target_ms: number | null;
};

export type PerfSummaryResponse = {
  window: string;
  filters: {
    device: string | null;
    model: string | null;
    variable: string | null;
  };
  metrics: Record<string, PerfMetricSummary>;
};

export type PerfTimeseriesPoint = PerfMetricSummary & {
  bucket_start: string;
};

export type PerfTimeseriesResponse = {
  metric: string;
  window: string;
  bucket: "hour" | "day";
  filters: {
    device: string | null;
    model: string | null;
    variable: string | null;
  };
  points: PerfTimeseriesPoint[];
};

export type PerfBreakdownItem = PerfMetricSummary & {
  key: string;
};

export type PerfBreakdownResponse = {
  metric: string;
  window: string;
  by: string;
  filters: {
    device: string | null;
    model: string | null;
    variable: string | null;
  };
  items: PerfBreakdownItem[];
};

export type UsageSummaryResponse = {
  window: string;
  events: Array<{
    event_name: string;
    count: number;
  }>;
};

export type VerificationAutoChecks = {
  has_valid_pixels?: boolean;
  range_present?: boolean;
  coverage_present?: boolean;
  monotonic?: boolean | null;
};

export type VerificationDiagnostics = {
  monotonic?: {
    ok?: boolean;
    reason?: string;
    decreased_pixel_count?: number;
    decreased_fraction?: number;
    max_decrease?: number;
    max_increase?: number;
    max_decrease_lon?: number | null;
    max_decrease_lat?: number | null;
  } | null;
};

export type VerificationSummaryResponse = {
  window: string;
  filters: {
    model: string | null;
    variable: string | null;
  };
  total_rows: number;
  auto_pass_rows: number;
  manual_review_rows: number;
  flagged_rows: number;
};

export type VerificationResult = {
  id: number;
  created_at: number;
  updated_at: number;
  model_id: string;
  variable_id: string;
  run_id: string;
  forecast_hour: number;
  auto_status: "pass" | "warning";
  manual_status: "review" | "pass" | "fail";
  benchmark_site?: string | null;
  reviewer_name?: string | null;
  reviewer_member_id?: number | null;
  notes?: string | null;
  auto_checks: VerificationAutoChecks;
  diagnostics: VerificationDiagnostics;
  coverage_fraction?: number | null;
  valid_pixel_count: number;
  total_pixel_count: number;
  range_min?: number | null;
  range_max?: number | null;
  warning_summary?: string | null;
  severity: string;
  last_checked_at: number;
};

export type VerificationResultsResponse = {
  window: string;
  filters: {
    model: string | null;
    variable: string | null;
    manual_status: string | null;
    flagged_only: boolean;
    attention_only: boolean;
  };
  results: VerificationResult[];
};

async function fetchJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    credentials: "include",
    ...init,
  });
  if (!response.ok) {
    let message = `Request failed (${response.status})`;
    try {
      const body = (await response.json()) as { error?: { message?: string } };
      if (body?.error?.message) {
        message = body.error.message;
      }
    } catch {
      // Ignore parse failures.
    }
    throw new Error(message);
  }
  return (await response.json()) as T;
}

export async function fetchTwfStatus(): Promise<TwfStatus> {
  return fetchJson<TwfStatus>(`${API_ORIGIN}/auth/twf/status`);
}

export async function fetchAdminPerfSummary(params: {
  window: string;
  device?: string;
  model?: string;
  variable?: string;
}): Promise<PerfSummaryResponse> {
  const search = new URLSearchParams();
  search.set("window", params.window);
  if (params.device && params.device !== "all") search.set("device", params.device);
  if (params.model && params.model !== "all") search.set("model", params.model);
  if (params.variable && params.variable !== "all") search.set("variable", params.variable);
  return fetchJson<PerfSummaryResponse>(`${API_ORIGIN}/api/v4/admin/performance/summary?${search.toString()}`);
}

export async function fetchAdminPerfTimeseries(params: {
  metric: string;
  window: string;
  bucket?: string;
  device?: string;
  model?: string;
  variable?: string;
}): Promise<PerfTimeseriesResponse> {
  const search = new URLSearchParams();
  search.set("metric", params.metric);
  search.set("window", params.window);
  if (params.bucket) search.set("bucket", params.bucket);
  if (params.device && params.device !== "all") search.set("device", params.device);
  if (params.model && params.model !== "all") search.set("model", params.model);
  if (params.variable && params.variable !== "all") search.set("variable", params.variable);
  return fetchJson<PerfTimeseriesResponse>(`${API_ORIGIN}/api/v4/admin/performance/timeseries?${search.toString()}`);
}

export async function fetchAdminPerfBreakdown(params: {
  metric: string;
  by: string;
  window: string;
  device?: string;
  model?: string;
  variable?: string;
  limit?: number;
}): Promise<PerfBreakdownResponse> {
  const search = new URLSearchParams();
  search.set("metric", params.metric);
  search.set("by", params.by);
  search.set("window", params.window);
  if (params.limit) search.set("limit", String(params.limit));
  if (params.device && params.device !== "all") search.set("device", params.device);
  if (params.model && params.model !== "all") search.set("model", params.model);
  if (params.variable && params.variable !== "all") search.set("variable", params.variable);
  return fetchJson<PerfBreakdownResponse>(`${API_ORIGIN}/api/v4/admin/performance/breakdown?${search.toString()}`);
}

export async function fetchAdminUsageSummary(window: string): Promise<UsageSummaryResponse> {
  const search = new URLSearchParams();
  search.set("window", window);
  return fetchJson<UsageSummaryResponse>(`${API_ORIGIN}/api/v4/admin/usage/summary?${search.toString()}`);
}

export async function fetchAdminVerificationSummary(params: {
  window: string;
  model?: string;
  variable?: string;
}): Promise<VerificationSummaryResponse> {
  const search = new URLSearchParams();
  search.set("window", params.window);
  if (params.model && params.model !== "all") search.set("model", params.model);
  if (params.variable && params.variable !== "all") search.set("variable", params.variable);
  return fetchJson<VerificationSummaryResponse>(`${API_ORIGIN}/api/v4/admin/verification/summary?${search.toString()}`);
}

export async function fetchAdminVerificationResults(params: {
  window: string;
  model?: string;
  variable?: string;
  manualStatus?: string;
  flaggedOnly?: boolean;
  attentionOnly?: boolean;
  limit?: number;
}): Promise<VerificationResultsResponse> {
  const search = new URLSearchParams();
  search.set("window", params.window);
  if (params.limit) search.set("limit", String(params.limit));
  if (params.model && params.model !== "all") search.set("model", params.model);
  if (params.variable && params.variable !== "all") search.set("variable", params.variable);
  if (params.manualStatus && params.manualStatus !== "all") search.set("manual_status", params.manualStatus);
  if (params.flaggedOnly) search.set("flagged_only", "true");
  if (params.attentionOnly) search.set("attention_only", "true");
  return fetchJson<VerificationResultsResponse>(`${API_ORIGIN}/api/v4/admin/verification/results?${search.toString()}`);
}

export async function updateAdminVerificationReview(
  reviewId: number,
  payload: {
    manual_status: "review" | "pass" | "fail";
    benchmark_site?: string;
    notes?: string;
  },
): Promise<VerificationResult> {
  return fetchJson<VerificationResult>(`${API_ORIGIN}/api/v4/admin/verification/results/${reviewId}/review`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

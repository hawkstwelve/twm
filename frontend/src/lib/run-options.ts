export type RunOption = {
  value: string;
  label: string;
};

const RUN_ID_RE = /^(\d{4})(\d{2})(\d{2})_(\d{2})z$/i;

export function formatRunLabel(runId: string): string {
  const match = runId.match(RUN_ID_RE);
  if (!match) {
    return runId;
  }
  const [, , month, day, hour] = match;
  return `${hour}Z ${Number(month)}/${day}`;
}

export function latestRunLabel(runId: string | null): string {
  if (!runId) {
    return "Latest";
  }
  return `Latest (${formatRunLabel(runId)})`;
}

export function buildRunOptions(runs: string[], latestRunId: string | null): RunOption[] {
  const unique = Array.from(new Set(runs.filter(Boolean)));
  const concrete = unique
    .filter((runId) => runId !== latestRunId)
    .sort((a, b) => b.localeCompare(a));

  return [
    { value: "latest", label: latestRunLabel(latestRunId) },
    ...concrete.map((runId) => ({ value: runId, label: formatRunLabel(runId) })),
  ];
}

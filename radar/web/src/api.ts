// Typed client for the Restatement Radar API (radar/api/app.py).

export type Status = "PENDING" | "CONFIRMED" | "ENRICHED" | "NEEDS_REVIEW" | "CLOSED";

export interface Headline {
  item: string;
  period_start: string | null;
  period_end: string;
  previously_reported: string | null;
  adjustment: string;
  restated: string | null;
}

export interface CaseSummary {
  id: number;
  cik: number;
  company: string;
  tickers: string[];
  status: Status;
  type: "BIG_R" | "LITTLE_R" | "UNKNOWN";
  sub_item: "a" | "b" | null;
  severity: number | null;
  opened_at: string;
  updated_at: string;
  headline: Headline[] | null;
  checks_passed: number | null;
  checks_failed: number | null;
  first_filed: string | null;
  last_filed: string | null;
  forms: string[] | null;
  periods: string[] | null;
}

export interface Filing {
  accession: string;
  form: string;
  filed_date: string;
  accepted_at: string | null;
  role: "warning" | "warning_amendment" | "amendment" | "revision";
  url: string;
  triage_score: number | null;
  delta_source: string | null;
  item402_text: string | null;
  amendment_description: string | null;
}

export interface CaseDetail extends CaseSummary {
  filings: Filing[];
  affected_periods: { period_end: string; kinds: string[]; sources: string[] }[];
  summary: Record<string, unknown>;
}

export interface DeltaRow {
  concept: string;
  label: string | null;
  canonical_item: string | null;
  name: string;
  statement: "IS" | "BS" | "CF" | "EQ" | null;
  period_start: string | null;
  period_end: string;
  unit: string | null;
  previously_reported: number | null;
  adjustment: number | null;
  restated: number | null;
  pct: number | null;
  source: "xbrl" | "html" | "companyfacts";
  check_ok: boolean | null;
  accession: string;
  filed_date: string;
}

export interface DeltaPeriod {
  start: string | null;
  end: string;
  label: string;
  rows: DeltaRow[];
}

export interface GraphNode {
  id: string;
  name: string;
  statement: "IS" | "BS" | "CF";
  delta: number | null;
  measured: boolean;
}

export interface GraphEdge {
  src_item: string;
  dst_item: string;
  sign: number;
  relationship: string;
  src_delta: number;
  dst_delta: number | null;
}

export interface Graph {
  period?: { start: string | null; end: string; label: string };
  periods: { start: string | null; end: string; label: string; edges: number }[];
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface Check {
  name: string;
  status: "pass" | "fail";
  period_start: string | null;
  period_end: string;
  gap: number;
  details: { assumed_unchanged?: string[]; likely_cause?: string; tolerance?: string };
}

export interface Stats {
  detection_latency: { n: number; p50_seconds: number | null; p95_seconds: number | null };
  cases: Partial<Record<Status, number>>;
  warnings: { last_30: number; last_90: number; last_365: number };
  warnings_by_year: { year: number; n: number }[];
  median_days_warning_to_amendment: number | null;
}

export interface RadarEvent {
  id: number;
  ts: string;
  type: string;
  case_id: number | null;
  accession: string | null;
  payload: Record<string, unknown>;
}

// API responses may be cached for a few seconds (browser or CDN). After a live event the
// app sets this to the event id, giving later requests a new cache key so they see it.
let cacheVersion: number | null = null;
export function bustCache(version: number) {
  cacheVersion = version;
}

async function get<T>(path: string): Promise<T> {
  const url = cacheVersion === null ? path : `${path}${path.includes("?") ? "&" : "?"}v=${cacheVersion}`;
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} for ${path}`);
  return res.json() as Promise<T>;
}

export interface CaseQuery {
  status?: string;
  q?: string;
  sort?: string;
  limit?: number;
  offset?: number;
}

export const api = {
  cases: (p: CaseQuery) => {
    const qs = new URLSearchParams();
    Object.entries(p).forEach(([k, v]) => {
      if (v !== undefined && v !== "") qs.set(k, String(v));
    });
    return get<{ total: number; items: CaseSummary[]; offset: number }>(`/api/cases?${qs}`);
  },
  case: (id: number) => get<CaseDetail>(`/api/cases/${id}`),
  deltas: (id: number) => get<DeltaPeriod[]>(`/api/cases/${id}/deltas`),
  graph: (id: number, start?: string | null, end?: string) => {
    const qs = new URLSearchParams();
    if (end) qs.set("end", end);
    if (start) qs.set("start", start);
    return get<Graph>(`/api/cases/${id}/graph?${qs}`);
  },
  checks: (id: number) => get<Check[]>(`/api/cases/${id}/checks`),
  stats: () => get<Stats>("/api/stats"),
};

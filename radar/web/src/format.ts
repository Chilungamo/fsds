// Number and label formatting shared by every component.

const MINUS = "−";

export function compact(v: number): string {
  const a = Math.abs(v);
  const sign = v < 0 ? MINUS : "";
  if (a >= 1e9) return `${sign}${(a / 1e9).toFixed(a >= 1e10 ? 1 : 2)}B`;
  if (a >= 1e6) return `${sign}${(a / 1e6).toFixed(a >= 1e7 ? 1 : 2)}M`;
  if (a >= 1e3) return `${sign}${(a / 1e3).toFixed(a >= 1e4 ? 0 : 1)}K`;
  return `${sign}${a.toLocaleString("en-US", { maximumFractionDigits: 2 })}`;
}

export function money(v: number | null | undefined, unit?: string | null): string {
  if (v === null || v === undefined || Number.isNaN(v)) return "—";
  const sign = v < 0 ? MINUS : "";
  if (unit === "USD/shares") return `${sign}$${Math.abs(v).toFixed(2)}`;
  if (unit === "shares") return compact(v);
  return `${sign}$${compact(Math.abs(v))}`;
}

/** Signed change with a direction glyph, so meaning never rests on colour alone. */
export function delta(v: number | null | undefined, unit?: string | null): string {
  if (v === null || v === undefined) return "—";
  if (v === 0) return "no change";
  return `${v > 0 ? "▲ +" : `▼ ${MINUS}`}${money(Math.abs(v), unit)}`;
}

export function pct(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(v)) return "";
  const p = Math.abs(v * 100);
  const body = p >= 100 ? p.toFixed(0) : p >= 10 ? p.toFixed(1) : p.toFixed(2);
  return `${v > 0 ? "+" : v < 0 ? MINUS : ""}${body}%`;
}

export function direction(v: number | null | undefined): "up" | "down" | "flat" {
  if (!v) return "flat";
  return v > 0 ? "up" : "down";
}

export function dateLabel(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso.length === 10 ? `${iso}T12:00:00Z` : iso);
  return d.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric", timeZone: "UTC" });
}

export const STATUS_LABEL: Record<string, string> = {
  PENDING: "Pending restatement",
  CONFIRMED: "Restated",
  ENRICHED: "Restated",
  NEEDS_REVIEW: "Needs review",
  CLOSED: "Closed",
};

export const ROLE_LABEL: Record<string, string> = {
  warning: "Item 4.02 warning",
  warning_amendment: "Warning amended",
  amendment: "Restated filing",
  revision: "Revision",
};

export const CHECK_LABEL: Record<string, string> = {
  balance_sheet_balances: "Balance sheet balances",
  net_income_rollup: "Net income = pre-tax − tax",
  gross_profit_rollup: "Gross profit = revenue − cost",
  cash_flow_sums: "Cash flows sum to change in cash",
  net_income_flows_to_retained_earnings: "Net income flows to retained earnings",
};

export const STATEMENT_LABEL: Record<string, string> = {
  IS: "Income statement",
  BS: "Balance sheet",
  CF: "Cash flow statement",
  EQ: "Equity",
};

export interface HeadlineLike {
  item: string;
  adjustment: string;
  previously_reported: string | null;
  period_start: string | null;
  period_end: string;
}

export function headlineItem(headline: HeadlineLike[] | null | undefined): HeadlineLike | null {
  return headline?.find((h) => h.item === "net_income") ?? headline?.[0] ?? null;
}

export const ITEM_NAME: Record<string, string> = {
  net_income: "Net income",
  revenue: "Revenue",
  total_equity: "Total equity",
  total_assets: "Total assets",
  total_liabilities: "Total liabilities",
  cfo: "Operating cash flow",
};

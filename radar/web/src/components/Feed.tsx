import type { CaseSummary } from "../api";
import { dateLabel, delta, direction, headlineItem, ITEM_NAME, STATUS_LABEL } from "../format";

export const FILTERS = [
  { key: "", label: "All" },
  { key: "PENDING", label: "Pending" },
  { key: "CONFIRMED", label: "Restated" },
  { key: "NEEDS_REVIEW", label: "Needs review" },
];

export function StatusBadge({ status }: { status: string }) {
  const icon = status === "PENDING" ? "⚠" : status === "NEEDS_REVIEW" ? "?" : "●";
  return (
    <span className={`badge ${status}`}>
      <span aria-hidden="true">{icon}</span>
      {STATUS_LABEL[status] ?? status}
    </span>
  );
}

export function Severity({ value }: { value: number | null }) {
  if (value === null) return null;
  return (
    <span className="sev" title="Severity: size of the net income and equity changes, Big R, auditor-initiated, periods affected">
      <span className="sev-bar" aria-hidden="true">
        <span className="sev-fill" style={{ width: `${Math.round(value * 100)}%`, display: "block" }} />
      </span>
      <span className="num">severity {Math.round(value * 100)}</span>
    </span>
  );
}

interface Props {
  items: CaseSummary[];
  total: number;
  selected: number | null;
  fresh: Set<number>;
  status: string;
  sort: string;
  query: string;
  loading: boolean;
  onStatus: (s: string) => void;
  onSort: (s: string) => void;
  onQuery: (q: string) => void;
  onSelect: (id: number) => void;
  onMore: () => void;
}

export function Feed(p: Props) {
  return (
    <aside className="feed" aria-label="Restatement feed">
      <div className="feed-controls">
        <input
          className="search"
          type="search"
          placeholder="Search company, ticker or CIK"
          aria-label="Search company, ticker or CIK"
          value={p.query}
          onChange={(e) => p.onQuery(e.target.value)}
        />
        <div className="segmented" role="group" aria-label="Filter by status">
          {FILTERS.map((f) => (
            <button key={f.key} aria-pressed={p.status === f.key} onClick={() => p.onStatus(f.key)}>
              {f.label}
            </button>
          ))}
        </div>
        <div className="segmented" role="group" aria-label="Sort">
          <button aria-pressed={p.sort === "recent"} onClick={() => p.onSort("recent")}>Latest activity</button>
          <button aria-pressed={p.sort === "severity"} onClick={() => p.onSort("severity")}>Most severe</button>
        </div>
      </div>
      <div className="feed-meta" aria-live="polite">
        {p.loading ? "Loading…" : `${p.total.toLocaleString()} cases`}
      </div>
      <ul className="feed-list">
        {p.items.map((c) => {
          const h = headlineItem(c.headline);
          const adj = h ? Number(h.adjustment) : null;
          return (
            <li key={c.id}>
              <button
                className={`card${p.fresh.has(c.id) ? " fresh" : ""}`}
                aria-current={p.selected === c.id}
                onClick={() => p.onSelect(c.id)}
              >
                <div className="card-top">
                  <span className="card-name">
                    {c.company}
                    {c.tickers?.[0] && <span className="ticker">{c.tickers[0]}</span>}
                  </span>
                  <span className="muted num" style={{ fontSize: 12 }}>{dateLabel(c.last_filed)}</span>
                </div>
                <div className="card-row">
                  <StatusBadge status={c.status} />
                  {c.sub_item === "b" && <span className="chip" title="Auditor-initiated (Item 4.02(b))">auditor</span>}
                  <Severity value={c.severity} />
                </div>
                {h && (
                  <div className="card-row">
                    <span className="secondary">{ITEM_NAME[h.item] ?? h.item}</span>
                    <span className={`chg ${direction(adj)}`}>{delta(adj)}</span>
                    <span className="muted">· {h.period_end}</span>
                  </div>
                )}
                {!h && c.periods && c.periods.length > 0 && (
                  <div className="card-row muted">
                    {c.periods.length} period{c.periods.length > 1 ? "s" : ""} unreliable · latest {c.periods[c.periods.length - 1]}
                  </div>
                )}
              </button>
            </li>
          );
        })}
      </ul>
      {p.items.length < p.total && (
        <button className="more" onClick={p.onMore} disabled={p.loading}>
          Show more
        </button>
      )}
    </aside>
  );
}

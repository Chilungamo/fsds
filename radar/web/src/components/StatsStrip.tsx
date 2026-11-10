import { Bar, BarChart, Cell, ResponsiveContainer, Tooltip, XAxis } from "recharts";
import type { Stats } from "../api";

function YearTooltip({ active, payload }: { active?: boolean; payload?: { payload: { year: number; n: number } }[] }) {
  if (!active || !payload?.length) return null;
  const { year, n } = payload[0].payload;
  return (
    <div className="tooltip">
      <div className="t">{year}</div>
      {n.toLocaleString()} Item 4.02 warnings
    </div>
  );
}

export function StatsStrip({ stats }: { stats: Stats | null }) {
  if (!stats) return <div className="stats" aria-busy="true" />;
  const restated = (stats.cases.CONFIRMED ?? 0) + (stats.cases.ENRICHED ?? 0);
  const peak = Math.max(...stats.warnings_by_year.map((y) => y.n));
  return (
    <section className="stats" aria-label="Summary statistics">
      <div className="tile">
        <div className="label">Item 4.02 warnings, 30 days</div>
        <div className="value">{stats.warnings.last_30}</div>
        <div className="sub">{stats.warnings.last_365} in the last year</div>
      </div>
      <div className="tile">
        <div className="label">Pending restatements</div>
        <div className="value">{(stats.cases.PENDING ?? 0).toLocaleString()}</div>
        <div className="sub">warned, no restated numbers yet</div>
      </div>
      <div className="tile">
        <div className="label">Restated (confirmed)</div>
        <div className="value">{restated.toLocaleString()}</div>
        <div className="sub">since 2019</div>
      </div>
      <div className="tile">
        <div className="label">Warning → restated filing</div>
        <div className="value">
          {stats.median_days_warning_to_amendment === null ? "—" : `${stats.median_days_warning_to_amendment} days`}
        </div>
        <div className="sub">median</div>
      </div>
      <div className="tile">
        <div className="label">Alert latency (p95)</div>
        <div className="value">
          {stats.detection_latency.p95_seconds === null ? "—" : `${Math.round(stats.detection_latency.p95_seconds)} s`}
        </div>
        <div className="sub">
          EDGAR acceptance → detected · {stats.detection_latency.n} filings, 7 days
        </div>
      </div>
      <div className="tile chart">
        <div className="label">Item 4.02 warnings per year</div>
        <div style={{ height: 52 }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={stats.warnings_by_year} margin={{ top: 4, right: 0, bottom: 0, left: 0 }} barCategoryGap={2}>
              <XAxis dataKey="year" tickLine={false} axisLine={false} tick={{ fontSize: 10, fill: "var(--text-muted)" }} interval={0} tickFormatter={(y: number) => `'${String(y).slice(2)}`} height={14} />
              <Tooltip content={<YearTooltip />} cursor={{ fill: "var(--surface-2)" }} />
              <Bar dataKey="n" radius={[3, 3, 0, 0]} isAnimationActive={false}>
                {stats.warnings_by_year.map((y) => (
                  <Cell key={y.year} fill={y.n === peak ? "var(--accent)" : "var(--neutral-bar)"} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </section>
  );
}

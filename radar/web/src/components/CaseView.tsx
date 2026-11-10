import { useCallback, useEffect, useMemo, useState } from "react";
import { api, type CaseDetail, type Check, type DeltaPeriod, type Graph } from "../api";
import { dateLabel, delta, direction, ITEM_NAME, money, pct, ROLE_LABEL } from "../format";
import { Checks } from "./Checks";
import { DeltaTable } from "./DeltaTable";
import { Severity, StatusBadge } from "./Feed";
import { ImpactGraph } from "./ImpactGraph";
import { Waterfall } from "./Waterfall";

const key = (p: { start: string | null; end: string }) => `${p.start ?? ""}|${p.end}`;

function listPeriods(ends: string[]): string {
  const labels = ends.map((e) => dateLabel(e));
  if (labels.length === 1) return labels[0];
  if (labels.length <= 3) return `${labels.slice(0, -1).join("; ")} and ${labels[labels.length - 1]}`;
  return `${labels.length} periods from ${labels[0]} to ${labels[labels.length - 1]}`;
}

function Summary({ c, periods }: { c: CaseDetail; periods: DeltaPeriod[] }) {
  const warning = c.filings.find((f) => f.role === "warning");
  const unreliable = c.affected_periods.map((p) => p.period_end);
  const s = (c.summary ?? {}) as { amendment_promised?: boolean };
  const ni = periods
    .flatMap((p) => p.rows.filter((r) => r.canonical_item === "net_income" && r.adjustment).map((r) => ({ r, p })))
    .sort((a, b) => Math.abs(b.r.adjustment ?? 0) - Math.abs(a.r.adjustment ?? 0))[0];

  if (c.status === "PENDING") {
    return (
      <p className="summary-text">
        {warning ? `On ${dateLabel(warning.filed_date)} ` : ""}
        {c.company} disclosed that its financial statements
        {unreliable.length ? ` for ${listPeriods(unreliable)}` : ""} should no longer be relied upon
        {c.sub_item === "b" ? ", after notice from its auditor" : ""}. Restated figures have not been filed yet
        {s.amendment_promised ? "; the company said it will amend the affected reports." : "."}
      </p>
    );
  }
  return (
    <p className="summary-text">
      {c.company} restated{" "}
      {unreliable.length ? listPeriods(unreliable) : "previously issued financial statements"}.{" "}
      {ni && ni.r.previously_reported !== null && (
        <>
          Net income for the {ni.p.label.toLowerCase()} {ni.r.adjustment! < 0 ? "fell" : "rose"} by{" "}
          <strong>{money(Math.abs(ni.r.adjustment!))}</strong>
          {ni.r.pct !== null && ` (${pct(ni.r.pct)})`}, from {money(ni.r.previously_reported)} to {money(ni.r.restated)}.
        </>
      )}
      {!ni && periods.length === 0 && " No line-item figures could be extracted from the restated filings."}
    </p>
  );
}

export function CaseView({ id }: { id: number }) {
  const [c, setCase] = useState<CaseDetail | null>(null);
  const [periods, setPeriods] = useState<DeltaPeriod[]>([]);
  const [checks, setChecks] = useState<Check[]>([]);
  const [graph, setGraph] = useState<Graph | null>(null);
  const [period, setPeriod] = useState<string | null>(null);
  const [picked, setPicked] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    Promise.all([api.case(id), api.deltas(id), api.checks(id)])
      .then(([cd, dl, ck]) => {
        setCase(cd);
        setPeriods(dl);
        setChecks(ck);
        setError(null);
        setPeriod((prev) => {
          if (prev && dl.some((p) => key(p) === prev)) return prev;
          // default: the period with the largest net income change, else the latest
          const withNi = dl
            .map((p) => ({ p, ni: Math.abs(p.rows.find((r) => r.canonical_item === "net_income")?.adjustment ?? 0) }))
            .filter((x) => x.ni > 0 && x.p.start)
            .sort((a, b) => b.ni - a.ni || b.p.end.localeCompare(a.p.end))[0];
          return withNi ? key(withNi.p) : dl.length ? key(dl[dl.length - 1]) : null;
        });
      })
      .catch((e: Error) => setError(e.message));
  }, [id]);

  useEffect(() => {
    setCase(null);
    setPeriod(null);
    setGraph(null);
    setPicked(null);
    load();
  }, [id, load]);

  // reload when the live stream says this case changed
  useEffect(() => {
    const h = (e: Event) => {
      if ((e as CustomEvent<number>).detail === id) load();
    };
    window.addEventListener("radar:case-changed", h);
    return () => window.removeEventListener("radar:case-changed", h);
  }, [id, load]);

  const current = useMemo(() => periods.find((p) => key(p) === period) ?? null, [periods, period]);

  useEffect(() => {
    if (!current) return;
    api.graph(id, current.start, current.end).then(setGraph).catch(() => setGraph(null));
  }, [id, current]);

  if (error) return <div className="empty">Could not load case {id}: {error}</div>;
  if (!c) return <div className="empty" aria-busy="true">Loading case…</div>;

  const periodChecks = current
    ? checks.filter((k) => k.period_end === current.end && (k.period_start === current.start || k.period_start === null))
    : [];
  const passed = checks.filter((k) => k.status === "pass").length;

  return (
    <div className="detail-inner">
      <section className="panel" aria-labelledby="case-title">
        <div className="case-head">
          <div>
            <h1 id="case-title">
              {c.company}
              {c.tickers?.length ? <span className="ticker" style={{ fontSize: 14 }}>{c.tickers.join(" · ")}</span> : null}
            </h1>
            <div className="meta">
              <StatusBadge status={c.status} />
              {c.type === "BIG_R" && <span className="chip" title="Reissuance restatement: prior statements could no longer be relied upon">Big R</span>}
              {c.sub_item === "b" && <span className="chip">auditor-initiated</span>}
              <Severity value={c.severity} />
              <span>CIK {c.cik}</span>
            </div>
          </div>
          <a className="icon-btn" href={`https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${c.cik}`} target="_blank" rel="noreferrer">
            Company on EDGAR ↗
          </a>
        </div>
        <Summary c={c} periods={periods} />
        {c.headline && c.headline.length > 0 && (
          <div className="kpis">
            {c.headline.map((h) => {
              const adj = Number(h.adjustment);
              const prev = h.previously_reported === null ? null : Number(h.previously_reported);
              return (
                <div className="kpi" key={h.item}>
                  <div className="label">{ITEM_NAME[h.item] ?? h.item} · {h.period_end}</div>
                  <div className={`value chg ${direction(adj)}`}>{delta(adj)}</div>
                  <div className="from">
                    {money(prev)} → {money(h.restated === null ? null : Number(h.restated))}
                    {prev ? ` (${pct(adj / Math.abs(prev))})` : ""}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </section>

      <section className="panel" aria-labelledby="timeline-title">
        <h2 id="timeline-title">Filings</h2>
        <p className="hint">The warning, then the filings that carry restated numbers. Links open the original document on SEC EDGAR.</p>
        <ol className="timeline">
          {c.filings.map((f) => (
            <li key={f.accession}>
              <span className="when">{dateLabel(f.filed_date)}</span>
              <span className={`node ${f.role.startsWith("warning") ? "warning" : "amendment"}`} aria-hidden="true" />
              <span className="what">
                <strong>{f.form}</strong> · {ROLE_LABEL[f.role] ?? f.role} ·{" "}
                <a href={f.url} target="_blank" rel="noreferrer">{f.accession}</a>
                {f.delta_source && <span className="muted"> · figures from {f.delta_source === "companyfacts" ? "original vs amended values" : f.delta_source === "html" ? "printed table" : "XBRL"}</span>}
              </span>
            </li>
          ))}
        </ol>
        {c.filings.some((f) => f.item402_text || f.amendment_description) && (
          <details style={{ marginTop: 10 }}>
            <summary>What the company said</summary>
            {c.filings.filter((f) => f.item402_text || f.amendment_description).map((f) => (
              <blockquote key={f.accession} className="source-text">
                <strong>{f.form}, {dateLabel(f.filed_date)}:</strong> {f.item402_text ?? f.amendment_description}
              </blockquote>
            ))}
          </details>
        )}
      </section>

      {periods.length > 0 && (
        <>
          <section className="panel" aria-labelledby="period-title">
            <h2 id="period-title">Restated figures</h2>
            <p className="hint">Pick a period. Values come from the latest filing that restated it.</p>
            <div className="period-tabs" role="group" aria-label="Reporting period">
              {periods.map((p) => (
                <button key={key(p)} aria-pressed={key(p) === period} onClick={() => { setPeriod(key(p)); setPicked(null); }}>
                  {p.label}
                </button>
              ))}
            </div>
            {current && (
              <div className="grid-2">
                <div>
                  <h2 style={{ fontSize: 13 }}>Net income bridge</h2>
                  {current.start ? <Waterfall rows={current.rows} /> : <p className="muted">Balance-sheet date: see the table and the graph for how the change flows.</p>}
                </div>
                <div>
                  <h2 style={{ fontSize: 13 }}>
                    Accounting checks <span className="muted" style={{ fontWeight: 400 }}>· {passed}/{checks.length} pass across all periods</span>
                  </h2>
                  <Checks checks={periodChecks} />
                </div>
              </div>
            )}
          </section>

          {current && (
            <section className="panel" aria-labelledby="table-title">
              <h2 id="table-title">{current.label}: line items</h2>
              <DeltaTable rows={current.rows} highlight={picked} />
            </section>
          )}

          <section className="panel" aria-labelledby="graph-title">
            <h2 id="graph-title">How the adjustments flow through the statements</h2>
            <p className="hint">{current?.label}{graph?.period && current && graph.period.end !== current.end ? "" : ""}</p>
            {graph && graph.nodes.length > 0 ? (
              <ImpactGraph graph={graph} picked={picked} onPick={setPicked} />
            ) : (
              <p className="muted">No connected line items in this period.</p>
            )}
          </section>
        </>
      )}
      <p className="disclaimer">
        Data from SEC EDGAR, extracted automatically. Not investment advice; check figures against the source filings.
      </p>
    </div>
  );
}

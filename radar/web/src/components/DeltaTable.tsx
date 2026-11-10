import type { DeltaRow } from "../api";
import { delta, direction, money, pct, STATEMENT_LABEL } from "../format";

const SOURCE_LABEL: Record<string, string> = {
  xbrl: "XBRL",
  html: "table",
  companyfacts: "vs original",
};

export function DeltaTable({ rows, highlight }: { rows: DeltaRow[]; highlight: string | null }) {
  const groups = new Map<string, DeltaRow[]>();
  rows.forEach((r) => {
    const key = r.statement ?? "?";
    groups.set(key, [...(groups.get(key) ?? []), r]);
  });
  return (
    <div style={{ overflowX: "auto" }}>
      <table className="deltas">
        <caption className="sr-only">Restated line items for the selected period</caption>
        <thead>
          <tr>
            <th scope="col">Line item</th>
            <th scope="col">As reported</th>
            <th scope="col">Adjustment</th>
            <th scope="col">As restated</th>
            <th scope="col">Change</th>
          </tr>
        </thead>
        <tbody>
          {[...groups.entries()].map(([stmt, items]) => (
            <FragmentRows key={stmt} title={STATEMENT_LABEL[stmt] ?? "Other"} rows={items} highlight={highlight} />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function FragmentRows({ title, rows, highlight }: { title: string; rows: DeltaRow[]; highlight: string | null }) {
  return (
    <>
      <tr className="group">
        <td colSpan={5}>{title}</td>
      </tr>
      {rows.map((r) => (
        <tr key={`${r.concept}-${r.accession}`} data-highlight={!!highlight && r.canonical_item === highlight}>
          <td>
            {r.name}
            <span className="src" title={`From ${r.accession}`}>{SOURCE_LABEL[r.source] ?? r.source}</span>
            {r.check_ok === false && (
              <span className="warn" title="As reported + adjustment does not equal as restated in the filing">⚠</span>
            )}
          </td>
          <td>{money(r.previously_reported, r.unit)}</td>
          <td>
            <span className={`chg ${direction(r.adjustment)}`}>{delta(r.adjustment, r.unit)}</span>
          </td>
          <td>{money(r.restated, r.unit)}</td>
          <td className="secondary">{pct(r.pct)}</td>
        </tr>
      ))}
    </>
  );
}

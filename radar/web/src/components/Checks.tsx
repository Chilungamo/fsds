import type { Check } from "../api";
import { CHECK_LABEL, money } from "../format";

export function Checks({ checks }: { checks: Check[] }) {
  if (!checks.length) {
    return <p className="muted">Not enough restated lines in this period to test the accounting identities.</p>;
  }
  return (
    <ul className="checks">
      {checks.map((c, i) => (
        <li key={i} className={c.status}>
          <span className="icon" aria-hidden="true">{c.status === "pass" ? "✓" : "✕"}</span>
          <div>
            <div>
              <span className="sr-only">{c.status === "pass" ? "Passed: " : "Failed: "}</span>
              {CHECK_LABEL[c.name] ?? c.name}
            </div>
            {c.details.likely_cause && <div className="note">Likely cause: {c.details.likely_cause}</div>}
            {!!c.details.assumed_unchanged?.length && (
              <div className="note">Assumed unchanged: {c.details.assumed_unchanged.join(", ").replace(/_/g, " ")}</div>
            )}
          </div>
          <span className="num secondary" style={{ fontSize: 12 }}>
            {c.status === "pass" ? "balances" : `off by ${money(Math.abs(c.gap))}`}
          </span>
        </li>
      ))}
    </ul>
  );
}

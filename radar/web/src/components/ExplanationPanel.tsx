import type { ReactNode } from "react";
import type { Explanation, ExplanationFact } from "../api";
import { ITEM_NAME } from "../format";

const FIELD_LABEL: Record<ExplanationFact["field"], string> = {
  previously_reported: "as originally reported",
  adjustment: "change",
  restated: "as restated",
  pct_change: "change vs. originally reported",
};

function factTitle(f: ExplanationFact): string {
  const item = f.item ? ITEM_NAME[f.item] ?? f.item : "";
  const when = f.period_start ? `${f.period_start} to ${f.period_end}` : `as of ${f.period_end}`;
  const value = f.field === "pct_change" ? `${Number(f.value).toFixed(2)}%` : Number(f.value).toLocaleString("en-US");
  return `${item}, ${FIELD_LABEL[f.field]}, ${when}: ${value} (from the filing's data)`;
}

/** The sentence with every validated figure marked; hovering one shows the stored value. */
function withFacts(text: string, facts: ExplanationFact[]): ReactNode[] {
  const shown = [...new Set(facts.map((f) => f.shown))].sort((a, b) => b.length - a.length);
  if (!shown.length) return [text];
  const escaped = shown.map((s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const parts = text.split(new RegExp(`(${escaped.join("|")})`, "g"));
  return parts.map((part, i) => {
    const fact = facts.find((f) => f.shown === part);
    return fact ? (
      <mark key={i} className="fact" title={factTitle(fact)} tabIndex={0}>
        {part}
      </mark>
    ) : (
      part
    );
  });
}

export function ExplanationPanel({ e }: { e: Explanation }) {
  const checked = e.validation.figures_checked + e.validation.dates_checked;
  return (
    <div className="explanation">
      <p className="explanation-headline">{withFacts(e.headline, e.facts)}</p>
      <p className="summary-text">
        {e.sentences.map((s, i) => (
          <span key={i}>
            {withFacts(s, e.facts)}{" "}
          </span>
        ))}
      </p>
      {e.causes.length > 0 && (
        <details className="causes">
          <summary>
            Likely cause{e.causes.length > 1 ? "s" : ""}:{" "}
            {e.causes.map((c) => (
              <span key={c.code} className="chip cause" title={`confidence ${Math.round(c.confidence * 100)}%`}>
                {c.label}
              </span>
            ))}
          </summary>
          {e.causes.map((c) => (
            <div key={c.code} className="cause-evidence">
              <strong>{c.label}</strong>
              {c.from_items.length > 0 && (
                <span className="muted"> · restated lines: {c.from_items.map((i) => ITEM_NAME[i] ?? i).join(", ")}</span>
              )}
              {c.evidence.map((q, i) => (
                <blockquote key={i} className="source-text">“{q}”</blockquote>
              ))}
            </div>
          ))}
        </details>
      )}
      <p className="provenance">
        {e.method === "llm" ? "Written with Claude from the filings" : "Generated from the filings"}
        {checked > 0 && ` · ${checked} figures and dates checked against the source data`}
        {e.facts.length > 0 && " · hover a highlighted figure to see where it comes from"}
      </p>
    </div>
  );
}

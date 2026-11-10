import { Bar, BarChart, CartesianGrid, Cell, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { DeltaRow } from "../api";
import { compact, delta, money } from "../format";

// How each income-statement line moves net income (XBRL natural signs: expenses positive).
const EFFECT: Record<string, number> = {
  revenue: 1,
  other_income: 1,
  warrant_fv_change: 1,
  cogs: -1,
  sga: -1,
  rd: -1,
  impairment: -1,
  interest_expense: -1,
  tax_expense: -1,
};

interface Step {
  name: string;
  range: [number, number];
  value: number;
  kind: "start" | "end" | "up" | "down";
}

export function buildSteps(rows: DeltaRow[]): Step[] | null {
  const byItem = new Map(rows.filter((r) => r.canonical_item).map((r) => [r.canonical_item!, r]));
  const ni = byItem.get("net_income");
  if (!ni || ni.previously_reported === null || ni.restated === null || !ni.adjustment) return null;
  const drivers: { name: string; effect: number }[] = [];
  for (const [item, sign] of Object.entries(EFFECT)) {
    const r = byItem.get(item);
    if (r?.adjustment) drivers.push({ name: r.name, effect: sign * r.adjustment });
  }
  // total operating expenses only when its components are not shown separately
  const opex = byItem.get("opex");
  if (opex?.adjustment && !["sga", "rd", "impairment"].some((i) => byItem.get(i)?.adjustment)) {
    drivers.push({ name: opex.name, effect: -opex.adjustment });
  }
  const explained = drivers.reduce((s, d) => s + d.effect, 0);
  const residual = ni.adjustment - explained;
  if (Math.abs(residual) > Math.max(1, Math.abs(ni.adjustment) * 0.005)) {
    drivers.push({ name: "Other / not itemised", effect: residual });
  }
  drivers.sort((a, b) => Math.abs(b.effect) - Math.abs(a.effect));

  const steps: Step[] = [{ name: "As reported", range: [0, ni.previously_reported], value: ni.previously_reported, kind: "start" }];
  let level = ni.previously_reported;
  for (const d of drivers.slice(0, 7)) {
    steps.push({ name: d.name, range: [level, level + d.effect], value: d.effect, kind: d.effect >= 0 ? "up" : "down" });
    level += d.effect;
  }
  if (drivers.length > 7) {
    const rest = drivers.slice(7).reduce((s, d) => s + d.effect, 0);
    steps.push({ name: `${drivers.length - 7} more`, range: [level, level + rest], value: rest, kind: rest >= 0 ? "up" : "down" });
  }
  steps.push({ name: "As restated", range: [0, ni.restated], value: ni.restated, kind: "end" });
  return steps;
}

const FILL: Record<Step["kind"], string> = {
  start: "var(--neutral-bar)",
  end: "var(--neutral-bar)",
  up: "var(--up)",
  down: "var(--down)",
};

function StepTooltip({ active, payload }: { active?: boolean; payload?: { payload: Step }[] }) {
  if (!active || !payload?.length) return null;
  const s = payload[0].payload;
  return (
    <div className="tooltip">
      <div className="t">{s.name}</div>
      {s.kind === "start" || s.kind === "end" ? `Net income ${money(s.value)}` : `${delta(s.value)} to net income`}
    </div>
  );
}

export function Waterfall({ rows }: { rows: DeltaRow[] }) {
  const steps = buildSteps(rows);
  if (!steps) {
    return <p className="muted">No net income change in this period, so there is no bridge to draw.</p>;
  }
  return (
    <figure style={{ margin: 0 }}>
      <div style={{ height: 280 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={steps} margin={{ top: 8, right: 8, bottom: 8, left: 8 }} barCategoryGap="22%">
            <CartesianGrid vertical={false} stroke="var(--border)" strokeDasharray="2 4" />
            <XAxis dataKey="name" tick={{ fontSize: 11, fill: "var(--text-secondary)" }} interval={0} tickLine={false} axisLine={{ stroke: "var(--border)" }} height={44} />
            <YAxis tickFormatter={(v: number) => compact(v)} tick={{ fontSize: 11, fill: "var(--text-muted)" }} tickLine={false} axisLine={false} width={56} />
            <ReferenceLine y={0} stroke="var(--text-muted)" />
            <Tooltip content={<StepTooltip />} cursor={{ fill: "var(--surface-2)" }} />
            <Bar dataKey="range" radius={4} isAnimationActive={false}>
              {steps.map((s, i) => (
                <Cell key={i} fill={FILL[s.kind]} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      <figcaption className="muted" style={{ fontSize: 12 }}>
        Net income as reported → each restated line's effect (▲ blue raises, ▼ red lowers) → as restated. Exact figures are in the table.
      </figcaption>
    </figure>
  );
}

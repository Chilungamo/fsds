"""Three-statement propagation engine (design doc §8).

Input: a case's line-item deltas (from XBRL or HTML restatement tables).
Output, per reporting period:

* integrity checks — does the restatement still obey double-entry arithmetic?
* an impact graph — which line items each adjustment flows into, and by how much;
* a severity score for sorting the dashboard (§5.5).

All functions are pure; the database layer lives in radar.enrich.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from radar.normalize.normalize import catalog

ZERO = Decimal(0)

# (source, target, sign, relationship). Arithmetic roll-ups first, then economic links
# that show where an error's other leg usually lands.
EDGES: list[tuple[str, str, int, str]] = [
    # income statement roll-up
    ("revenue", "gross_profit", +1, "roll-up"),
    ("cogs", "gross_profit", -1, "roll-up"),
    ("gross_profit", "operating_income", +1, "roll-up"),
    ("sga", "opex", +1, "roll-up"),
    ("impairment", "opex", +1, "roll-up"),
    ("opex", "operating_income", -1, "roll-up"),
    ("operating_income", "pretax_income", +1, "roll-up"),
    ("warrant_fv_change", "pretax_income", +1, "other income"),
    ("pretax_income", "net_income", +1, "roll-up"),
    ("tax_expense", "net_income", -1, "roll-up"),
    ("net_income", "eps_basic", +1, "per share"),
    # income statement -> balance sheet
    ("net_income", "retained_earnings", +1, "closing entry"),
    ("impairment", "goodwill", -1, "write-down"),
    ("cogs", "inventory", -1, "cost relieved from inventory"),
    ("revenue", "accounts_receivable", +1, "revenue billed"),
    ("revenue", "deferred_revenue", -1, "revenue recognised from deferred"),
    ("tax_expense", "deferred_tax_assets", -1, "deferred tax"),
    ("warrant_fv_change", "warrant_liabilities", -1, "fair value remeasurement"),
    # balance sheet roll-up
    ("cash", "total_current_assets", +1, "roll-up"),
    ("accounts_receivable", "total_current_assets", +1, "roll-up"),
    ("inventory", "total_current_assets", +1, "roll-up"),
    ("total_current_assets", "total_assets", +1, "roll-up"),
    ("ppe", "total_assets", +1, "roll-up"),
    ("goodwill", "total_assets", +1, "roll-up"),
    ("intangibles", "total_assets", +1, "roll-up"),
    ("deferred_tax_assets", "total_assets", +1, "roll-up"),
    ("deferred_revenue", "total_current_liabilities", +1, "roll-up"),
    ("total_current_liabilities", "total_liabilities", +1, "roll-up"),
    ("warrant_liabilities", "total_liabilities", +1, "roll-up"),
    ("retained_earnings", "total_equity", +1, "roll-up"),
    ("apic", "total_equity", +1, "roll-up"),
    ("total_liabilities", "liabilities_and_equity", +1, "roll-up"),
    ("temporary_equity", "liabilities_and_equity", +1, "roll-up"),
    ("total_equity", "liabilities_and_equity", +1, "roll-up"),
    # added with the extended catalog
    ("rd", "opex", +1, "roll-up"),
    ("other_income", "pretax_income", +1, "roll-up"),
    ("interest_expense", "pretax_income", -1, "roll-up"),
    ("net_income", "comprehensive_income", +1, "roll-up"),
    ("prepaid_assets", "total_current_assets", +1, "roll-up"),
    ("operating_lease_rou", "total_assets", +1, "roll-up"),
    ("other_assets_noncurrent", "total_assets", +1, "roll-up"),
    ("accounts_payable", "total_current_liabilities", +1, "roll-up"),
    ("accrued_liabilities", "total_current_liabilities", +1, "roll-up"),
    ("deferred_tax_liabilities", "total_liabilities", +1, "roll-up"),
    ("other_liabilities_noncurrent", "total_liabilities", +1, "roll-up"),
    ("common_stock", "total_equity", +1, "roll-up"),
    ("aoci", "total_equity", +1, "roll-up"),
    ("tax_expense", "deferred_tax_liabilities", +1, "deferred tax"),
    ("sbc", "cfo", +1, "non-cash add-back"),
    ("d_and_a", "cfo", +1, "non-cash add-back"),
    ("deferred_tax_expense", "cfo", +1, "non-cash add-back"),
    ("chg_prepaid", "cfo", -1, "working capital"),
    ("chg_accounts_payable", "cfo", +1, "working capital"),
    ("chg_accrued", "cfo", +1, "working capital"),
    # income statement -> cash flow statement
    ("net_income", "cfo", +1, "indirect method start"),
    ("chg_accounts_receivable", "cfo", -1, "working capital"),
    ("chg_inventory", "cfo", -1, "working capital"),
    ("chg_deferred_revenue", "cfo", +1, "working capital"),
    ("capex", "cfi", -1, "investing outflow"),
    ("capex", "ppe", +1, "capitalised cost"),
    ("cfo", "net_change_cash", +1, "roll-up"),
    ("cfi", "net_change_cash", +1, "roll-up"),
    ("cff", "net_change_cash", +1, "roll-up"),
    ("net_change_cash", "cash", +1, "cash reconciliation"),
]


# Restating these usually moves retained earnings directly, bypassing net income
# (SPAC restatements remeasure redeemable shares against accumulated deficit).
DIRECT_EQUITY_ITEMS = ("temporary_equity", "apic")


@dataclass(frozen=True, order=True)
class Period:
    start: str | None  # None = instant (balance sheet date)
    end: str

    @property
    def is_instant(self) -> bool:
        return self.start is None

    def opening(self) -> Period:
        """The balance-sheet date just before a duration starts."""
        assert self.start
        return Period(None, (date.fromisoformat(self.start) - timedelta(days=1)).isoformat())


@dataclass
class Cell:
    item: str
    previously_reported: Decimal | None
    adjustment: Decimal
    restated: Decimal | None
    tolerance: Decimal
    concept: str
    source: str


@dataclass
class Check:
    name: str
    period: Period
    status: str  # pass | fail
    gap: Decimal
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class Edge:
    period: Period
    src: str
    dst: str
    sign: int
    relationship: str
    src_delta: Decimal
    dst_delta: Decimal | None


View = dict[Period, dict[str, Cell]]


def _d(v: Any) -> Decimal | None:
    return None if v is None else v if isinstance(v, Decimal) else Decimal(str(v))


def build_view(rows: list[dict[str, Any]]) -> View:
    """One cell per (period, canonical item) from undimensioned rows. XBRL beats HTML;
    within a source, the concept listed first in canonical.toml wins."""
    cat = catalog()
    best: dict[tuple[Period, str], tuple[tuple, Cell]] = {}
    for r in rows:
        item = r.get("canonical_item")
        adj = _d(r.get("adjustment"))
        if not item or r.get("dims") or adj is None:
            continue
        start = r["period_start"]
        start = start.isoformat() if isinstance(start, date) else start
        end = r["period_end"]
        end = end.isoformat() if isinstance(end, date) else end
        period = Period(start, end)
        local = r["concept"].rsplit(":", 1)[-1]
        concepts = cat.items[item].concepts
        rank = (r.get("source") != "xbrl", concepts.index(local) if local in concepts else 99)
        cell = Cell(
            item,
            _d(r.get("previously_reported")),
            adj,
            _d(r.get("restated")),
            _d(r.get("tolerance")) or Decimal(1),
            r["concept"],
            r.get("source", "xbrl"),
        )
        key = (period, item)
        if key not in best or rank < best[key][0]:
            best[key] = (rank, cell)
    view: View = defaultdict(dict)
    for (period, item), (_, cell) in best.items():
        view[period][item] = cell
    return dict(view)


# --- integrity checks ------------------------------------------------------------------


@dataclass
class _Term:
    item: str
    sign: int
    period: Period
    # An opening balance carries the cumulative effect of every earlier error, so it may
    # never be assumed unchanged just because this filing does not restate it.
    may_assume_unchanged: bool = True


def _evaluate(
    name: str,
    period: Period,
    lhs: list[_Term],
    rhs_options: list[list[_Term]],
    view: View,
    unchanged: set[str],
) -> Check | None:
    """lhs = rhs, where rhs may have alternatives (e.g. "liabilities and equity" or
    "liabilities + temporary equity + equity"). A missing term counts as 0 only if the
    filing reported that line item without restating it."""

    def resolve(terms: list[_Term]) -> tuple[Decimal, Decimal, list, list] | None:
        total, tol, used, assumed = ZERO, ZERO, [], []
        for t in terms:
            cell = view.get(t.period, {}).get(t.item)
            if cell is not None:
                total += t.sign * cell.adjustment
                tol += cell.tolerance
                used.append(
                    {"item": t.item, "period_end": t.period.end, "delta": str(cell.adjustment)}
                )
            elif t.item in unchanged and t.may_assume_unchanged:
                assumed.append(t.item)
            else:
                return None
        return total, tol, used, assumed

    left = resolve(lhs)
    if left is None or not left[2]:
        return None
    for option in rhs_options:
        right = resolve(option)
        if right is None or (not right[2] and not left[3]):
            continue
        if not right[2] and not left[2]:
            continue
        gap = left[0] - right[0]
        tol = max(left[1] + right[1], Decimal(1))
        if abs(gap) > tol and (left[3] or right[3]):
            # failing only under an assumption: the "unchanged" line was probably just
            # left out of the restatement table. Inconclusive, not a failure.
            return None
        return Check(
            name,
            period,
            "pass" if abs(gap) <= tol else "fail",
            gap,
            {
                "lhs": left[2],
                "rhs": right[2],
                "assumed_unchanged": left[3] + right[3],
                "tolerance": str(tol),
            },
        )
    return None


def integrity_checks(view: View, unchanged: set[str] | None = None) -> list[Check]:
    unchanged = unchanged or set()
    out: list[Check] = []

    def T(item: str, sign: int, period: Period, assume: bool = True) -> _Term:
        return _Term(item, sign, period, assume)

    for p in sorted(view, key=lambda p: (p.end, p.start or "")):
        cells = view[p]
        if p.is_instant:
            if "total_assets" in cells or "liabilities_and_equity" in cells:
                c = _evaluate(
                    "balance_sheet_balances",
                    p,
                    [T("total_assets", 1, p)],
                    [
                        [T("liabilities_and_equity", 1, p)],
                        [
                            T("total_liabilities", 1, p),
                            T("temporary_equity", 1, p),
                            T("total_equity", 1, p),
                        ],
                        [T("total_liabilities", 1, p), T("total_equity", 1, p)],
                    ],
                    view,
                    unchanged,
                )
                out += [c] if c else []
            continue
        checks = [
            (
                "gross_profit_rollup",
                [T("gross_profit", 1, p)],
                [[T("revenue", 1, p), T("cogs", -1, p)]],
            ),
            (
                "net_income_rollup",
                [T("net_income", 1, p)],
                [[T("pretax_income", 1, p), T("tax_expense", -1, p)]],
            ),
            (
                "cash_flow_sums",
                [T("net_change_cash", 1, p)],
                [
                    [T("cfo", 1, p), T("cfi", 1, p), T("cff", 1, p), T("fx_effect", 1, p)],
                    [T("cfo", 1, p), T("cfi", 1, p), T("cff", 1, p)],
                ],
            ),
        ]
        if "retained_earnings" in view.get(Period(None, p.end), {}):
            checks.append(
                (
                    "net_income_flows_to_retained_earnings",
                    [
                        T("retained_earnings", 1, Period(None, p.end)),
                        T("retained_earnings", -1, p.opening(), assume=False),
                    ],
                    [[T("net_income", 1, p)]],
                )
            )
        for name, lhs, rhs in checks:
            if not any(t.item in cells or t.period != p for t in lhs):
                continue
            c = _evaluate(name, p, lhs, rhs, view, unchanged)
            if c and c.status == "fail" and name == "net_income_flows_to_retained_earnings":
                bs = view.get(Period(None, p.end), {})
                if any(bs.get(i) and bs[i].adjustment for i in DIRECT_EQUITY_ITEMS):
                    c.details["likely_cause"] = (
                        "direct equity adjustment: retained earnings also absorbed changes "
                        "outside net income (e.g. remeasurement of redeemable shares)"
                    )
            out += [c] if c else []
    return out


# --- impact graph --------------------------------------------------------------------------


def impact_edges(view: View) -> list[Edge]:
    """Edges whose source line item changed. Flows from an income-statement or cash-flow
    period into the balance sheet land on the period-end date."""
    cat = catalog()
    out: list[Edge] = []
    for p, cells in view.items():
        for src, dst, sign, rel in EDGES:
            cell = cells.get(src)
            if cell is None or cell.adjustment == 0:
                continue
            dst_period = Period(None, p.end) if cat.items[dst].statement == "BS" else p
            if cat.items[src].statement == "BS" and cat.items[dst].statement != "BS":
                continue
            target = view.get(dst_period, {}).get(dst)
            out.append(
                Edge(p, src, dst, sign, rel, cell.adjustment, target.adjustment if target else None)
            )
    return out


# --- severity -----------------------------------------------------------------------------


def severity(view: View, big_r: bool, auditor_initiated: bool, periods_affected: int) -> float:
    """Design doc §5.5. 0..1, higher = more serious."""

    def worst_ratio(item: str, instant: bool) -> float:
        best = 0.0
        for p, cells in view.items():
            c = cells.get(item)
            if c is None or p.is_instant != instant or c.adjustment == 0:
                continue
            base = abs(c.previously_reported or ZERO)
            ratio = 1.0 if base == 0 else float(min(abs(c.adjustment) / base, Decimal(1)))
            best = max(best, ratio)
        return best

    score = (
        0.40 * worst_ratio("net_income", instant=False)
        + 0.25 * worst_ratio("total_equity", instant=True)
        + 0.15 * big_r
        + 0.10 * auditor_initiated
        + 0.10 * min(periods_affected / 8, 1)
    )
    return round(min(score, 1.0), 3)

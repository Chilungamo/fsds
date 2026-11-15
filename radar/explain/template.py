"""Plain-English explanation of a restatement, from templates (design doc §5.4).

Every sentence is built from stored data: the Item 4.02 facts, the classified causes and the
restated line items. Each number the text shows is recorded as a ``Fact`` naming the line
item, period and field it came from, so the validator (step 3) can check the text against
the database and the dashboard can link each figure back to its source.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from radar.explain.causes import Cause
from radar.normalize.normalize import catalog
from radar.propagate.engine import EDGES, Edge, Period, View

ZERO = Decimal(0)

# Subtotals: the "where did it go" sentences name the line items underneath them.
TOTALS = {
    "gross_profit", "opex", "operating_income", "pretax_income", "net_income",
    "income_continuing_ops", "comprehensive_income", "eps_basic", "eps_diluted",
    "total_current_assets", "total_assets", "total_current_liabilities", "total_liabilities",
    "total_equity", "liabilities_and_equity", "retained_earnings", "cfo", "cfi", "cff",
    "net_change_cash",
}  # fmt: skip
PER_SHARE = {"eps_basic", "eps_diluted"}
LEAD_ITEMS = ("net_income", "revenue", "operating_income", "pretax_income")


@dataclass
class CaseFacts:
    """What the Item 4.02 notice and the case record say."""

    company: str
    warning_date: str | None = None  # ISO date of the 8-K
    sub_item: str | None = None  # 'a' company concluded, 'b' auditor notified
    case_type: str = "UNKNOWN"  # BIG_R | LITTLE_R | UNKNOWN
    period_ends: list[str] = field(default_factory=list)  # ISO dates named in the notice
    amendment_form: str | None = None  # first restating amendment, for cases with no 8-K
    amendment_date: str | None = None


@dataclass
class Fact:
    """One number shown in the text, and where it came from."""

    shown: str  # exactly as it appears in the text, e.g. "$43.2 million" or "71.6%"
    value: Decimal  # the stored (or derived) number it stands for
    item: str | None  # canonical line item; None for a derived ratio's denominator-free text
    period_start: str | None
    period_end: str | None
    field: str  # previously_reported | adjustment | restated | pct_change


@dataclass
class Explanation:
    headline: str
    sentences: list[str]
    facts: list[Fact]
    causes: list[Cause]
    method: str = "template"
    known_dates: set[str] = field(default_factory=set)  # ISO dates the text may mention
    names: set[str] = field(default_factory=set)  # names whose digits are not figures

    @property
    def text(self) -> str:
        return " ".join(self.sentences)


# --- formatting ----------------------------------------------------------------------------


def long_date(iso: str) -> str:
    d = date.fromisoformat(iso)
    return f"{d:%B} {d.day}, {d.year}"


def money(v: Decimal, per_share: bool = False) -> str:
    """Magnitude only (sentences carry the direction): $43.2 million, $1.82 billion."""
    a = abs(v)
    if per_share:
        return f"${a:.2f}"
    if a >= 1_000_000_000:
        return f"${a / 1_000_000_000:.2f} billion"
    if a >= 1_000_000:
        return f"${a / 1_000_000:.1f} million"
    return f"${a:,.0f}"


def span_label(p: Period) -> str:
    if p.is_instant:
        return long_date(p.end)
    days = (date.fromisoformat(p.end) - date.fromisoformat(p.start)).days + 1
    months = round(days / 30.44)
    span = {3: "quarter", 6: "six months", 9: "nine months", 12: "year"}.get(months)
    return f"the {span or f'{days} days'} ended {long_date(p.end)}"


def _join(parts: list[str]) -> str:
    return parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + " and " + parts[-1]


class _Writer:
    """Formats numbers and remembers each one as a Fact."""

    def __init__(self) -> None:
        self.facts: list[Fact] = []

    def num(self, item: str, p: Period, field_: str, v: Decimal) -> str:
        shown = money(v, item in PER_SHARE)
        self.facts.append(Fact(shown, v, item, p.start, p.end, field_))
        return shown

    def pct(self, item: str, p: Period, adj: Decimal, base: Decimal | None) -> str:
        if not base:
            return ""
        ratio = abs(adj) / abs(base) * 100
        shown = f"{ratio:.1f}%"
        self.facts.append(Fact(shown, ratio, item, p.start, p.end, "pct_change"))
        return f" ({shown})"


def _restated(cell) -> Decimal | None:
    if cell.restated is not None:
        return cell.restated
    if cell.previously_reported is not None:
        return cell.previously_reported + cell.adjustment
    return None


# --- choosing what to talk about -------------------------------------------------------------


def main_period(view: View) -> Period | None:
    """The duration period to lead with: latest end, then longest, among those where a lead
    item changed."""
    candidates = [
        p
        for p, cells in view.items()
        if not p.is_instant and any(cells.get(i) and cells[i].adjustment for i in LEAD_ITEMS)
    ]
    if not candidates:
        candidates = [p for p, cells in view.items() if not p.is_instant]
    return max(
        candidates, key=lambda p: (p.end, -date.fromisoformat(p.start).toordinal()), default=None
    )


def balance_date(view: View, near: Period | None) -> Period | None:
    instants = [
        p for p, cells in view.items() if p.is_instant and any(c.adjustment for c in cells.values())
    ]
    if near and Period(None, near.end) in instants:
        return Period(None, near.end)
    return max(instants, default=None)


def flow_path(view: View, edges: list[Edge], period: Period | None) -> list[str]:
    """Shortest route the error takes from where it starts to equity (or the furthest
    changed line it reaches), through the impact graph of one period and its closing date."""
    if period is None:
        return []
    instant = Period(None, period.end)
    changed = {i for p in (period, instant) for i, c in view.get(p, {}).items() if c.adjustment}
    graph: dict[str, list[str]] = {}
    for e in edges:
        if e.period in (period, instant) and e.src in changed:
            graph.setdefault(e.src, []).append(e.dst)
    reported = {i for p in (period, instant) for i in view.get(p, {})}

    def search(root: str) -> list[str]:
        prev: dict[str, str | None] = {root: None}
        queue = deque([root])
        while queue:
            node = queue.popleft()
            for nxt in graph.get(node, []):
                if nxt in prev:
                    continue
                prev[nxt] = node
                # walk through changed lines and through subtotals the filing does not
                # report (implied); a line reported as unchanged stops the walk
                if nxt in changed or nxt not in reported:
                    queue.append(nxt)
        hits = [n for n in prev if n in changed]
        if len(hits) < 2:
            return [root]
        goal = next(
            (g for g in ("total_equity", "retained_earnings", "net_income") if g in hits), None
        )
        if goal is None:
            goal = max(hits, key=lambda n: len(_walk(prev, n)))
        return _walk(prev, goal)

    # where the error starts: changed lines no other changed line feeds
    roots = [
        i for i in changed - TOTALS if i in graph and not any(i in ds for ds in graph.values())
    ]
    if not roots:
        return []
    for src, dst, _sign, _rel in EDGES:  # unreported subtotals continue the walk
        if src not in reported:
            graph.setdefault(src, []).append(dst)

    def size(i: str) -> Decimal:
        c = view.get(period, {}).get(i) or view.get(instant, {}).get(i)
        return abs(c.adjustment) if c else ZERO

    rank = {"total_equity": 3, "retained_earnings": 2, "net_income": 1}
    paths = [search(r) for r in roots]
    return max(paths, key=lambda path: (rank.get(path[-1], 0), size(path[0]), len(path)))


def _walk(prev: dict[str, str | None], node: str) -> list[str]:
    path: list[str] = []
    cur: str | None = node
    while cur is not None:
        path.append(cur)
        cur = prev[cur]
    return path[::-1]


# --- sentences -----------------------------------------------------------------------------


FORM_NAMES = {"10-K/A": "annual report", "10-Q/A": "quarterly report"}


def _notice(case: CaseFacts) -> str:
    who = case.company
    if case.warning_date is None and case.amendment_form:
        kind = FORM_NAMES.get(case.amendment_form, "report")
        when = f" on {long_date(case.amendment_date)}" if case.amendment_date else ""
        return (
            f"{who} filed an amended {kind} ({case.amendment_form}){when} that changes figures "
            "it had previously reported."
        )
    when = f" on {long_date(case.warning_date)}" if case.warning_date else ""
    ends = sorted(set(case.period_ends))
    if not ends:
        what = "previously issued financial statements"
    elif len(ends) <= 3:
        noun = "period" if len(ends) == 1 else "periods"
        what = f"its financial statements for the {noun} ended " + _join(
            [long_date(e) for e in ends]
        )
    else:
        what = (
            f"its financial statements for {len(ends)} periods from "
            f"{long_date(ends[0])} to {long_date(ends[-1])}"
        )
    why = ", after being notified by its auditor," if case.sub_item == "b" else ""
    return f"{who} said{when}{why} that {what} should no longer be relied upon."


def _cause_sentence(causes: list[Cause]) -> str:
    if not causes:
        return "The filing does not say clearly what caused the error."
    first = causes[0].label.lower()
    if len(causes) == 1:
        return f"The company attributes the error to {first}."
    rest = _join([c.label.lower() for c in causes[1:]])
    return f"The company attributes the error mainly to {first}, and also cites {rest}."


def _plan(case: CaseFacts) -> str | None:
    return {
        "BIG_R": "It is restating and reissuing those statements (a “Big R” restatement).",
        "LITTLE_R": (
            "It is correcting the figures in later filings instead of reissuing the old ones "
            "(a “little r” revision)."
        ),
    }.get(case.case_type)


def _lead_sentence(w: _Writer, view: View, p: Period) -> tuple[str, str, Decimal] | None:
    """(sentence, item, adjustment) for the most important income-statement change."""
    cells = view[p]
    item = next((i for i in LEAD_ITEMS if cells.get(i) and cells[i].adjustment), None)
    if item is None:
        return None
    c = cells[item]
    name = catalog().items[item].name
    before, after = c.previously_reported, _restated(c)
    when = span_label(p)
    if before is None or after is None:
        direction = "lowered" if c.adjustment < 0 else "raised"
        return (
            f"{name} for {when} is {direction} by {w.num(item, p, 'adjustment', c.adjustment)}.",
            item,
            c.adjustment,
        )
    adj = w.num(item, p, "adjustment", c.adjustment)
    pct = w.pct(item, p, c.adjustment, before)
    if item == "net_income" and before < 0 and after < 0:
        verb = "widens" if after < before else "narrows"
        text = (
            f"The net loss for {when} {verb} from {w.num(item, p, 'previously_reported', before)} "
            f"to {w.num(item, p, 'restated', after)}, a change of {adj}{pct}."
        )
    elif item == "net_income" and (before < 0) != (after < 0):
        was = "net income" if before >= 0 else "a net loss"
        now = "a net loss" if after < 0 else "net income"
        text = (
            f"For {when}, the company now reports {now} of {w.num(item, p, 'restated', after)} "
            f"instead of {was} of {w.num(item, p, 'previously_reported', before)}."
        )
    else:
        verb = "falls" if c.adjustment < 0 else "rises"
        text = (
            f"{name} for {when} {verb} by {adj}{pct}, from "
            f"{w.num(item, p, 'previously_reported', before)} "
            f"to {w.num(item, p, 'restated', after)}."
        )
    return text, item, c.adjustment


def _revenue_sentence(w: _Writer, view: View, p: Period, lead: str) -> str | None:
    c = view[p].get("revenue")
    if lead == "revenue" or not c or not c.adjustment:
        return None
    verb = "lower" if c.adjustment < 0 else "higher"
    return (
        f"Revenue for the same period is {w.num('revenue', p, 'adjustment', c.adjustment)} {verb}"
        f"{w.pct('revenue', p, c.adjustment, c.previously_reported)}."
    )


def _balance_sentence(w: _Writer, view: View, b: Period) -> str | None:
    cells = view[b]
    parts: list[str] = []
    eq = cells.get("total_equity")
    if eq and eq.adjustment:
        after = _restated(eq)
        verb = "falls" if eq.adjustment < 0 else "rises"
        tail = ""
        if after is not None:
            label = "a deficit of " if after < 0 else ""
            tail = f" to {label}{w.num('total_equity', b, 'restated', after)}"
        parts.append(
            f"total equity at {long_date(b.end)} {verb} by "
            f"{w.num('total_equity', b, 'adjustment', eq.adjustment)}{tail}"
        )
    lines = sorted(
        (c for i, c in cells.items() if i not in TOTALS and c.adjustment),
        key=lambda c: -abs(c.adjustment),
    )
    if lines:
        top = lines[0]
        name = catalog().items[top.item].name.lower()
        verb = "down" if top.adjustment < 0 else "up"
        moved = f"{name} ({verb} {w.num(top.item, b, 'adjustment', top.adjustment)})"
        parts.append(
            f"the main balance sheet change is {moved}"
            if parts
            else f"On the balance sheet at {long_date(b.end)}, the largest change is {moved}"
        )
    if not parts:
        return None
    text = "; ".join(parts) + "."
    return text[0].upper() + text[1:]


def _cash_sentence(w: _Writer, view: View, p: Period) -> str | None:
    """Errors that only move cash between sections leave cash itself unchanged."""
    cells = view[p]
    sections = {i: cells[i] for i in ("cfo", "cfi", "cff") if cells.get(i) and cells[i].adjustment}
    if not sections:
        return None
    cash_moved = any(
        view.get(q, {}).get(i) and view[q][i].adjustment
        for q in (p, Period(None, p.end))
        for i in ("net_change_cash", "cash")
    )
    names = {"cfo": "operating", "cfi": "investing", "cff": "financing"}
    if not cash_moved and len(sections) >= 2 and sum(c.adjustment for c in sections.values()) == 0:
        up = [i for i, c in sections.items() if c.adjustment > 0]
        down = [i for i, c in sections.items() if c.adjustment < 0]
        amount = w.num(down[0], p, "adjustment", sections[down[0]].adjustment)
        return (
            f"Cash itself is unchanged: {amount} moves from {names[down[0]]} to "
            f"{_join([names[i] for i in up])} cash flows."
        )
    c = sections.get("cfo")
    if c:
        verb = "lower" if c.adjustment < 0 else "higher"
        return f"Operating cash flow is {w.num('cfo', p, 'adjustment', c.adjustment)} {verb}."
    return None


def _path_sentence(path: list[str]) -> str | None:
    if len(path) < 3:
        return None
    names = [catalog().items[i].name.lower() for i in path[:7]]
    return f"The change starts in {names[0]} and carries through {' → '.join(names[1:])}."


def _headline(
    case: CaseFacts, causes: list[Cause], lead: tuple[str, str, Decimal] | None, w: _Writer
) -> str:
    if lead is None:
        return f"{case.company} warns its financial statements can't be relied on"
    _text, item, adj = lead
    name = catalog().items[item].name.lower()
    shown = next(f.shown for f in w.facts if f.item == item and f.field == "adjustment")
    verb = "cuts" if adj < 0 else "adds to"
    if item == "net_income" and adj < 0:
        verb = "lowers"
    elif item == "net_income":
        verb = "raises"
    subject = causes[0].label if causes else "Restatement"
    return f"{subject} {verb} {name} by {shown}"


def explain(case: CaseFacts, causes: list[Cause], view: View, edges: list[Edge]) -> Explanation:
    w = _Writer()
    sentences = [_notice(case), _cause_sentence(causes)]
    if plan := _plan(case):
        sentences.append(plan)
    p = main_period(view)
    lead = _lead_sentence(w, view, p) if p else None
    if not view:
        sentences.append(
            "Restated figures have not been filed yet; they will appear here when the "
            "amended report arrives."
        )
    else:
        if lead:
            sentences.append(lead[0])
            if rev := _revenue_sentence(w, view, p, lead[1]):
                sentences.append(rev)
        b = balance_date(view, p)
        if b and (bs := _balance_sentence(w, view, b)):
            sentences.append(bs)
        if p and (cash := _cash_sentence(w, view, p)):
            sentences.append(cash)
        if path := _path_sentence(flow_path(view, edges, p)):
            sentences.append(path)
    known = {d for d in [case.warning_date, case.amendment_date, *case.period_ends] if d}
    known |= {d for p in view for d in (p.start, p.end) if d}
    return Explanation(
        _headline(case, causes, lead, w),
        sentences,
        w.facts,
        causes,
        known_dates=known,
        names={case.company},
    )

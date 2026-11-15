"""Number validator for explanations (design doc §5.4, M4 exit: 100% of displayed numbers
validated).

Two independent checks, so a figure is only shown when both pass:

1. **Facts against the data.** Each ``Fact`` an explanation records must match the stored
   line item it names: the same period, the same field, the same value (a percentage is
   recomputed from the cell).
2. **Text against the facts.** Every financial number written in the text (amounts,
   per-share amounts, percentages) is read back and must equal, at the precision it is
   shown, a validated fact. This is what makes it safe to let a language model reword the
   text in step 4: it can change the words, never the numbers.

Dates, years, "Item 4.02" and small counts ("6 periods") are not financial figures and are
left alone. ``enforce`` drops any sentence that fails and reports what it dropped.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from decimal import Decimal

from radar.explain.template import Explanation, Fact, _restated
from radar.propagate.engine import Period, View

MONTHS = (
    "January|February|March|April|May|June|July|August|September|October|November|December"
    "|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
)
# Removed before numbers are read: they contain digits but are not financial figures.
_NOT_FIGURES = re.compile(
    rf"\b(?:{MONTHS})\.?\s+\d{{1,2}}(?:st|nd|rd|th)?,?\s+\d{{4}}"  # September 27, 2025
    rf"|\b\d{{1,2}}\s+(?:{MONTHS})\s+\d{{4}}"  # 27 September 2025
    r"|\b\d{4}-\d{2}-\d{2}\b"  # 2025-09-27
    r"|\bItem\s+\d\.\d{2}(?:\s*\([ab]\))?"  # Item 4.02(a)
    r"|\b(?:Q[1-4]|FY)\s?'?\d{2,4}\b"  # Q3 2025, FY2024
    r"|\b(?:19|20)\d{2}\b",  # a bare year
    re.I,
)
_SCALE = {"thousand": 3, "k": 3, "million": 6, "m": 6, "mn": 6, "billion": 9, "bn": 9, "b": 9}
_FIGURE = re.compile(
    r"(?P<dollar>\$)?\s?(?P<num>\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)"
    r"(?:\s?(?P<scale>thousand|million|billion|mn|bn|[kmb])\b)?(?P<pct>\s?%| percent\b)?",
    re.I,
)


_MONTH_NUM = {
    m: i
    for i, names in enumerate(
        [("january", "jan"), ("february", "feb"), ("march", "mar"), ("april", "apr"),
         ("may",), ("june", "jun"), ("july", "jul"), ("august", "aug"),
         ("september", "sep", "sept"), ("october", "oct"), ("november", "nov"),
         ("december", "dec")],
        start=1,
    )
    for m in names
}  # fmt: skip
_DATE = re.compile(
    rf"\b(?P<m1>{MONTHS})\.?\s+(?P<d1>\d{{1,2}})(?:st|nd|rd|th)?,?\s+(?P<y1>\d{{4}})"
    rf"|\b(?P<d2>\d{{1,2}})\s+(?P<m2>{MONTHS})\s+(?P<y2>\d{{4}})"
    r"|\b(?P<iso>\d{4}-\d{2}-\d{2})\b",
    re.I,
)


def dates(text: str) -> list[tuple[str, str]]:
    """(as written, ISO) for every full date in ``text``; unparseable ones map to ''."""
    from datetime import date

    out = []
    for m in _DATE.finditer(text):
        try:
            if m.group("iso"):
                iso = date.fromisoformat(m.group("iso")).isoformat()
            else:
                month = m.group("m1") or m.group("m2")
                day = m.group("d1") or m.group("d2")
                year = m.group("y1") or m.group("y2")
                iso = date(int(year), _MONTH_NUM[month.lower()], int(day)).isoformat()
        except ValueError:
            iso = ""
        out.append((m.group().strip(), iso))
    return out


@dataclass(frozen=True)
class Figure:
    """A number as written: its value and how precisely it was written."""

    text: str
    value: Decimal
    tolerance: Decimal  # half a unit in the last place shown
    kind: str  # money | pct | number


@dataclass
class Problem:
    kind: str  # fact_mismatch | missing_cell | unmatched_number | unmatched_date
    detail: str
    sentence: int | None = None  # index into Explanation.sentences; None for the headline


@dataclass
class Validation:
    figures_checked: int = 0
    facts_checked: int = 0
    dates_checked: int = 0
    problems: list[Problem] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems


# --- reading numbers out of text ------------------------------------------------------------


def _name_pattern(names: set[str] | frozenset[str]) -> re.Pattern | None:
    """Words of company names that contain digits ("8x8", "180", "II" is fine already):
    the company's name is not a figure, written in full or shortened."""
    words = {w.strip(".,()&") for n in names for w in n.split() if any(ch.isdigit() for ch in w)}
    words.discard("")
    if not words:
        return None
    return re.compile(r"(?<![\w$])(?:" + "|".join(map(re.escape, words)) + r")(?![\w%])", re.I)


def figures(text: str, names: set[str] | frozenset[str] = frozenset()) -> list[Figure]:
    """Financial figures in ``text``. Bare whole numbers under 1,000 without a currency sign,
    scale or percent are counts ("6 periods", "two-class") and are skipped, as are dates and
    the company's own name."""
    masked = _NOT_FIGURES.sub(lambda m: " " * len(m.group()), text)
    if rx := _name_pattern(names):
        masked = rx.sub(lambda m: " " * len(m.group()), masked)
    out = []
    for m in _FIGURE.finditer(masked):
        num = m.group("num").replace(",", "")
        places = len(num.split(".")[1]) if "." in num else 0
        scale = _SCALE.get((m.group("scale") or "").lower(), 0)
        value = Decimal(num).scaleb(scale)
        tol = Decimal(5).scaleb(scale - places - 1)
        kind = "pct" if m.group("pct") else "money" if m.group("dollar") or scale else "number"
        if kind == "number" and "." not in num and value < 1000:
            continue
        out.append(Figure(m.group().strip(), value, tol, kind))
    return out


# --- facts against the data -----------------------------------------------------------------


def _expected(fact: Fact, view: View) -> Decimal | None:
    cell = view.get(Period(fact.period_start, fact.period_end), {}).get(fact.item or "")
    if cell is None:
        return None
    if fact.field == "previously_reported":
        return cell.previously_reported
    if fact.field == "adjustment":
        return cell.adjustment
    if fact.field == "restated":
        return _restated(cell)
    if fact.field == "pct_change":
        base = cell.previously_reported
        return abs(cell.adjustment) / abs(base) * 100 if base else None
    return None


def check_facts(facts: list[Fact], view: View) -> tuple[list[Fact], list[Problem]]:
    """(facts that match the stored data, problems)."""
    good, problems = [], []
    for f in facts:
        where = f"{f.item} {f.field} {f.period_start or ''}..{f.period_end}"
        want = _expected(f, view)
        if want is None:
            problems.append(Problem("missing_cell", f"{where}: no stored value"))
        elif abs(want - f.value) > Decimal("0.0001") * max(abs(want), 1):
            problems.append(Problem("fact_mismatch", f"{where}: fact {f.value} but stored {want}"))
        else:
            good.append(f)
    return good, problems


# --- text against the facts -----------------------------------------------------------------


def matches(fig: Figure, fact: Fact) -> bool:
    """Does the written figure stand for this fact, at the precision it was written?
    Amounts are written as magnitudes (the words carry the direction)."""
    is_pct = fact.field == "pct_change"
    if (fig.kind == "pct") != is_pct:
        return False
    return abs(abs(fact.value) - fig.value) <= fig.tolerance + Decimal("1e-9")


def check_text(
    text: str, facts: list[Fact], names: set[str] | frozenset[str] = frozenset()
) -> tuple[int, list[str]]:
    """(figures read, figures that match no fact)."""
    found = figures(text, names)
    return len(found), [f.text for f in found if not any(matches(f, x) for x in facts)]


def validate(e: Explanation, view: View) -> Validation:
    good, problems = check_facts(e.facts, view)
    v = Validation(facts_checked=len(e.facts), problems=problems)
    for i, text in [(None, e.headline), *enumerate(e.sentences)]:
        n, bad = check_text(text, good, e.names)
        v.figures_checked += n
        v.problems += [
            Problem("unmatched_number", f"{b!r} matches no stored value", i) for b in bad
        ]
        found = dates(text)
        v.dates_checked += len(found)
        v.problems += [
            Problem("unmatched_date", f"{shown!r} is not a date in the filings", i)
            for shown, iso in found
            if iso not in e.known_dates
        ]
    return v


def enforce(e: Explanation, view: View, fallback_headline: str) -> tuple[Explanation, Validation]:
    """The explanation with every sentence that failed removed (and the headline replaced if
    it failed), plus the validation of the original. What is returned always validates."""
    v = validate(e, view)
    if v.ok:
        return e, v
    bad_sentences = {p.sentence for p in v.problems if p.sentence is not None}
    good, _ = check_facts(e.facts, view)
    headline_bad = any(
        p.sentence is None for p in v.problems if p.kind in ("unmatched_number", "unmatched_date")
    )
    kept = [s for i, s in enumerate(e.sentences) if i not in bad_sentences]
    shown = " ".join([e.headline if not headline_bad else fallback_headline, *kept])
    used = [f for f in good if any(matches(fig, f) for fig in figures(shown, e.names))]
    cleaned = replace(
        e,
        headline=fallback_headline if headline_bad else e.headline,
        sentences=kept,
        facts=used,
    )
    return cleaned, v

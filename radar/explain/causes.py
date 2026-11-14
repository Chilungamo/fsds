"""Root-cause classifier for restatements (design doc §2.3, §5.4).

Version 1 is transparent and rule-based: each cause has weighted phrases; a cause's score
is the sum of the weights of the distinct phrases found in the company's own words (the
Item 4.02 section and the amendment's explanatory note), plus evidence from which line
items actually changed. Every label comes with the sentences that triggered it, so the
dashboard can show why a case was tagged.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field

# code -> (label, [(pattern, weight), ...])
CAUSES: dict[str, tuple[str, list[tuple[str, float]]]] = {
    "REV": (
        "Revenue recognition",
        [
            (r"revenue recogni\w+", 1.5),
            (r"\bASC 606\b|revenue from contracts with customers", 1.5),
            (r"percent(?:age)?[- ]of[- ]completion", 1.5),
            (r"principal (?:versus|vs\.?) agent|gross (?:versus|vs\.?) net", 1.5),
            (r"performance obligations?", 1.0),
            (r"(?:offset to|reduction of|recogni[sz]ed as)\s+(?:\w+\s+){0,2}revenues?", 1.5),
            (r"software subscriptions?|bill[- ]and[- ]hold|cut-?off", 1.0),
            (r"\b(?:net sales|revenues?)\b", 0.3),
        ],
    ),
    "EXP": (
        "Expense capitalisation or accruals",
        [
            (r"costs? to obtain (?:\w+ ){0,2}contracts?|\bASC 340-40\b", 2.0),
            (
                r"(?:expensed|capitali[sz]ed)\b[^.]{0,80}"
                r"rather than (?:deferred|capitali[sz]ed|expensed)",
                2.0,
            ),
            (r"improper(?:ly)? (?:continued )?capitali[sz]\w+", 1.5),
            (r"should have been (?:expensed|capitali[sz]ed|deferred)|\bASC 730\b", 1.5),
            (r"accrued (?:liabilities|expenses)|under-?accru\w+|over-?accru\w+", 1.0),
            (r"(?:stock|share)-based compensation", 1.0),
        ],
    ),
    "IMPAIR": (
        "Impairment or asset valuation",
        [
            (r"\bimpairment\b", 1.5),
            (r"\bgoodwill\b", 1.0),
            (r"written down|write-?downs?\b|write-?offs?\b", 1.0),
            (
                r"(?:carrying value|fair value) of (?:the )?"
                r"(?:\w+ ){0,3}(?:reporting unit|assets?)",
                1.0,
            ),
        ],
    ),
    "INV": (
        "Inventory and cost of sales",
        [
            (r"\binventor(?:y|ies)\b", 1.5),
            (r"physical (?:inventory )?counts?", 1.5),
            (r"cost of (?:goods sold|revenues?|sales)", 0.5),
            (r"obsolescence|standard costs?|\bLIFO\b", 1.0),
        ],
    ),
    "TAX": (
        "Income taxes",
        [
            (
                r"(?<!before )income tax(?:es)? (?:expense|benefit|provision|liabilit\w+)"
                r"|provision for income taxes",
                1.5,
            ),
            (r"deferred (?:income )?tax (?:assets?|liabilit\w+)", 1.5),
            (r"valuation allowance|\bASC 740\b|uncertain tax positions?", 1.5),
        ],
    ),
    "DEBT_EQ": (
        "Debt and equity classification",
        [
            (r"\bwarrants?\b", 1.0),
            (r"\bASC 815-40\b|derivative liabilit\w+|embedded derivatives?", 1.5),
            (r"temporary equity|subject to (?:possible )?redemption", 1.0),
            (
                r"classified as (?:a )?liabilit\w+(?: rather than| instead of)? equity"
                r"|liabilities,? rather than equity",
                1.5,
            ),
            (r"convertible (?:notes|debt|preferred)", 1.0),
        ],
    ),
    "EPS": (
        "Earnings per share",
        [
            (
                r"(?:net )?(?:income|loss|earnings)(?:\s*/\s*\(loss\))? "
                r"per (?:ordinary |common )?share",
                1.5,
            ),
            (r"weighted[- ]average (?:number of )?(?:ordinary |common )?shares", 1.0),
            (r"two-class method|allocation of (?:net )?income among (?:the )?classes", 1.5),
        ],
    ),
    "LEASE": (
        "Leases",
        [
            (r"\bASC 842\b", 1.5),
            (r"right-of-use assets?|lease liabilit\w+", 1.5),
            (r"\boperating leases?\b|\bfinance leases?\b", 0.5),
        ],
    ),
    "CF_CLASS": (
        "Cash flow classification",
        [
            (
                r"(?:classif|reclassif)\w+ (?:\w+ ){0,6}(?:between|from|to) "
                r"(?:\w+ ){0,3}(?:operating|investing|financing) activities",
                2.0,
            ),
            (r"statements? of cash flows", 0.5),
        ],
    ),
    "CONSOL": (
        "Consolidation",
        [
            (r"variable interest entit\w+|\bVIEs?\b|primary beneficiary", 1.5),
            (r"deconsolidat\w+|should (?:not )?have been consolidated", 1.5),
        ],
    ),
}

# Line items whose restatement is evidence for a cause (weight 1.0 each, once per cause).
ITEM_EVIDENCE: dict[str, set[str]] = {
    "IMPAIR": {"impairment", "goodwill", "intangibles"},
    "INV": {"inventory", "chg_inventory"},
    "REV": {"revenue", "deferred_revenue", "chg_deferred_revenue"},
    "TAX": {"tax_expense", "deferred_tax_assets", "deferred_tax_liabilities"},
    "DEBT_EQ": {"warrant_liabilities", "warrant_fv_change", "temporary_equity"},
    "LEASE": {"operating_lease_rou"},
}

THRESHOLD = 1.5
SECONDARY_SHARE = 0.5  # a second or third cause needs at least half the top cause's score
_COMPILED = {
    code: [(re.compile(p, re.I), w) for p, w in patterns]
    for code, (_label, patterns) in CAUSES.items()
}


@dataclass
class Cause:
    code: str
    label: str
    score: float
    confidence: float
    evidence: list[str] = field(default_factory=list)  # the company's own sentences
    from_items: list[str] = field(default_factory=list)  # restated line items that support it


def _snippet(text: str, start: int, end: int, before: int = 110, after: int = 170) -> str:
    """The words around a match, trimmed to whole words, as quoted evidence."""
    lo, hi = max(0, start - before), min(len(text), end + after)
    if lo > 0:
        lo = text.find(" ", lo) + 1 or lo
    if hi < len(text):
        hi = text.rfind(" ", start, hi) if text.rfind(" ", start, hi) > end else hi
    body = " ".join(text[lo:hi].split())
    return f"{'…' if lo > 0 else ''}{body}{'…' if hi < len(text) else ''}"


def classify(text: str, changed_items: Iterable[str] = (), limit: int = 3) -> list[Cause]:
    """Causes found in ``text``, most likely first. Empty when nothing reaches the
    threshold (the dashboard then shows no cause rather than a guess)."""
    text = text or ""
    items = set(changed_items)
    found: list[Cause] = []
    for code, patterns in _COMPILED.items():
        score, hits = 0.0, []
        for rx, weight in patterns:
            m = rx.search(text)
            if m:
                score += weight
                hits.append((weight, m.start(), m.end()))
        supporting = sorted(items & ITEM_EVIDENCE.get(code, set()))
        if supporting:
            score += 1.0
        if score < THRESHOLD:
            continue
        evidence: list[str] = []
        for _w, start, end in sorted(hits, key=lambda h: -h[0]):
            quote = _snippet(text, start, end)
            if not any(quote[1:60] in e for e in evidence):
                evidence.append(quote)
            if len(evidence) == 2:
                break
        found.append(
            Cause(
                code=code,
                label=CAUSES[code][0],
                score=round(score, 2),
                confidence=round(min(score / 4.0, 1.0), 2),
                evidence=evidence,
                from_items=supporting,
            )
        )
    found.sort(key=lambda c: -c.score)
    if found:
        floor = found[0].score * SECONDARY_SHARE
        found = [found[0]] + [c for c in found[1:] if c.score >= floor]
    return found[:limit]


if __name__ == "__main__":  # python -m radar.explain.causes FILE.htm[.gz] ...
    import sys

    from radar.extract.ixbrl import load_document
    from radar.parse.item402 import parse_item402

    for path in sys.argv[1:]:
        section = parse_item402(load_document(path)).section_text
        causes = classify(section)
        print(path.rsplit("/", 1)[-1])
        for c in causes:
            print(f"  {c.code:<9} {c.label:<36} score {c.score:<5} confidence {c.confidence}")
            print(f"            “{c.evidence[0][:170]}”")
        if not causes:
            print("  (no cause above the threshold)")

"""Parse the Item 4.02 (Non-Reliance) section of a Form 8-K.

Extracts what the dashboard can show before any restated numbers exist: which periods
can no longer be relied upon, whether the auditor forced the issue (4.02(b)), dollar
amounts mentioned, and whether an amendment is promised.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from lxml import html as lxml_html

PARSER_VERSION = "item402/2"

MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
_MONTH_DAY = rf"(?:{MONTHS})\s+\d{{1,2}}(?!\d)"
_YEAR = r"(?:19|20)\d{2}\b"
_SEP = r"(?:\s*,\s*(?:and\s+)?|\s+and\s+|,?\s+of\s+each\s+of\s+)"
# Everything after "ended" that is a list of month-days and years:
#   "March 31, 2025, June 30, 2025 and September 30, 2025"
#   "December 31, 2025, 2024 and 2023"
#   "September 30, June 30 and March 31 of each of 2025, 2024 and 2023"
# "ended <dates>" for income-statement periods; "(balance sheet) as of <dates>" for the
# point-in-time statements typical of SPAC warrant restatements (2021).
_ENDED_RUN = re.compile(
    rf"\b(?P<anchor>ended|as\s+of)\s+(?P<run>{_MONTH_DAY}(?:{_SEP}(?:{_MONTH_DAY}|{_YEAR}))*)",
    re.I,
)
_BALANCE_SHEET = re.compile(r"balance\s+sheets?|statements?\s+of\s+financial\s+position", re.I)
_TOKEN = re.compile(rf"(?P<md>{_MONTH_DAY})|(?P<year>{_YEAR})", re.I)
# The sentence that actually states non-reliance; its periods are the authoritative list.
_NON_RELIANCE_SENTENCE = re.compile(r"relied upon|relied on\b|should be restated", re.I)
_SENTENCE_SPLIT = re.compile(r"(?<!\bInc)(?<!\bCorp)(?<!\bCo)(?<!\bLtd)(?<!\bNo)\.\s+(?=[A-Z])")
_HEADING = re.compile(r"Item\s*4\.02\.?\s*(?=Non[-\s]?Reliance)", re.I)
# The next item's heading ends the section. "Item 4.02" itself never does: filers refer
# back to it mid-section ("... the matters disclosed pursuant to this Item 4.02.").
_NEXT_HEADING = re.compile(
    r"Item\s*(?!4\.02)\d\.\d{2}\.?\s*(?=[A-Z][a-z])|SIGNATURES?\b"
    r"|Pursuant to the requirements of the Securities Exchange Act"
)
_MONEY = re.compile(
    r"\$\s?(?P<num>\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)\s*(?P<unit>thousand|million|billion)?",
    re.I,
)
_SCALE = {"": 1, "thousand": 1e3, "million": 1e6, "billion": 1e9}
_SUB_B = re.compile(
    r"4\.02\s*\(b\)|\(b\)\s*(?:Notice|Non-Reliance on a Previously Issued Audit)|"
    r"(?:independent (?:registered )?(?:public )?account\w*|auditor)\s+"
    r"(?:advised|notified|informed)",
    re.I,
)
_AUDITOR_DISCUSSED = re.compile(
    r"discuss\w*(?:[^.]|\.(?=\d)){0,250}?(?:independent registered public accounting firm|"
    r"independent (?:auditor|accountant)s?|auditors?\b)",
    re.I,
)
_AMENDMENT_PROMISED = re.compile(
    r"(?:amend\w*|restate\w*)[^.]{0,200}?Form\s+10-[KQ]|Form\s+10-[KQ]/A", re.I
)
_PREPARING = re.compile(r"prepar(?:e|es|ed|ing|ation)\b", re.I)
_ANNUAL = re.compile(r"\byears?\b|annual", re.I)
_INTERIM = re.compile(r"months|quarter\w*|interim|periods?\b", re.I)
# "its Quarterly Report on Form 10-Q for the three months ended ..." names a report, not a
# non-reliance period (the periods are stated as "financial statements ... ended ..."),
# while "Form 10-Q/A for the period ended" names the coming amendment of an affected period.
_UPCOMING_REPORT = re.compile(r"Form\s+10-[KQ]\s+(?:for|as of)\b[^.]*$", re.I)
# Other dated contexts that are not restated periods: auditor engagement years and
# internal-control conclusions ("not effective as of the end dates of the fiscal years ...").
_OTHER_CONTEXT = re.compile(
    r"(?:accounting firm|auditors?)\s+for\s+the\s+|effective\s+as\s+of|end\s+dates?\s+of", re.I
)
_SENTENCE_END = re.compile(r"[.;]\s+(?=[A-Z(])")


def _kind(lead: str) -> str:
    """The period keyword nearest to "ended" wins: "nine months ended Sept 30, the year
    ended Dec 31" makes the second date annual."""
    last = {}
    for kind, rx in (("annual", _ANNUAL), ("interim", _INTERIM)):
        hits = list(rx.finditer(lead))
        if hits:
            last[kind] = hits[-1].end()
    return max(last, key=last.get) if last else "unknown"


@dataclass
class AffectedPeriod:
    period_end: str  # ISO date
    kind: str  # annual | interim | unknown


@dataclass
class Item402:
    found: bool
    sub_item: str  # 'a' (issuer concluded) or 'b' (auditor notice)
    periods: list[AffectedPeriod] = field(default_factory=list)
    amounts: list[float] = field(default_factory=list)
    auditor_discussed: bool = False
    amendment_promised: bool = False
    section_text: str = ""

    def to_dict(self, text_limit: int = 4000) -> dict[str, Any]:
        d = asdict(self)
        d["section_text"] = self.section_text[:text_limit]
        return d


def document_text(raw: bytes) -> str:
    """Visible text of an HTML/XHTML filing with whitespace collapsed."""
    doc = lxml_html.fromstring(raw)
    for bad in doc.xpath("//script|//style|//*[local-name()='header']"):
        bad.drop_tree()  # ix:header holds hidden XBRL facts, not prose
    return " ".join(doc.text_content().split())


def find_section(text: str) -> str | None:
    m = _HEADING.search(text) or re.search(r"Item\s*4\.02", text, re.I)
    if not m:
        return None
    rest = text[m.end() :]
    # skip the heading title itself before looking for the next heading
    title_end = re.search(r"Interim Review\.?|Audit Report\.?", rest[:250])
    offset = title_end.end() if title_end else 0
    nxt = _NEXT_HEADING.search(rest, offset)
    return rest[: nxt.start()] if nxt else rest[:20000]


def _iso(month_day: str, year: str) -> str | None:
    try:
        return (
            datetime.strptime(f"{' '.join(month_day.split())} {year}", "%B %d %Y")
            .date()
            .isoformat()
        )
    except ValueError:  # e.g. "February 30"
        return None


def expand_dates(run: str) -> list[str]:
    """Turn a run of month-days and years into ISO dates.

    Month-days collect until years follow; each year applies to the whole group, so
    "Sept 30, June 30 and March 31 of each of 2025 and 2024" gives six dates and
    "December 31, 2025, 2024 and 2023" gives three.
    """
    dates: list[str] = []
    pending: list[str] = []  # month-days waiting for their year(s)
    group: list[str] = []  # month-days the most recent year applied to
    prev = None
    for t in _TOKEN.finditer(run):
        if t["md"]:
            if prev == "year":
                pending = []
            pending.append(t["md"])
            prev = "md"
        else:
            if prev == "md":
                group, pending = pending, []
            dates += [d for md in group if (d := _iso(md, t["year"]))]
            prev = "year"
    return dates


def _periods_in(text: str, skip_report_refs: bool) -> dict[tuple[str, str], AffectedPeriod]:
    found: dict[tuple[str, str], AffectedPeriod] = {}
    for m in _ENDED_RUN.finditer(text):
        before = text[max(0, m.start() - 150) : m.start()]
        cuts = list(_SENTENCE_END.finditer(before))
        if cuts:  # stay inside the current sentence
            before = before[cuts[-1].end() :]
        if (
            _PREPARING.search(before)
            or (skip_report_refs and _UPCOMING_REPORT.search(before[-60:]))
            or _OTHER_CONTEXT.search(before)
        ):
            continue
        if m["anchor"].lower() != "ended" and not _BALANCE_SHEET.search(before[-80:]):
            continue  # "as of" counts only for balance sheets ("as of the date of ...")
        kind = _kind(before[-45:]) if m["anchor"].lower() == "ended" else "unknown"
        for d in expand_dates(m["run"]):
            found[(d, kind)] = AffectedPeriod(d, kind)
    return found


def affected_periods(section: str) -> list[AffectedPeriod]:
    """Reporting periods that can no longer be relied upon, classified annual/interim.

    Dates follow "ended" (lists included). If the section has a sentence stating the
    non-reliance ("... should no longer be relied upon"), only its dates count; later
    background paragraphs mention many other periods. Dates of the period being
    *prepared* ("In preparing its 10-Q for the period ended ...") are skipped.
    """
    statement = " ".join(
        s for s in _SENTENCE_SPLIT.split(section) if _NON_RELIANCE_SENTENCE.search(s)
    )
    # inside the non-reliance sentence "its Form 10-Q for the periods ended ..." names the
    # affected reports, so report references are kept there
    found = _periods_in(statement, skip_report_refs=False) if statement else {}
    if not found:
        found = _periods_in(section, skip_report_refs=True)
    return sorted(found.values(), key=lambda p: (p.period_end, p.kind))


def amounts(section: str) -> list[float]:
    out = []
    for m in _MONEY.finditer(section):
        try:
            out.append(float(m["num"].replace(",", "")) * _SCALE[(m["unit"] or "").lower()])
        except ValueError:
            continue
    return out


def parse_item402(raw: bytes) -> Item402:
    section = find_section(document_text(raw))
    if section is None:
        return Item402(found=False, sub_item="a")
    return Item402(
        found=True,
        sub_item="b" if _SUB_B.search(section) else "a",
        periods=affected_periods(section),
        amounts=amounts(section),
        auditor_discussed=bool(_AUDITOR_DISCUSSED.search(section)),
        amendment_promised=bool(_AMENDMENT_PROMISED.search(section)),
        section_text=section.strip(),
    )

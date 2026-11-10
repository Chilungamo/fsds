"""Fallback extractor: restatement tables printed in HTML without XBRL restatement tags.

Small filers often tag only the face statements, so their "As Previously Reported |
Adjustment | As Restated" tables exist only as HTML (design doc §6.3). The layout varies:

* the period can sit above the header, inside the header cells ("September 30, 2019 As
  Reported"), or in section rows ("Balance sheet as of November 24, 2020");
* one table may hold several column groups (one per period) side by side;
* numbers are split across cells ("$", "(6,648", ")") and padded with invisible characters.

The table is turned into a grid with colspans expanded; each role (previously reported,
adjustment, restated) owns a band of columns, and a row's value for a role is whatever
number appears in that band.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation

from lxml import html as lxml_html

from radar.normalize.normalize import catalog, normalize_label

PREV, ADJ, REST = "previously_reported", "adjustment", "restated"
_ROLE_PATTERNS = [
    (
        PREV,
        re.compile(
            r"(?:previously|originally) (?:reported|filed|issued|presented)"
            r"|as reported|as filed|as presented",
            re.I,
        ),
    ),
    (
        ADJ,
        re.compile(r"adjust|correction|impact|effect of|reclassification|\berrors?\b|change", re.I),
    ),
    (REST, re.compile(r"restated|as revised|as corrected|as adjusted|revised", re.I)),
]

MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
_DATE = rf"(?:{MONTHS})\s+\d{{1,2}},?\s+\d{{4}}"
_DATE_RE = re.compile(_DATE, re.I)
_N_MONTHS = {"three": 3, "six": 6, "nine": 9, "twelve": 12, "3": 3, "6": 6, "9": 9, "12": 12}
_DURATION_RE = re.compile(
    rf"(?P<n>three|six|nine|twelve|3|6|9|12)[- ]months? (?:period )?ended,?\s+(?P<end>{_DATE})"
    rf"|(?:fiscal )?year ended,?\s+(?P<yend>{_DATE})"
    rf"|quarter ended,?\s+(?P<qend>{_DATE})"
    rf"|(?:period )?from\s+(?P<from>{_DATE})(?:\s*\([^)]*\))?\s+(?:to|through)\s+(?P<to>{_DATE})",
    re.I,
)
_SCALE_RE = re.compile(r"in (thousands|millions|billions)", re.I)
_SCALES = {"thousands": Decimal(1000), "millions": Decimal(10**6), "billions": Decimal(10**9)}
_STATEMENT_RE = [
    ("BS", re.compile(r"balance sheets?|financial (?:position|condition)", re.I)),
    ("CF", re.compile(r"cash flows?", re.I)),
    # "statement of changes in stockholders' equity", not a "... AND STOCKHOLDERS' EQUITY" caption
    (
        "EQ",
        re.compile(
            r"statements? of (?:changes in )?(?:stockholders?|shareholders?)['’]?s?['’]? "
            r"(?:equity|deficit)|changes in (?:stockholders?['’]?s?['’]? )?equity",
            re.I,
        ),
    ),
    (
        "IS",
        re.compile(
            r"statements? of (?:operations|income|comprehensive|earnings|loss)|income statement",
            re.I,
        ),
    ),
]
_PER_SHARE = re.compile(r"per (?:common |ordinary )?share|\beps\b", re.I)
_SHARE_COUNT = re.compile(
    r"(?:weighted|number of|outstanding).*shares|shares.*(?:outstanding|issued)", re.I
)
_NUMBER = re.compile(r"\(?-?\$?\s*\d[\d,]*(?:\.\d+)?\)?")


@dataclass
class HtmlRow:
    label: str
    canonical_item: str | None
    statement: str | None
    period_start: str | None
    period_end: str
    unit: str
    previously_reported: Decimal | None
    adjustment: Decimal | None
    restated: Decimal | None
    tolerance: Decimal

    @property
    def check_ok(self) -> bool | None:
        if None in (self.previously_reported, self.adjustment, self.restated):
            return None
        return abs(self.previously_reported + self.adjustment - self.restated) <= self.tolerance

    @property
    def concept(self) -> str:
        return f"label:{self.statement or '?'}:{normalize_label(self.label)[:200]}"


# --- small helpers --------------------------------------------------------------------


def clean(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = "".join(ch for ch in text if unicodedata.category(ch)[0] != "C" or ch in "\n\t")
    return " ".join(text.split())


def parse_number(text: str) -> Decimal | None:
    """'$ (6,648 )' -> -6648; '—' or '-' -> 0; '' -> None."""
    t = clean(text).replace("$", "").replace(" ", "")
    if not t:
        return None
    if re.fullmatch(r"[-–—]+|nil|none", t, re.I):
        return Decimal(0)
    m = _NUMBER.search(t)
    if not m:
        return None
    raw = m.group(0)
    negative = raw.startswith("(") or "(" in t.split(raw)[0] or raw.lstrip("($ ").startswith("-")
    digits = re.sub(r"[^\d.]", "", raw)
    try:
        value = Decimal(digits)
    except InvalidOperation:
        return None
    return -value if negative else value


def _month_end(y: int, m: int) -> date:
    return date(y + (m == 12), m % 12 + 1, 1) - timedelta(days=1)


def _minus_months(d: date, months: int) -> date:
    """Same day ``months`` earlier; month ends map to month ends (Sep 30 - 9 = Dec 31)."""
    y, m = divmod(d.year * 12 + d.month - 1 - months, 12)
    m += 1
    if d == _month_end(d.year, d.month):
        return _month_end(y, m)
    for day in (d.day, 30, 29, 28):
        try:
            return date(y, m, day)
        except ValueError:
            continue
    raise ValueError(d)


def _iso(s: str) -> date:
    return datetime.strptime(re.sub(r"\s+", " ", s.replace(",", "")).strip(), "%B %d %Y").date()


def parse_period(text: str) -> tuple[str | None, str] | None:
    """Most specific period mentioned in ``text``: (start or None for an instant, end)."""
    for m in _DURATION_RE.finditer(text):
        try:
            if m["n"]:
                end = _iso(m["end"])
                start = _minus_months(end, _N_MONTHS[m["n"].lower()]) + timedelta(days=1)
            elif m["yend"]:
                end = _iso(m["yend"])
                start = _minus_months(end, 12) + timedelta(days=1)
            elif m["qend"]:
                end = _iso(m["qend"])
                start = _minus_months(end, 3) + timedelta(days=1)
            else:
                start, end = _iso(m["from"]), _iso(m["to"])
        except ValueError:
            continue
        return start.isoformat(), end.isoformat()
    dates = _DATE_RE.findall(text)
    if dates:
        try:
            return None, _iso(dates[-1]).isoformat()
        except ValueError:
            return None
    return None


def statement_of(text: str) -> str | None:
    for code, rx in _STATEMENT_RE:
        if rx.search(text):
            return code
    return None


def preceding_text(el, limit: int = 600) -> str:
    """Text just before ``el`` in document order (captions such as "in thousands")."""
    parts: list[str] = []
    node = el
    total = 0
    while node is not None and total < limit:
        for sib in node.itersiblings(preceding=True):
            if not isinstance(sib.tag, str):  # comments, processing instructions
                continue
            t = clean(sib.text_content())
            if t:
                parts.append(t)
                total += len(t)
                if total >= limit:
                    break
        node = node.getparent()
    return " ".join(reversed(parts))[-limit:]


# --- grid and header ---------------------------------------------------------------------


def grid(table) -> list[list[str]]:
    rows = []
    for tr in table.iter("tr"):
        row: list[str] = []
        for cell in tr:
            if cell.tag not in ("td", "th"):
                continue
            try:
                span = max(1, min(int(cell.get("colspan", "1")), 50))
            except ValueError:
                span = 1
            row += [clean(cell.text_content())] * span
        if any(row):
            rows.append(row)
    return rows


def _role_of(text: str) -> str | None:
    for role, rx in _ROLE_PATTERNS:
        if rx.search(text):
            return role
    return None


@dataclass
class Group:
    bands: dict[str, tuple[int, int]]  # role -> [start, end)
    header_text: str  # header cells of this group (may carry the period)


def find_header(rows: list[list[str]]) -> tuple[int, list[Group]] | None:
    """Locate the header row (role labels may span up to three rows) and its column groups."""
    for r in range(min(len(rows), 15)):
        for window in (1, 2, 3):
            if r + window > len(rows):
                break
            width = max(len(x) for x in rows[r : r + window])
            cols = [
                " ".join(
                    rows[i][c] for i in range(r, r + window) if c < len(rows[i]) and rows[i][c]
                )
                for c in range(width)
            ]
            spans: list[tuple[str, int, int, str]] = []  # role, start, end, text
            for c, text in enumerate(cols):
                if c == 0 or not text:
                    continue  # column 0 holds the row labels
                role = _role_of(text)
                if role is None:
                    continue
                if spans and spans[-1][0] == role and spans[-1][2] == c and spans[-1][3] == text:
                    spans[-1] = (role, spans[-1][1], c + 1, text)
                else:
                    spans.append((role, c, c + 1, text))
            roles = [s[0] for s in spans]
            if PREV in roles and REST in roles:
                return r + window - 1, _groups(spans, width)
    return None


def _groups(spans, width: int) -> list[Group]:
    groups: list[list[tuple[str, int, int, str]]] = []
    for s in spans:
        if not groups or s[0] == PREV or (s[0] in {g[0] for g in groups[-1]}):
            groups.append([s])
        else:
            groups[-1].append(s)
    out = []
    for gi, g in enumerate(groups):
        bands = {}
        next_start = groups[gi + 1][0][1] if gi + 1 < len(groups) else width + 50
        for i, (role, start, _end, _text) in enumerate(g):
            band_end = g[i + 1][1] if i + 1 < len(g) else next_start
            bands[role] = (start, band_end)
        if PREV in bands and REST in bands:
            out.append(Group(bands, " ".join(t for *_, t in g)))
    return out


def _band_value(row: list[str], band: tuple[int, int]) -> Decimal | None:
    cells = [row[c] for c in range(band[0], min(band[1], len(row)))]
    # a colspan repeats the same text; keep consecutive cells only once
    deduped = [t for i, t in enumerate(cells) if t and (i == 0 or t != cells[i - 1])]
    return parse_number(" ".join(deduped))


# --- table extraction ---------------------------------------------------------------------


def extract_table(table, form: str = "") -> list[HtmlRow]:
    rows = grid(table)
    found = find_header(rows)
    if not found:
        return []
    header_end, groups = found
    above = " ".join(" ".join(dict.fromkeys(r)) for r in rows[: header_end + 1])
    before = preceding_text(table)
    scale_m = list(_SCALE_RE.finditer(f"{before} {above}"))
    scale = _SCALES[scale_m[-1].group(1).lower()] if scale_m else Decimal(1)
    table_period = parse_period(above) or parse_period(before[-300:])
    table_statement = statement_of(above) or statement_of(before[-300:])

    cat = catalog()
    out: list[HtmlRow] = []
    section_period, section_statement = None, None
    for row in rows[header_end + 1 :]:
        label = next(
            (t for t in row[: min(b[0] for g in groups for b in g.bands.values())] if t), ""
        )
        values_present = any(
            _band_value(row, b) is not None for g in groups for b in g.bands.values()
        )
        if not values_present:
            text = " ".join(dict.fromkeys(t for t in row if t))
            section_period = parse_period(text) or section_period
            section_statement = statement_of(text) or section_statement
            continue
        if not label:
            continue
        statement_ctx = section_statement or table_statement
        item = cat.item_for_label(label, statement_ctx) if statement_ctx else None
        item = item or cat.item_for_label(label)
        per_share = bool(_PER_SHARE.search(label)) or (
            item is not None and item.key.startswith("eps")
        )
        shares = not per_share and bool(_SHARE_COUNT.search(label))
        row_scale = Decimal(1) if per_share or shares else scale
        for g in groups:
            period = section_period or parse_period(g.header_text) or table_period
            if period is None:
                continue
            vals = {role: _band_value(row, band) for role, band in g.bands.items()}
            prev, adj, rest = vals.get(PREV), vals.get(ADJ), vals.get(REST)
            if prev is None and rest is None:
                continue
            row_statement = (item.statement if item else None) or statement_ctx
            if row_statement in ("IS", "CF") and period[0] is None:
                continue  # a flow item needs a duration; a lone date is not enough
            if adj is None and prev is not None and rest is not None and ADJ not in g.bands:
                adj = rest - prev
            out.append(
                HtmlRow(
                    label=label,
                    canonical_item=item.key if item else None,
                    statement=row_statement,
                    period_start=period[0],
                    period_end=period[1],
                    unit="USD/shares" if per_share else "shares" if shares else "USD",
                    previously_reported=None if prev is None else prev * row_scale,
                    adjustment=None if adj is None else adj * row_scale,
                    restated=None if rest is None else rest * row_scale,
                    tolerance=row_scale if not per_share else Decimal("0.01"),
                )
            )
    return out


def extract_html(raw: bytes, form: str = "") -> list[HtmlRow]:
    """All restatement-table rows in a document, first occurrence of each line kept."""
    doc = lxml_html.fromstring(raw)
    seen: set[tuple] = set()
    out: list[HtmlRow] = []
    for table in doc.iter("table"):
        text = clean(table.text_content()).lower()
        if not (
            ("previously" in text or "as reported" in text or "originally" in text)
            and ("restated" in text or "adjust" in text or "revised" in text)
        ):
            continue
        for row in extract_table(table, form):
            key = (row.concept, row.period_start, row.period_end)
            if key not in seen:
                seen.add(key)
                out.append(row)
    return out

"""Extract restatement tables from Inline XBRL (iXBRL) filings.

Filers tag the columns of a restatement table with a dimension on the context:

=========================  ======================================================
Column in the filing       XBRL representation
=========================  ======================================================
As Previously Reported     ``srt:RestatementAxis = srt:ScenarioPreviouslyReportedMember``
Adjustment                 ``srt:RestatementAxis = srt:RestatementAdjustmentMember``
As Restated                default context (no RestatementAxis) or a "restated" member
=========================  ======================================================

The SEC companyfacts API omits dimensional facts, so we read them straight from the
filing. Members are matched on their *local name* because filers sometimes use
their own extension members (``abc:RestatementAdjustmentMember``) on the standard axis.
"""

from __future__ import annotations

import gzip
import re
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from lxml import etree

NS = {
    "ix": "http://www.xbrl.org/2013/inlineXBRL",
    "xbrli": "http://www.xbrl.org/2003/instance",
    "xbrldi": "http://xbrl.org/2006/xbrldi",
}

RESTATEMENT_AXES = {"RestatementAxis"}
PREVIOUSLY_REPORTED = {"ScenarioPreviouslyReportedMember", "PreviouslyReportedMember"}
ADJUSTMENT = {
    "RestatementAdjustmentMember",
    "RevisionOfPriorPeriodErrorCorrectionAdjustmentMember",
    "RevisionOfPriorPeriodAdjustmentMember",
}
COVER_FLAGS = {
    "DocumentType",
    "AmendmentFlag",
    "AmendmentDescription",
    "EntityRegistrantName",
    "EntityCentralIndexKey",
    "DocumentPeriodEndDate",
    "DocumentFiscalYearFocus",
    "DocumentFiscalPeriodFocus",
    "DocumentFinStmtErrorCorrectionFlag",
    "DocumentFinStmtRestatementRecoveryAnalysisFlag",
}

ROLE_PREVIOUS = "previously_reported"
ROLE_ADJUSTMENT = "adjustment"
ROLE_RESTATED = "restated"


def local(qname: str | None) -> str:
    return (qname or "").rsplit(":", 1)[-1].rsplit("}", 1)[-1]


@dataclass
class Context:
    id: str
    start: str | None
    end: str | None
    dims: dict[str, str]  # axis qname -> member qname


ERROR_BREAKDOWN_AXES = {
    "ErrorCorrectionsAndPriorPeriodAdjustmentsRestatementByRestatementPeriodAndAmountAxis"
}


def fmt_decimal(v: Decimal | None) -> str | None:
    """Plain decimal string without exponent: Decimal('5.2138E+7') -> '52138000'."""
    if v is None:
        return None
    s = format(v, "f")
    return s.rstrip("0").rstrip(".") if "." in s else s


@dataclass
class RestatementRow:
    concept: str
    period_start: str | None
    period_end: str | None
    unit: str | None
    dims: dict[str, str]
    previously_reported: Decimal | None
    adjustment: Decimal | None
    restated: Decimal | None
    tolerance: Decimal = Decimal(1)
    adjustment_derived: bool = False
    restated_derived: bool = False
    conflicts: list[str] = field(default_factory=list)

    @property
    def check_ok(self) -> bool | None:
        """True when previously reported + adjustment == restated (within rounding)."""
        if None in (self.previously_reported, self.adjustment, self.restated):
            return None
        if self.adjustment_derived or self.restated_derived:
            return None  # identity holds by construction; nothing was checked
        assert self.previously_reported is not None and self.adjustment is not None
        assert self.restated is not None
        return abs(self.previously_reported + self.adjustment - self.restated) <= self.tolerance

    @property
    def error_member(self) -> str | None:
        """Member naming a specific error when the filer breaks adjustments down by cause."""
        for axis, member in self.dims.items():
            if local(axis) in ERROR_BREAKDOWN_AXES:
                return member
        return None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        for k in ("previously_reported", "adjustment", "restated", "tolerance"):
            d[k] = fmt_decimal(d[k])
        d["check_ok"] = self.check_ok
        d["error_member"] = self.error_member
        return d


@dataclass
class RestatementExtraction:
    cover: dict[str, str]
    rows: list[RestatementRow]
    axes_seen: dict[str, set[str]]  # restatement axis -> members used in the document

    @property
    def has_restatement_dimension(self) -> bool:
        return bool(self.axes_seen)

    def find(
        self, concept: str, period_end: str, period_start: str | None = None, **dims: str
    ) -> RestatementRow | None:
        """Look up one row; ``dims`` defaults to the undimensioned (face statement) row."""
        for r in self.rows:
            if (
                local(r.concept) == local(concept)
                and r.period_end == period_end
                and r.period_start == period_start
                and {local(a): local(m) for a, m in r.dims.items()}
                == {local(a): local(m) for a, m in dims.items()}
            ):
                return r
        return None


def decimals_of(decimals: str | None) -> float:
    try:
        return float(int(decimals or ""))
    except ValueError:  # "INF" or missing: exact
        return float("inf")


def rounding_tolerance(decimals: float) -> Decimal:
    """One unit of the reported precision: decimals=-3 (thousands) -> 1000."""
    return Decimal(0) if decimals == float("inf") else Decimal(1).scaleb(-int(decimals))


def _round_to(value: Decimal, decimals: float) -> Decimal:
    if decimals == float("inf"):
        return value
    q = Decimal(1).scaleb(-int(decimals))
    return (value / q).quantize(Decimal(1), rounding="ROUND_HALF_UP") * q


def choose(observations: list[tuple[Decimal, float]]) -> tuple[Decimal, float, bool]:
    """Pick the most precise value for a cell.

    The same fact often appears twice: in a table (``decimals=-3``) and rounded in the
    narrative (``$124.4 million``, ``decimals=-5``). The most precise one wins; a
    conflict is reported only if a less precise value is not a rounding of it, or if
    two equally precise values differ.
    """
    best_dec = max(d for _, d in observations)
    best = sorted({v for v, d in observations if d == best_dec})
    value = best[0]
    conflict = len(best) > 1 or any(
        _round_to(value, d) != _round_to(v, d) for v, d in observations if d < best_dec
    )
    return value, best_dec, conflict


# ---------------------------------------------------------------------------
# parsing
# ---------------------------------------------------------------------------


def load_document(path: str | Path) -> bytes:
    p = Path(path)
    data = p.read_bytes()
    return gzip.decompress(data) if p.suffix == ".gz" else data


def parse(xhtml: bytes) -> etree._Element:
    parser = etree.XMLParser(huge_tree=True, recover=True, resolve_entities=False, no_network=True)
    root = etree.fromstring(xhtml, parser)
    if root is None:
        raise ValueError("Document could not be parsed as XHTML")
    return root


def contexts(root: etree._Element) -> dict[str, Context]:
    out: dict[str, Context] = {}
    for c in root.iterfind(".//xbrli:context", NS):
        dims = {
            m.get("dimension", ""): (m.text or "").strip()
            for m in c.iterfind(".//xbrldi:explicitMember", NS)
        }
        instant = c.findtext(".//xbrli:instant", namespaces=NS)
        start = c.findtext(".//xbrli:startDate", namespaces=NS)
        end = c.findtext(".//xbrli:endDate", namespaces=NS)
        out[c.get("id", "")] = Context(
            id=c.get("id", ""),
            start=start.strip() if start else None,
            end=(end or instant or "").strip() or None,
            dims=dims,
        )
    return out


def units(root: etree._Element) -> dict[str, str]:
    out: dict[str, str] = {}
    for u in root.iterfind(".//xbrli:unit", NS):
        num = [local(m.text) for m in u.iterfind(".//xbrli:unitNumerator/xbrli:measure", NS)]
        den = [local(m.text) for m in u.iterfind(".//xbrli:unitDenominator/xbrli:measure", NS)]
        if num:
            out[u.get("id", "")] = "*".join(num) + ("/" + "*".join(den) if den else "")
        else:
            out[u.get("id", "")] = "*".join(local(m.text) for m in u.iterfind("xbrli:measure", NS))
    return out


_NUM_CLEAN = re.compile(r"[^\d.]")


def fact_value(el: etree._Element) -> Decimal | None:
    """Numeric value of an ix:nonFraction honouring format, scale and sign."""
    if el.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true":
        return None
    fmt = local(el.get("format"))
    raw = "".join(el.itertext()).strip()
    if fmt in {"fixed-zero", "fixedzero", "zerodash"} or raw in {"", "-", "—", "–"}:
        return Decimal(0)
    if fmt in {"num-comma-decimal", "numcommadecimal", "numdotcomma"}:
        raw = raw.replace(".", "").replace(" ", "").replace(",", ".")
    try:
        value = Decimal(_NUM_CLEAN.sub("", raw))
    except InvalidOperation:
        return None
    value = value.scaleb(int(el.get("scale", "0")))
    return -value if el.get("sign") == "-" else value


def cover_facts(root: etree._Element) -> dict[str, str]:
    out: dict[str, str] = {}
    for tag in ("nonNumeric", "nonFraction"):
        for el in root.iterfind(f".//ix:{tag}", NS):
            name = local(el.get("name"))
            if name in COVER_FLAGS and name not in out:
                text = " ".join("".join(el.itertext()).split())
                if name.endswith("Flag"):
                    text = normalize_flag(text)
                out[name] = text
    return out


_TRUE = {"true", "yes", "1", "\u2612", "\u2611", "x"}  # ☒ ☑
_FALSE = {"false", "no", "0", "\u2610", ""}  # ☐


def normalize_flag(text: str) -> str:
    """Cover-page check boxes are often tagged on the box glyph itself (☒ / ☐)."""
    t = text.strip().lower()
    if t in _TRUE:
        return "true"
    if t in _FALSE:
        return "false"
    return text


def _role(member: str | None) -> str | None:
    if member is None:
        return ROLE_RESTATED
    name = local(member)
    if name in PREVIOUSLY_REPORTED:
        return ROLE_PREVIOUS
    if name in ADJUSTMENT:
        return ROLE_ADJUSTMENT
    if "Restated" in name or "AsRevised" in name:
        return ROLE_RESTATED
    return None  # some other member on the axis (e.g. a specific error); not a column


def extract(xhtml: bytes) -> RestatementExtraction:
    root = parse(xhtml)
    ctx = contexts(root)
    unit_map = units(root)

    axes_seen: dict[str, set[str]] = defaultdict(set)  # only axes used by actual facts

    # (concept, start, end, unit, other dims) -> role -> [(value, decimals)]
    cells: dict[tuple, dict[str, list[tuple[Decimal, float]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for el in root.iterfind(".//ix:nonFraction", NS):
        if el.find(".//ix:nonFraction", NS) is not None:
            continue  # outer wrapper of a nested fact; the inner one is used
        c = ctx.get(el.get("contextRef", ""))
        value = fact_value(el)
        if c is None or value is None:
            continue
        dims = dict(c.dims)
        member = None
        for axis in list(dims):
            if local(axis) in RESTATEMENT_AXES:
                member = dims.pop(axis)
                axes_seen[axis].add(member)
        role = _role(member)
        if role is None:
            continue
        key = (
            el.get("name", ""),
            c.start,
            c.end,
            unit_map.get(el.get("unitRef", ""), el.get("unitRef")),
            tuple(sorted(dims.items())),
        )
        cells[key][role].append((value, decimals_of(el.get("decimals"))))

    rows: list[RestatementRow] = []
    for (concept, start, end, unit, dims), by_role in cells.items():
        if ROLE_PREVIOUS not in by_role and ROLE_ADJUSTMENT not in by_role:
            continue  # ordinary fact, not part of a restatement table
        chosen: dict[str, Decimal] = {}
        precision: list[float] = []
        conflicts: list[str] = []
        for role, observations in by_role.items():
            value, dec, conflict = choose(observations)
            chosen[role] = value
            precision.append(dec)
            if conflict:
                conflicts.append(role)
        prev = chosen.get(ROLE_PREVIOUS)
        adj = chosen.get(ROLE_ADJUSTMENT)
        rest = chosen.get(ROLE_RESTATED)
        row = RestatementRow(
            concept=concept,
            period_start=start,
            period_end=end,
            unit=unit,
            dims=dict(dims),
            previously_reported=prev,
            adjustment=adj,
            restated=rest,
            tolerance=rounding_tolerance(min(precision)),
            conflicts=sorted(conflicts),
        )
        if adj is None and prev is not None and rest is not None:
            row.adjustment, row.adjustment_derived = rest - prev, True
        if rest is None and prev is not None and adj is not None:
            row.restated, row.restated_derived = prev + adj, True
        rows.append(row)

    rows.sort(key=lambda r: (r.period_end or "", r.period_start or "", r.concept, str(r.dims)))
    return RestatementExtraction(cover=cover_facts(root), rows=rows, axes_seen=dict(axes_seen))


def extract_file(path: str | Path) -> RestatementExtraction:
    return extract(load_document(path))

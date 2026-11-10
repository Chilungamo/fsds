"""Decide whether a 10-K/A or 10-Q/A actually restates financial statements.

Most amendments do not: 10-K/As usually just add Part III information (executive pay,
governance) or exhibits. The score is a transparent weighted sum of signals so every
decision can be explained and tuned (design doc §5.2).
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

from radar.extract.ixbrl import RestatementExtraction

TRIAGE_VERSION = "triage/2"
CONFIRM_AT = 0.5
REVIEW_AT = 0.3

WEIGHTS = {
    "restatement_axis": 0.45,
    "cover_error_flag": 0.25,
    "full_statements": 0.10,
    "restatement_text": 0.10,
    "states_restatement": 0.30,
    "open_case": 0.10,
    "part_iii_only": -0.60,
}

_RESTATEMENT_TEXT = re.compile(
    r"\brestat(?:e|ed|ement|ing)\b|as previously reported|non-?reliance|"
    r"correct(?:ion|ed)? of (?:an |the )?(?:error|misstatement)",
    re.I,
)
# The explanatory note says the amendment exists to restate the *financial statements*
# ("... to restate its unaudited condensed consolidated interim financial statements",
# "... to reflect the restatement of the Company's financial statements"). "Amend and
# restate Item 9" or "restates Part III in its entirety" does not qualify.
_STATES_RESTATEMENT = re.compile(
    r"\bto (?:amend and )?(?:restate|reflect the restatement of|correct)\b[^.]{0,120}?"
    r"financial statements|restatement of (?:its|the|our)\b[^.]{0,80}?financial statements",
    re.I,
)
_LIMITED_PURPOSE = re.compile(
    r"\b(?:sole(?:ly)?|only|limited)\b[^.]{0,120}\b(?:purpose|to (?:include|file|furnish|"
    r"provide|amend|add|correct)|being filed)",
    re.I,
)
# "... amends the Original 10-K to include the information required by Items 10 through 14
# of Part III" — the standard wording, with or without "solely".
_PART_III_INCLUDE = re.compile(
    r"to (?:include|provide|add|furnish)[^.]{0,80}?(?:Items?\s+10\s+(?:through|to|-)\s+14|"
    r"Part\s+III\b)",
    re.I,
)
_CHECKBOX_BOILERPLATE = re.compile(r"[^.]*indicate by check mark[^.]*\.?", re.I)
_PART_III_OR_EXHIBIT = re.compile(
    r"Part\s+III|Items?\s+1[0-4]\b|exhibits?\b|signature|auditor'?s? (?:report|consent)", re.I
)
_SENTENCE = re.compile(r"[^.]*(?:\.(?=\d)[^.]*)*\.?")


@dataclass
class TriageResult:
    score: float
    decision: str  # CONFIRMED | NEEDS_REVIEW | IGNORE
    signals: dict[str, bool]
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"version": TRIAGE_VERSION}


def explanatory_note(extraction: RestatementExtraction, text: str, chars: int = 20000) -> str:
    """The amendment's own description of itself: the tagged dei:AmendmentDescription when
    present, otherwise the start of the document."""
    note = extraction.cover.get("AmendmentDescription", "")
    if len(note) <= 200:
        note = f"{note} {text[:chars]}"
    # the cover page's check-box questions mention "correction of an error" verbatim
    return _CHECKBOX_BOILERPLATE.sub(" ", note)


def is_limited_purpose(note: str) -> str | None:
    """Return the sentence if the note says the amendment only adds Part III/exhibits."""
    for sentence in _SENTENCE.findall(note):
        limited = (
            _LIMITED_PURPOSE.search(sentence) and _PART_III_OR_EXHIBIT.search(sentence)
        ) or _PART_III_INCLUDE.search(sentence)
        if limited and not _RESTATEMENT_TEXT.search(sentence):
            return sentence.strip()[:500]
    return None


def triage(extraction: RestatementExtraction, text: str, has_open_case: bool) -> TriageResult:
    note = explanatory_note(extraction, text)
    changed = [r for r in extraction.rows if r.adjustment and not r.dims]
    limited = is_limited_purpose(note)
    restatement_text = _RESTATEMENT_TEXT.search(note)
    states = _STATES_RESTATEMENT.search(note)
    signals = {
        "restatement_axis": bool(changed),
        "cover_error_flag": extraction.cover.get("DocumentFinStmtErrorCorrectionFlag") == "true",
        "full_statements": "Assets" in extraction.face_concepts
        and bool({"NetIncomeLoss", "ProfitLoss"} & extraction.face_concepts),
        "restatement_text": bool(restatement_text),
        "states_restatement": bool(states),
        "open_case": has_open_case,
        "part_iii_only": limited is not None,
    }
    score = round(sum(WEIGHTS[k] for k, on in signals.items() if on), 3)
    decision = (
        "CONFIRMED" if score >= CONFIRM_AT else "NEEDS_REVIEW" if score >= REVIEW_AT else "IGNORE"
    )
    evidence = {
        "changed_rows": len(changed),
        "restatement_text": restatement_text.group(0) if restatement_text else None,
        "states_restatement": states.group(0)[:300] if states else None,
        "limited_purpose": limited,
    }
    return TriageResult(score=score, decision=decision, signals=signals, evidence=evidence)

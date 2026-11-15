"""LLM narrative for explanations (design doc §5.4, M4 step 4).

Claude rewrites the template explanation into plainer, better-flowing English and may use the
company's own description of the error for context. It is given the template text and the
list of validated figures, and told to use no other numbers. Its output then has to pass the
step-3 validator in full; if any figure fails, or the call fails for any reason, the template
explanation is kept. So the narrative can improve the words, never the numbers, and the
dashboard never waits on (or breaks because of) the API.

Needs the ``anthropic`` package (``pip install -e ".[llm]"``) and ANTHROPIC_API_KEY. Without
either, ``narrate`` returns the template explanation unchanged.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, replace
from typing import Any

from radar.explain.template import Explanation
from radar.explain.validate import Validation, dates, figures, matches, validate
from radar.propagate.engine import View

log = logging.getLogger(__name__)

MODEL = os.environ.get("RADAR_LLM_MODEL", "claude-opus-5-5")
EFFORT = os.environ.get("RADAR_LLM_EFFORT", "low")  # a rewrite, not a reasoning task
MAX_CONTEXT_CHARS = 6000  # of the company's own Item 4.02 text, sent for context

SYSTEM = """\
You write the summary card for a dashboard that tracks SEC financial restatements. Readers \
are investors and analysts who want to know, quickly and accurately, what went wrong, how \
big it is and where it shows up in the financial statements.

You are given a draft explanation generated from structured data, the figures it is allowed \
to use, and the company's own words from its Item 4.02 filing. Rewrite the draft so it reads \
naturally and is easy to follow.

Rules:
- Use only figures from the ALLOWED FIGURES list, written exactly as listed (you may drop \
figures you don't need). Do not compute, round, add up or invent any other number, \
including share counts, totals or ratios. Dates may be written as in the draft.
- Write amounts as magnitudes and let the words give the direction ("fell by $4.4 million", \
"the net loss widened"), as the draft does.
- Use the company's own words only to describe the cause more concretely. Never add facts \
that are in neither the draft nor the company's words, and never speculate about motives, \
fraud, management, the auditor or the share price.
- Keep "Big R" / "little r" wording if the draft has it.
- Headline: at most 12 words. Body: 3 to 6 sentences, plain English, no jargon a general \
investor would not know, no bullet points, no markdown.
"""

SCHEMA = {
    "type": "object",
    "properties": {
        "headline": {"type": "string"},
        "sentences": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["headline", "sentences"],
    "additionalProperties": False,
}


@dataclass
class NarrativeResult:
    explanation: Explanation  # what to show: the narrative, or the template if it failed
    used_llm: bool
    reason: str  # "ok", or why the template was kept
    validation: Validation | None = None
    usage: dict[str, int] | None = None


def _client():
    """An Anthropic client, or None if the package or the key is missing."""
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return None
    try:
        import anthropic
    except ImportError:
        return None
    return anthropic.Anthropic(max_retries=3, timeout=120.0)


def build_prompt(e: Explanation, company_text: str) -> str:
    figures = "\n".join(
        f"- {f.shown}  ({f.item}, {f.field.replace('_', ' ')}, "
        f"{f.period_start + ' to ' if f.period_start else 'as of '}{f.period_end})"
        for f in e.facts
    )
    words = " ".join(company_text.split())[:MAX_CONTEXT_CHARS]
    return (
        f"DRAFT HEADLINE:\n{e.headline}\n\n"
        f"DRAFT EXPLANATION:\n{e.text}\n\n"
        f"ALLOWED FIGURES:\n{figures or '(none: write no amounts or percentages)'}\n\n"
        f"COMPANY'S OWN WORDS (Item 4.02):\n<filing>\n{words}\n</filing>"
    )


def _call(client: Any, prompt: str) -> tuple[dict[str, Any] | None, str, dict[str, int]]:
    """(parsed JSON or None, reason, token usage)."""
    response = client.beta.messages.create(
        model=MODEL,
        max_tokens=4000,
        system=SYSTEM,
        messages=[{"role": "user", "content": prompt}],
        output_config={"effort": EFFORT, "format": {"type": "json_schema", "schema": SCHEMA}},
        # if a safety classifier declines, the API retries on a fallback model in the same call
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
    )
    usage = {
        "input_tokens": response.usage.input_tokens,
        "output_tokens": response.usage.output_tokens,
    }
    if response.stop_reason != "end_turn":
        return None, f"stop_reason={response.stop_reason}", usage
    text = next((b.text for b in response.content if b.type == "text"), "")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return None, "invalid JSON", usage
    return data, "ok", usage


def narrate(
    e: Explanation, view: View, company_text: str = "", client: Any | None = None
) -> NarrativeResult:
    """The LLM rewrite of ``e`` if it validates in full, otherwise ``e`` itself."""
    client = client if client is not None else _client()
    if client is None:
        return NarrativeResult(e, False, "no API key or anthropic package")
    try:
        data, reason, usage = _call(client, build_prompt(e, company_text))
    except Exception as exc:  # network, rate limit, API error: keep the template
        log.warning("narrative call failed: %s", exc)
        return NarrativeResult(e, False, f"API error: {type(exc).__name__}")
    if data is None:
        return NarrativeResult(e, False, reason, usage=usage)
    sentences = [s.strip() for s in data.get("sentences", []) if s and s.strip()]
    headline = (data.get("headline") or "").strip()
    if not headline or not sentences:
        return NarrativeResult(e, False, "empty response", usage=usage)
    # dates the company itself gives (e.g. when its audit committee concluded) may be used
    own_dates = {iso for _shown, iso in dates(company_text) if iso}
    candidate = replace(
        e,
        headline=headline,
        sentences=sentences,
        method="llm",
        known_dates=e.known_dates | own_dates,
    )
    v = validate(candidate, view)
    if not v.ok:
        log.info("narrative rejected: %s", [p.detail for p in v.problems])
        return NarrativeResult(e, False, "failed validation", v, usage)
    # keep only the facts the narrative actually shows, so each figure links to its source
    shown = figures(" ".join([headline, *sentences]), e.names)
    candidate.facts = [f for f in e.facts if any(matches(fig, f) for fig in shown)]
    return NarrativeResult(candidate, True, "ok", v, usage)

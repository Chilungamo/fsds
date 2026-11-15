"""LLM narrative: accepted only when every figure validates; the template is kept otherwise.

The offline tests use a fake client. ``-m llm`` runs the real API on the fixture cases
(needs ANTHROPIC_API_KEY; a few cents per run).
"""

import json
import os
from types import SimpleNamespace

import pytest

from radar.explain.narrative import build_prompt, narrate
from radar.explain.validate import validate
from radar.extract.ixbrl import load_document
from radar.parse.item402 import parse_item402

from .test_template import F
from .test_validate import REAL, real


class FakeClient:
    """Stands in for anthropic.Anthropic(): returns a canned JSON reply."""

    def __init__(self, reply=None, stop_reason="end_turn", error=None):
        self.reply, self.stop_reason, self.error = reply, stop_reason, error
        self.calls = []
        self.beta = SimpleNamespace(messages=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        text = self.reply if isinstance(self.reply, str) else json.dumps(self.reply)
        return SimpleNamespace(
            stop_reason=self.stop_reason,
            content=[SimpleNamespace(type="text", text=text)],
            usage=SimpleNamespace(input_tokens=1200, output_tokens=180),
        )


GOOD = {
    "headline": "Goodwill write-down deepens Universal Logistics' loss by $43.2 million",
    "sentences": [
        "On March 3, 2026, Universal Logistics told investors not to rely on its results for "
        "the period ended September 27, 2025.",
        "It should have recorded a larger goodwill impairment, and is reissuing the statements "
        "(a “Big R” restatement).",
        "The fix widens the nine-month net loss from $60.4 million to $103.6 million.",
        "Goodwill and total equity both fall by $43.2 million.",
    ],
}


def test_valid_rewrite_is_used_and_keeps_only_the_facts_it_shows():
    e, view = real(0)
    r = narrate(e, view, "company words", client=FakeClient(GOOD))
    assert r.used_llm and r.reason == "ok"
    assert r.explanation.method == "llm" and r.explanation.headline == GOOD["headline"]
    assert {f.shown for f in r.explanation.facts} == {
        "$43.2 million",
        "$60.4 million",
        "$103.6 million",
    }
    assert validate(r.explanation, view).ok
    assert r.usage == {"input_tokens": 1200, "output_tokens": 180}


def test_rewrite_with_a_wrong_number_is_rejected():
    e, view = real(0)
    bad = {**GOOD, "sentences": [*GOOD["sentences"][:2], "The loss widens to $106.3 million."]}
    r = narrate(e, view, client=FakeClient(bad))
    assert not r.used_llm and r.reason == "failed validation"
    assert r.explanation is e  # the template, untouched
    assert r.validation.problems[0].detail.startswith("'$106.3 million'")


def test_invented_figure_is_rejected():
    e, view = real(0)
    bad = {**GOOD, "sentences": [*GOOD["sentences"], "The shares fell 12% on the news."]}
    assert narrate(e, view, client=FakeClient(bad)).reason == "failed validation"


@pytest.mark.parametrize(
    "client,reason",
    [
        (FakeClient(GOOD, stop_reason="refusal"), "stop_reason=refusal"),
        (FakeClient(GOOD, stop_reason="max_tokens"), "stop_reason=max_tokens"),
        (FakeClient("not json"), "invalid JSON"),
        (FakeClient({"headline": "", "sentences": []}), "empty response"),
        (FakeClient(error=TimeoutError("slow")), "API error: TimeoutError"),
    ],
)
def test_any_failure_keeps_the_template(client, reason):
    e, view = real(0)
    r = narrate(e, view, client=client)
    assert (r.used_llm, r.reason, r.explanation) == (False, reason, e)


def test_no_key_means_no_call(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    e, view = real(0)
    assert narrate(e, view).reason == "no API key or anthropic package"


def test_request_shape():
    e, view = real(0)
    client = FakeClient(GOOD)
    narrate(e, view, "The impairment was understated.", client=client)
    (call,) = client.calls
    assert call["output_config"]["format"]["type"] == "json_schema"
    assert call["fallbacks"] == "default" and "server-side-fallback-2026-07-01" in call["betas"]
    prompt = call["messages"][0]["content"]
    assert "- $103.6 million  (net_income, restated, 2025-01-01 to 2025-09-27)" in prompt
    assert "<filing>\nThe impairment was understated.\n</filing>" in prompt


def test_prompt_for_a_case_without_numbers_forbids_amounts():
    from radar.explain.template import CaseFacts, explain

    e = explain(CaseFacts("Acme"), [], {}, [])
    assert "(none: write no amounts or percentages)" in build_prompt(e, "")


@pytest.mark.llm
@pytest.mark.skipif(not os.environ.get("ANTHROPIC_API_KEY"), reason="ANTHROPIC_API_KEY not set")
@pytest.mark.parametrize("i", range(len(REAL)), ids=[r[2] for r in REAL])
def test_real_api_rewrite_validates(i):
    e, view = real(i)
    words = parse_item402(load_document(F / REAL[i][0])).section_text
    r = narrate(e, view, words)
    print(f"\n[{r.reason}] {r.explanation.headline}\n{r.explanation.text}\n{r.usage}")
    assert r.used_llm, (r.reason, r.validation and r.validation.problems)
    assert validate(r.explanation, view).ok

from decimal import Decimal

import pytest

from radar.extract.ixbrl import choose, extract, fmt_decimal, normalize_flag

HEAD = b"""<?xml version="1.0" encoding="utf-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
 xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
 xmlns:iso4217="http://www.xbrl.org/2003/iso4217" xmlns:us-gaap="http://fasb.org/us-gaap/2025"
 xmlns:srt="http://fasb.org/srt/2025" xmlns:dei="http://xbrl.sec.gov/dei/2025"
 xmlns:abc="http://abc.example/2025"
 xmlns:ixt="http://www.xbrl.org/inlineXBRL/transformation/2020-02-12"><body>
<ix:header><ix:resources>
 <xbrli:unit id="usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
 <xbrli:context id="c"><xbrli:entity><xbrli:identifier scheme="x">1</xbrli:identifier>
  </xbrli:entity><xbrli:period><xbrli:instant>2025-12-31</xbrli:instant></xbrli:period></xbrli:context>
 <xbrli:context id="p"><xbrli:entity><xbrli:identifier scheme="x">1</xbrli:identifier><xbrli:segment>
  <xbrldi:explicitMember dimension="srt:RestatementAxis">srt:ScenarioPreviouslyReportedMember</xbrldi:explicitMember>
  </xbrli:segment></xbrli:entity><xbrli:period><xbrli:instant>2025-12-31</xbrli:instant></xbrli:period></xbrli:context>
 <xbrli:context id="a"><xbrli:entity><xbrli:identifier scheme="x">1</xbrli:identifier><xbrli:segment>
  <xbrldi:explicitMember dimension="srt:RestatementAxis">abc:RestatementAdjustmentMember</xbrldi:explicitMember>
  </xbrli:segment></xbrli:entity><xbrli:period><xbrli:instant>2025-12-31</xbrli:instant></xbrli:period></xbrli:context>
</ix:resources></ix:header>
"""


def doc(body: str) -> bytes:
    return HEAD + body.encode() + b"</body></html>"


def test_scale_sign_and_custom_adjustment_member():
    r = extract(
        doc("""
      <ix:nonFraction name="us-gaap:InventoryNet" contextRef="p" unitRef="usd" scale="3" decimals="-3">71,819</ix:nonFraction>
      <ix:nonFraction name="us-gaap:InventoryNet" contextRef="a" unitRef="usd" scale="3" decimals="-3" sign="-">2,028</ix:nonFraction>
      <ix:nonFraction name="us-gaap:InventoryNet" contextRef="c" unitRef="usd" scale="3" decimals="-3">69,791</ix:nonFraction>
      <ix:nonFraction name="us-gaap:Cash" contextRef="c" unitRef="usd" decimals="0">5</ix:nonFraction>
    """)
    )
    (row,) = r.rows  # Cash has no restatement columns, so it is not a restatement row
    assert row.previously_reported == Decimal(71_819_000)
    assert row.adjustment == Decimal(-2_028_000)
    assert row.restated == Decimal(69_791_000)
    assert row.unit == "USD"
    assert row.check_ok is True


def test_fixed_zero_and_derived_adjustment():
    r = extract(
        doc("""
      <ix:nonFraction name="us-gaap:Goodwill" contextRef="p" unitRef="usd" decimals="0">10</ix:nonFraction>
      <ix:nonFraction name="us-gaap:Goodwill" contextRef="c" unitRef="usd" decimals="0">7</ix:nonFraction>
      <ix:nonFraction name="us-gaap:Assets" contextRef="p" unitRef="usd" decimals="0">3</ix:nonFraction>
      <ix:nonFraction name="us-gaap:Assets" contextRef="a" unitRef="usd" format="ixt:fixed-zero">—</ix:nonFraction>
    """)
    )
    assets = r.find("us-gaap:Assets", "2025-12-31")
    goodwill = r.find("us-gaap:Goodwill", "2025-12-31")
    assert assets.adjustment == 0 and assets.restated == 3 and assets.restated_derived
    assert goodwill.adjustment == -3 and goodwill.adjustment_derived
    assert goodwill.check_ok is None  # derived values are not evidence of consistency


def test_identity_failure_is_flagged():
    r = extract(
        doc("""
      <ix:nonFraction name="us-gaap:Assets" contextRef="p" unitRef="usd" decimals="0">100</ix:nonFraction>
      <ix:nonFraction name="us-gaap:Assets" contextRef="a" unitRef="usd" decimals="0">100</ix:nonFraction>
      <ix:nonFraction name="us-gaap:Assets" contextRef="c" unitRef="usd" decimals="0">100</ix:nonFraction>
    """)
    )
    assert r.rows[0].check_ok is False


def test_narrative_rounding_does_not_override_table_value():
    r = extract(
        doc("""
      <ix:nonFraction name="us-gaap:Goodwill" contextRef="p" unitRef="usd" scale="3" decimals="-3">81,245</ix:nonFraction>
      <ix:nonFraction name="us-gaap:Goodwill" contextRef="c" unitRef="usd" scale="6" decimals="-5">124.4</ix:nonFraction>
      <ix:nonFraction name="us-gaap:Goodwill" contextRef="c" unitRef="usd" scale="3" decimals="-3">124,411</ix:nonFraction>
    """)
    )
    row = r.rows[0]
    assert row.restated == Decimal(124_411_000)
    assert row.conflicts == []


def test_choose_reports_real_conflicts():
    assert choose([(Decimal(5), 0), (Decimal(6), 0)])[2] is True
    assert choose([(Decimal(124_411_000), -3), (Decimal(124_400_000), -5)])[2] is False
    assert choose([(Decimal(124_411_000), -3), (Decimal(125_000_000), -5)])[2] is True


def test_cover_flags_from_checkbox_glyphs():
    r = extract(
        doc("""
      <ix:nonNumeric name="dei:DocumentFinStmtErrorCorrectionFlag" contextRef="c">☒</ix:nonNumeric>
      <ix:nonNumeric name="dei:DocumentFinStmtRestatementRecoveryAnalysisFlag" contextRef="c">☐</ix:nonNumeric>
      <ix:nonNumeric name="dei:DocumentType" contextRef="c">10-K/A</ix:nonNumeric>
    """)
    )
    assert r.cover == {
        "DocumentFinStmtErrorCorrectionFlag": "true",
        "DocumentFinStmtRestatementRecoveryAnalysisFlag": "false",
        "DocumentType": "10-K/A",
    }
    assert not r.has_restatement_dimension


@pytest.mark.parametrize(
    "raw,want", [("☒", "true"), ("☐", "false"), ("true", "true"), ("maybe", "maybe")]
)
def test_normalize_flag(raw, want):
    assert normalize_flag(raw) == want


def test_fmt_decimal():
    assert fmt_decimal(Decimal("5.2138E+7")) == "52138000"
    assert fmt_decimal(Decimal("-1.640")) == "-1.64"
    assert fmt_decimal(None) is None

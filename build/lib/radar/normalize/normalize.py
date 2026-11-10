"""Map XBRL concepts and HTML row labels onto canonical line items (design doc §7)."""

from __future__ import annotations

import re
import tomllib
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

CONFIG = Path(__file__).with_name("canonical.toml")


@dataclass(frozen=True)
class Item:
    key: str
    statement: str
    name: str
    concepts: tuple[str, ...]
    labels: tuple[re.Pattern, ...]

    def preference(self, concept_local: str) -> int:
        return self.concepts.index(concept_local)


@dataclass(frozen=True)
class Catalog:
    items: dict[str, Item]
    by_concept: dict[str, str]  # local name -> item key

    def item_for_concept(self, concept: str) -> Item | None:
        key = self.by_concept.get(concept.rsplit(":", 1)[-1])
        return self.items[key] if key else None

    def item_for_label(self, label: str, statement: str | None = None) -> Item | None:
        text = normalize_label(label)
        if not text:
            return None
        for item in self.items.values():
            if statement and item.statement != statement:
                continue
            if any(p.fullmatch(text) for p in item.labels):
                return item
        return None


def normalize_label(label: str) -> str:
    """Lower-case, strip footnote marks, invisible characters and trailing colons."""
    text = unicodedata.normalize("NFKC", label)
    text = "".join(ch for ch in text if unicodedata.category(ch)[0] != "C")
    text = text.replace("’", "'").replace("–", "-").replace("—", "-")
    text = re.sub(r"\(\s*[a-z0-9]\s*\)\s*$|\*+$|\[\d+\]$", "", text.strip(), flags=re.I)
    text = re.sub(r"\s+", " ", text).strip(" :.;,")
    return text.lower()


@lru_cache(maxsize=1)
def catalog() -> Catalog:
    raw = tomllib.loads(CONFIG.read_text())
    items: dict[str, Item] = {}
    by_concept: dict[str, str] = {}
    for key, spec in raw.items():
        items[key] = Item(
            key=key,
            statement=spec["statement"],
            name=spec["name"],
            concepts=tuple(spec.get("concepts", [])),
            labels=tuple(re.compile(p, re.I) for p in spec.get("labels", [])),
        )
        for c in spec.get("concepts", []):
            if c in by_concept:
                raise ValueError(f"{c} mapped to both {by_concept[c]} and {key}")
            by_concept[c] = key
    return Catalog(items, by_concept)

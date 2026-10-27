"""Redacted, human-reviewable parser corpus.

A corpus file is JSON Lines. It must never contain private channel content that
has not been redacted. Each line carries the source text (already redacted),
the expected classification, and the expected fields that are material to the
test. The loader validates the file shape; the checker compares parser output
without printing the source text.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .model import ParsedCandidate
from .parser import SignalAnalysisParser


class CorpusError(Exception):
    """The corpus file is missing, malformed, or unsafe to use."""


@dataclass(frozen=True)
class CorpusExample:
    example_id: str
    language: str
    text: str
    expected_classification: str
    expected: dict[str, Any]
    human_reviewed: bool
    source: str


@dataclass
class CorpusCheck:
    total: int = 0
    matched: int = 0
    mismatched: int = 0
    human_reviewed: int = 0
    mismatches: list[dict[str, Any]] | None = None

    def as_json(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "matched": self.matched,
            "mismatched": self.mismatched,
            "human_reviewed": self.human_reviewed,
            "mismatches": self.mismatches or [],
        }


def load_corpus(path: str | Path) -> list[CorpusExample]:
    corpus_path = Path(path)
    if not corpus_path.is_file():
        raise CorpusError(f"corpus file not found: {corpus_path}")
    examples: list[CorpusExample] = []
    seen: set[str] = set()
    for line_number, raw in enumerate(corpus_path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        try:
            item = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CorpusError(f"line {line_number}: invalid JSON") from exc
        if not isinstance(item, dict):
            raise CorpusError(f"line {line_number}: expected a JSON object")
        example_id = str(item.get("id") or "").strip()
        if not example_id:
            raise CorpusError(f"line {line_number}: id is required")
        if example_id in seen:
            raise CorpusError(f"line {line_number}: duplicate id {example_id!r}")
        seen.add(example_id)
        text = item.get("text")
        if not isinstance(text, str) or not text.strip():
            raise CorpusError(f"line {line_number}: non-empty redacted text is required")
        classification = str(item.get("expected_classification") or "").strip()
        if not classification:
            raise CorpusError(f"line {line_number}: expected_classification is required")
        expected = item.get("expected") or {}
        if not isinstance(expected, dict):
            raise CorpusError(f"line {line_number}: expected must be an object")
        examples.append(
            CorpusExample(
                example_id=example_id,
                language=str(item.get("language") or "unknown"),
                text=text,
                expected_classification=classification,
                expected=expected,
                human_reviewed=bool(item.get("human_reviewed", False)),
                source=str(item.get("source") or "unspecified"),
            )
        )
    if not examples:
        raise CorpusError("corpus contains no examples")
    return examples


def check_corpus(
    examples: list[CorpusExample], *, parser: SignalAnalysisParser | None = None
) -> CorpusCheck:
    parser = parser or SignalAnalysisParser()
    mismatches: list[dict[str, Any]] = []
    report = CorpusCheck(mismatches=mismatches)
    for example in examples:
        report.total += 1
        report.human_reviewed += int(example.human_reviewed)
        parsed = parser.parse(example.text)
        mismatch = _compare(parsed, example)
        if mismatch:
            report.mismatched += 1
            mismatches.append(
                {"id": example.example_id, "field": mismatch, "language": example.language}
            )
        else:
            report.matched += 1
    return report


def _compare(parsed: ParsedCandidate, example: CorpusExample) -> str | None:
    if str(parsed.classification) != example.expected_classification:
        return "classification"
    expected = example.expected
    for field, raw in expected.items():
        if raw is None:
            if getattr(parsed, field, None) is not None:
                return field
            continue
        if field == "targets":
            actual_targets = [format(t.price, "f") for t in parsed.targets]
            if actual_targets != [format(Decimal(str(v)), "f") for v in raw]:
                return field
            continue
        actual_value = getattr(parsed, field, None)
        if actual_value is None:
            return field
        try:
            if isinstance(actual_value, Decimal):
                if actual_value != Decimal(str(raw)):
                    return field
            elif str(actual_value) != str(raw):
                return field
        except (InvalidOperation, ValueError):
            return field
    return None

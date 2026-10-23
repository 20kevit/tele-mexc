"""Redacted corpus loader and checker tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tg_signal_ingest.analysis import (
    CorpusError,
    CorpusExample,
    check_corpus,
    load_corpus,
)

FIXTURE = Path(__file__).parent / "fixtures" / "redacted_signal_corpus.jsonl"


def _write_jsonl(path: Path, items: list[object]) -> Path:
    path.write_text(
        "\n".join(json.dumps(item, ensure_ascii=False) for item in items) + "\n",
        encoding="utf-8",
    )
    return path


def _minimal_text(item_id: str = "x", text: str = "BUY BTCUSDT ENTRY 100 TP 120 SL 95"):
    return {
        "id": item_id,
        "language": "en",
        "text": text,
        "expected_classification": "new_signal",
        "expected": {},
    }


class TestCorpusLoader:
    def test_redacted_fixture_covers_every_classification(self):
        examples = load_corpus(FIXTURE)
        assert examples
        assert all(example.source == "synthetic_redacted" for example in examples)
        assert all(example.human_reviewed is False for example in examples)
        assert {example.language for example in examples} == {"en", "fa"}
        classifications = {example.expected_classification for example in examples}
        assert {
            "new_signal",
            "modification",
            "cancellation",
            "target_update",
            "stop_loss_update",
            "result",
            "non_signal",
            "ambiguous",
        } <= classifications

    def test_fixture_matches_current_parser(self):
        report = check_corpus(load_corpus(FIXTURE))
        assert report.total == 26
        assert report.matched == report.total
        assert report.mismatched == 0
        assert report.mismatches == []

    def test_loader_skips_comments_and_blank_lines(self, tmp_path):
        path = tmp_path / "corpus.jsonl"
        path.write_text(
            "# comment\n\n" + json.dumps(_minimal_text()) + "\n",
            encoding="utf-8",
        )
        examples = load_corpus(path)
        assert len(examples) == 1

    @pytest.mark.parametrize(
        "raw",
        [
            "{not json}\n",
            json.dumps([1, 2]) + "\n",
            json.dumps({"id": "", "text": "x", "expected_classification": "none"}) + "\n",
            json.dumps({"id": "x", "text": "  ", "expected_classification": "none"}) + "\n",
            json.dumps({"id": "x", "text": "hi"}) + "\n",
            json.dumps({"id": "x", "text": "hi", "expected_classification": "none",
                        "expected": "wrong"}) + "\n",
        ],
    )
    def test_malformed_lines_are_rejected(self, tmp_path, raw):
        path = tmp_path / "corpus.jsonl"
        path.write_text(raw, encoding="utf-8")
        with pytest.raises(CorpusError):
            load_corpus(path)

    def test_duplicate_ids_are_rejected(self, tmp_path):
        path = _write_jsonl(tmp_path / "corpus.jsonl", [_minimal_text(), _minimal_text()])
        with pytest.raises(CorpusError):
            load_corpus(path)

    def test_empty_corpus_is_rejected(self, tmp_path):
        path = tmp_path / "corpus.jsonl"
        path.write_text("# only a comment\n", encoding="utf-8")
        with pytest.raises(CorpusError):
            load_corpus(path)

    def test_missing_file_is_rejected(self, tmp_path):
        with pytest.raises(CorpusError):
            load_corpus(tmp_path / "absent.jsonl")


class TestCorpusChecker:
    def test_mismatch_report_does_not_repeat_source_text(self):
        example = CorpusExample(
            example_id="mismatch-1",
            language="en",
            text="THIS PRIVATE SOURCE TEXT SHOULD NOT APPEAR",
            expected_classification="new_signal",
            expected={},
            human_reviewed=False,
            source="synthetic_redacted",
        )
        report = check_corpus([example])
        payload = report.as_json()
        assert report.mismatched == 1
        assert payload["mismatches"] == [
            {"id": "mismatch-1", "field": "classification", "language": "en"}
        ]
        assert "PRIVATE" not in json.dumps(payload)

    def test_expected_field_mismatch_is_reported(self):
        example = CorpusExample(
            example_id="field-mismatch",
            language="en",
            text="BUY BTCUSDT ENTRY 100 TP 120 SL 95",
            expected_classification="new_signal",
            expected={"symbol": "ETHUSDT"},
            human_reviewed=False,
            source="synthetic_redacted",
        )
        report = check_corpus([example])
        assert report.mismatches == [
            {"id": "field-mismatch", "field": "symbol", "language": "en"}
        ]

    def test_none_means_the_parser_must_leave_it_missing(self):
        example = CorpusExample(
            example_id="missing-ok",
            language="en",
            text="BUY BTCUSDT ENTRY 100 TP 120 SL 95",
            expected_classification="new_signal",
            expected={"stop_loss": None},
            human_reviewed=False,
            source="synthetic_redacted",
        )
        report = check_corpus([example])
        assert report.mismatches[0]["field"] == "stop_loss"

    def test_human_reviewed_count_is_reported(self):
        example = CorpusExample(
            example_id="reviewed",
            language="en",
            text="CANCEL ALL",
            expected_classification="cancellation",
            expected={},
            human_reviewed=True,
            source="operator_reviewed",
        )
        report = check_corpus([example])
        assert report.human_reviewed == 1
        assert report.matched == 1

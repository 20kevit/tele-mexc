"""Structural guarantees for the durable decision registry.

The registry is documentation, but it is also the contract this repository is
implemented against.  These tests keep it machine-readable, complete and honest:
a decision may only claim `fixed`/`tested` when it names a commit and tests.
"""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "docs" / "decisions" / "registry.json"
SUMMARY = ROOT / "docs" / "DECISION_REGISTRY.md"

EXPECTED_IDS = {
    "N-01",
    "N-02",
    "N-03",
    "N-04",
    "N-05",
    "N-06",
    "N-07",
    "N-08",
    "N-09",
    "N-10",
    "F-01",
    "F-02",
    "F-04",
    "F-05",
    "F-06",
    "F-07",
    "F-09",
    "F-10",
    "F-12",
    "F-13",
}

REQUIRED_FIELDS = {
    "id",
    "title",
    "historical_evidence",
    "evidence_class",
    "decision",
    "required_behavior",
    "acceptance_criteria",
    "tests_required",
    "status",
    "commit",
    "limitations",
}


def _load() -> dict:
    return json.loads(REGISTRY.read_text(encoding="utf-8"))


def test_registry_exists_and_is_valid_json() -> None:
    assert REGISTRY.is_file()
    data = _load()
    assert data["schema_version"] == "1.0"
    assert data["baseline"]["commit"] == "d987c7febb90065cb148f643fea97c054e325fee"


def test_registry_covers_every_decision_exactly_once() -> None:
    data = _load()
    ids = [item["id"] for item in data["decisions"]]
    assert len(ids) == len(set(ids)), "duplicate decision id"
    assert set(ids) == EXPECTED_IDS


def test_every_decision_has_required_fields_and_valid_vocabulary() -> None:
    data = _load()
    statuses = set(data["status_values"])
    evidence = set(data["evidence_classes"])
    for item in data["decisions"]:
        assert set(item) >= REQUIRED_FIELDS, f"{item['id']} missing fields"
        assert item["status"] in statuses, f"{item['id']} invalid status"
        assert item["evidence_class"] in evidence, f"{item['id']} invalid evidence class"
        assert item["required_behavior"], f"{item['id']} has no required behaviour"
        assert item["acceptance_criteria"], f"{item['id']} has no acceptance criteria"


def test_fixed_and_tested_items_name_a_commit_and_tests() -> None:
    data = _load()
    for item in data["decisions"]:
        if item["status"] in {"fixed", "tested"}:
            assert item["commit"], f"{item['id']} claims {item['status']} without a commit"
            assert item["tests_required"], f"{item['id']} claims {item['status']} without tests"


def test_registry_records_older_build_provenance() -> None:
    data = _load()
    note = data["baseline"]["note"].lower()
    assert "older build" in note and "not" in note


def test_summary_document_lists_every_decision() -> None:
    text = SUMMARY.read_text(encoding="utf-8")
    for decision_id in EXPECTED_IDS:
        assert f"| {decision_id} |" in text, f"{decision_id} missing from summary table"

"""
Module to run dataset expectations for configuration files. this ensure data quality before upload to s3
"""

import json
import csv
import os
from pathlib import Path
from glob import glob

import pytest

from digital_land.expectations.checkpoints.csv import CsvCheckpoint

REPO_ROOT = Path(__file__).resolve().parents[2]
SEARCH_DIRS = ["pipeline", "collection"]

def _collect_files(filename):
    files = []
    for search_dir in SEARCH_DIRS:
        files.extend(glob(str(REPO_ROOT / search_dir / "*" / filename)))
    return sorted(files)


def _test_id(file_path):
    path = Path(file_path)
    return f"{path.parts[-3]}/{path.parts[-2]}"


def _format_line_reference(file_path, line_number):
    path = Path(file_path).resolve()
    try:
        relative_path = path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return f"{file_path}:{line_number}"

    repository = os.getenv("GITHUB_REPOSITORY")
    server_url = os.getenv("GITHUB_SERVER_URL", "https://github.com")
    branch = os.getenv("GITHUB_HEAD_REF") or os.getenv("GITHUB_REF_NAME")

    if repository and branch:
        return f"{server_url}/{repository}/blob/{branch}/{relative_path}#L{line_number}"

    return f"{relative_path}:{line_number}"


def _run_checkpoint(dataset, file_path, rules):
    try:
        checkpoint = CsvCheckpoint(dataset=dataset, file_path=file_path)
    except Exception as e:
        pytest.fail(f"Failed to initiate checkpoint for CSV '{file_path}': {e}")

    checkpoint.load(rules)
    checkpoint.run()

    failed = [entry for entry in checkpoint.log.entries if not entry["passed"]]
    if failed:
        messages = []
        for entry in failed:
            messages.append(f"  - {entry['name']}: {entry['message']}")
            details = entry.get("details")
            if details:
                if isinstance(details, str):
                    details = json.loads(details)
                messages.append(f"    {json.dumps(details, indent=4)}")
        assert False, "\n".join(messages)

# TEST OLD_ENTITY.CSV
OLD_ENTITY_RULES = [
    {
        "name": "old-entity values are unique",
        "operation": "check_unique",
        "parameters": {"field": "old-entity"},
        "severity": "error",
    },
    {
        "name": "old-entity and entity have no shared values",
        "operation": "check_no_shared_values",
        "parameters": {"field_1": "old-entity", "field_2": "entity"},
        "severity": "error",
    },
]

old_entity_files = _collect_files("old-entity.csv")

@pytest.mark.parametrize(
    "file_path",
    old_entity_files,
    ids=[_test_id(f) for f in old_entity_files],
)
def test_old_entity(file_path):
    _run_checkpoint(dataset="old-entity", file_path=file_path, rules=OLD_ENTITY_RULES)


@pytest.mark.parametrize(
    "file_path",
    old_entity_files,
    ids=[_test_id(f) for f in old_entity_files],
)
def test_old_entity_status_is_only_301_or_410(file_path):
    allowed_statuses = {"301", "410"}
    invalid_statuses = []

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for line_number, row in enumerate(reader, start=2):
            if not any((value or "").strip() for value in row.values()):
                continue

            status = (row.get("status") or "").strip()
            if status not in allowed_statuses:
                invalid_statuses.append((line_number, status))

    invalid_values = sorted({status for _, status in invalid_statuses})
    invalid_lines = [line_number for line_number, _ in invalid_statuses]
    invalid_refs = [_format_line_reference(file_path, line_number) for line_number in invalid_lines]

    assert not invalid_statuses, (
        f"Invalid status values in {file_path}: {invalid_values}. "
        f"References: {invalid_refs[:50]}"
        + ("..." if len(invalid_refs) > 50 else "")
        + ". "
        f"Line numbers: {invalid_lines[:50]}"
        + ("..." if len(invalid_lines) > 50 else "")
        + ". "
        "Expected only 301 or 410."
    )


# TEST ENTITY-ORGANISATION.CSV
ENTITY_ORGANISATION_RULES = [
    {
        "name": "entity-minimum and entity-maximum ranges do not overlap",
        "operation": "check_no_overlapping_ranges",
        "parameters": {"min_field": "entity-minimum", "max_field": "entity-maximum"},
        "severity": "error",
    },
]

entity_organisation_files = _collect_files("entity-organisation.csv")


@pytest.mark.parametrize(
    "file_path",
    entity_organisation_files,
    ids=[_test_id(f) for f in entity_organisation_files],
)
def test_entity_organisation(file_path, tmp_path):
    normalised = tmp_path / Path(file_path).name
    normalised.write_bytes(Path(file_path).read_bytes().replace(b'\r\n', b'\n').replace(b',\n', b'\n'))
    _run_checkpoint(
        dataset="entity-organisation", file_path=str(normalised), rules=ENTITY_ORGANISATION_RULES
    )
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

def _collect_files(pattern, search_dirs=None):
    search_dirs = search_dirs or SEARCH_DIRS
    files = []
    for search_dir in search_dirs:
        files.extend(glob(str(REPO_ROOT / search_dir / "*" / pattern)))
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


def _check_old_entity_shared_values_with_status(file_path):
    """
    Check that old-entity and entity have no conflicting shared values.
    Allows shared values only when BOTH conditions are true:
    - ID appears in entity column with status "301" (redirect target)
    - ID appears in old-entity column with status "410" (being retired)
    Fails on any other shared values.
    """
    all_rows = []
    old_entities_dict = {}  # {id: [line_numbers]}
    entity_targets_dict = {}  # {id: [line_numbers]}

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for line_number, row in enumerate(reader, start=2):
            if not any((value or "").strip() for value in row.values()):
                continue

            old_entity_id = (row.get("old-entity") or "").strip()
            entity_id = (row.get("entity") or "").strip()
            status = (row.get("status") or "").strip()

            all_rows.append({
                "line": line_number,
                "old_entity": old_entity_id,
                "entity": entity_id,
                "status": status,
            })

            if old_entity_id:
                old_entities_dict.setdefault(old_entity_id, []).append(line_number)
            if entity_id:
                entity_targets_dict.setdefault(entity_id, []).append(line_number)

    # Find shared values and check if they're allowed
    shared_values = set(old_entities_dict.keys()) & set(entity_targets_dict.keys())
    conflicts = []

    for shared_id in shared_values:
        # Both conditions must be true to allow the shared value
        has_entity_301 = any(row["entity"] == shared_id and row["status"] == "301" for row in all_rows)
        has_old_entity_410 = any(row["old_entity"] == shared_id and row["status"] == "410" for row in all_rows)

        is_allowed = has_entity_301 and has_old_entity_410

        if not is_allowed:
            conflicts.append({
                "shared_id": shared_id,
                "old_entity_rows": old_entities_dict[shared_id],
                "entity_target_rows": entity_targets_dict[shared_id],
            })

    if conflicts:
        details = []
        for conflict in conflicts:
            details.append(
                f"  - Entity '{conflict['shared_id']}' appears in both columns "
                f"(old-entity row {conflict['old_entity_rows']}, "
                f"entity row {conflict['entity_target_rows']})"
            )
        pytest.fail(
            f"Conflicting shared entities across 'old-entity' and 'entity' columns in {file_path}:\n" + "\n".join(details)
        )

# TEST OLD_ENTITY.CSV
OLD_ENTITY_RULES = [
    {
        "name": "old-entity values are unique",
        "operation": "check_unique",
        "parameters": {"field": "old-entity"},
        "severity": "error",
    },
]

old_entity_files = _collect_files("old-entity.csv")
all_config_csv_files = _collect_files("*.csv")
pipeline_csv_files = _collect_files("*.csv", search_dirs=["pipeline"])

@pytest.mark.parametrize(
    "file_path",
    old_entity_files,
    ids=[_test_id(f) for f in old_entity_files],
)
def test_old_entity(file_path):
    _run_checkpoint(dataset="old-entity", file_path=file_path, rules=OLD_ENTITY_RULES)
    _check_old_entity_shared_values_with_status(file_path)

@pytest.mark.parametrize(
    "file_path",
    all_config_csv_files,
    ids=[_test_id(f) for f in all_config_csv_files],
)
def test_pipeline_csv_row_length_matches_header(file_path):
    mismatched_rows = []

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)

        if header is None:
            pytest.fail(f"CSV file is empty: {file_path}")

        expected_columns = len(header)
        entity_index = header.index("entity") if "entity" in header else None
        for line_number, row in enumerate(reader, start=2):
            if not any((cell or "").strip() for cell in row):
                continue

            if len(row) != expected_columns:
                if entity_index is not None:
                    entity = row[entity_index].strip() if entity_index < len(row) else ""
                    mismatched_rows.append((line_number, len(row), entity))
                else:
                    mismatched_rows.append((line_number, len(row)))

    mismatch_refs = [
        _format_line_reference(file_path, line_number)
        for line_number, *_ in mismatched_rows[:50]
    ]

    assert not mismatched_rows, (
        f"Row length mismatch in {file_path}. Header has {expected_columns} columns; "
        f"mismatched rows (line, columns{', entity' if entity_index is not None else ''}): {mismatched_rows[:50]}"
        + ". "
        + f"References: {mismatch_refs}"
        + ("..." if len(mismatched_rows) > 50 else "")
    )

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
                entity = (row.get("old-entity") or "").strip()
                invalid_statuses.append((line_number, status, entity))

    invalid_values = sorted({status for _, status, _ in invalid_statuses})
    invalid_entities = [entity for _, _, entity in invalid_statuses]
    invalid_lines = [line_number for line_number, _, _ in invalid_statuses]

    assert not invalid_statuses, (
        f"Invalid status values in {file_path}: {invalid_values}. "
        f"Old Entity numbers: {invalid_entities[:50]}"
        + ("..." if len(invalid_entities) > 50 else "")
        + ". "
        f"Line numbers in file: {invalid_lines[:50]}"
        + ("..." if len(invalid_lines) > 50 else "")
        + ". "
        "Expected only 301 or 410."
    )


@pytest.mark.parametrize(
    "file_path",
    pipeline_csv_files,
    ids=[_test_id(f) for f in pipeline_csv_files],
)
def test_pipeline_csv_has_no_blank_rows(file_path):
    blank_rows = []
    last_non_empty_line = 0

    def _is_blank_row(row):
        return not row or all(not (cell or "").strip() for cell in row)

    def _is_truly_empty_line(row):
        return not row or (len(row) == 1 and not (row[0] or "").strip())

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for line_number, row in enumerate(reader, start=1):
            if _is_blank_row(row):
                blank_rows.append((line_number, row))
            if not _is_truly_empty_line(row):
                last_non_empty_line = line_number

    # Ignore truly empty lines at the end of the file. Rows with commas are not ignored.
    blank_line_numbers = [
        line_number
        for line_number, row in blank_rows
        if line_number <= last_non_empty_line or not _is_truly_empty_line(row)
    ]

    blank_refs = [
        _format_line_reference(file_path, line_number)
        for line_number in blank_line_numbers[:50]
    ]

    assert not blank_line_numbers, (
        f"Blank rows found in {file_path}. References: {blank_refs}. "
        + f"Line numbers: {blank_line_numbers[:50]}."
        + ("..." if len(blank_line_numbers) > 50 else "")
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
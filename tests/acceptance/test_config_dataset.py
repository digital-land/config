"""
Module to run dataset expectations for configuration files. this ensure data quality before upload to s3
"""

import json
import csv
import io
import os
import re
import urllib.request
from pathlib import Path
from glob import glob

import pytest

from digital_land.expectations.checkpoints.csv import CsvCheckpoint
from tests.acceptance.datatype_validators import (
    _is_valid_address_value,
    _is_valid_curie_list_value,
    _is_valid_curie_value,
    _is_valid_datetime_value,
    _is_valid_decimal_value,
    _is_valid_flag_value,
    _is_valid_hash_value,
    _is_valid_integer_value,
    _is_valid_json_value,
    _is_valid_latitude_value,
    _is_valid_longitude_value,
    _is_valid_multipolygon_value,
    _is_valid_pattern_value,
    _is_valid_point_value,
    _is_valid_reference_value,
    _is_valid_url_value,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SEARCH_DIRS = ["pipeline", "collection"]
SPECIFICATION_DATASET_URL = "https://raw.githubusercontent.com/digital-land/specification/refs/heads/main/specification/dataset.csv"
SPECIFICATION_FIELD_URL = "https://raw.githubusercontent.com/digital-land/specification/refs/heads/main/specification/field.csv"
_FIELD_TO_DATATYPE_MAP = None

# Overrides for fields where field.csv uses a broad datatype (e.g. string),
# but validation should enforce a more specific format.
FIELD_DATATYPE_OVERRIDES = {
    "organisations": "curie-list",
}

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


def _load_specification_csv(url):
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            content = response.read().decode("utf-8")
    except Exception as exc:
        pytest.fail(f"Could not load specification CSV from {url}: {exc}")

    return list(csv.DictReader(io.StringIO(content)))


def _field_to_datatype():
    global _FIELD_TO_DATATYPE_MAP
    if _FIELD_TO_DATATYPE_MAP is not None:
        return _FIELD_TO_DATATYPE_MAP

    field_rows = _load_specification_csv(SPECIFICATION_FIELD_URL)

    field_to_datatype = {}
    for row in field_rows:
        field_name = (row.get("field") or "").strip()
        datatype = (row.get("datatype") or "").strip()
        if field_name and datatype:
            field_to_datatype[field_name] = datatype

    _FIELD_TO_DATATYPE_MAP = field_to_datatype
    return _FIELD_TO_DATATYPE_MAP


def _ranges_for_collection_from_specification(collection_name):
    try:
        with urllib.request.urlopen(SPECIFICATION_DATASET_URL, timeout=30) as response:
            content = response.read().decode("utf-8")
    except Exception as exc:
        pytest.fail(f"Could not load specification dataset from {SPECIFICATION_DATASET_URL}: {exc}")

    ranges = []
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        if not isinstance(row, dict):
            continue

        if (row.get("collection") or "").strip() != collection_name:
            continue

        try:
            min_val = int((row.get("entity-minimum") or "").strip())
            max_val = int((row.get("entity-maximum") or "").strip())
        except (TypeError, ValueError):
            continue

        dataset_name = (row.get("dataset") or "").strip()
        ranges.append((dataset_name, min_val, max_val))

    return ranges


OLD_ENTITY_IGNORED_ORGANISATIONS = {
    "conservation-area": {
        "government-organisation:D1342",
        "government-organisation:PB1164",
    }
}


def _lookup_entity_to_organisation(collection_name):
    lookup_path = REPO_ROOT / "pipeline" / collection_name / "lookup.csv"
    if not lookup_path.exists():
        return {}

    entity_to_org = {}
    with open(lookup_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entity = (row.get("entity") or "").strip()
            organisation = (row.get("organisation") or "").strip()
            if not entity:
                continue
            entity_to_org[entity] = organisation

    return entity_to_org

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
all_config_csv_files = _collect_files("*.csv")
pipeline_csv_files = _collect_files("*.csv", search_dirs=["pipeline"])

@pytest.mark.parametrize(
    "file_path",
    old_entity_files,
    ids=[_test_id(f) for f in old_entity_files],
)
def test_old_entity(file_path):
    _run_checkpoint(dataset="old-entity", file_path=file_path, rules=OLD_ENTITY_RULES)

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
    all_config_csv_files,
    ids=[_test_id(f) for f in all_config_csv_files],
)
def test_columns_have_the_correct_values_based_on_datatype(file_path):
    field_to_datatype_map = _field_to_datatype()
    validators = {
        "address": _is_valid_address_value,
        "curie-list": _is_valid_curie_list_value,
        "curie": _is_valid_curie_value,
        "date": _is_valid_datetime_value,
        "datetime": _is_valid_datetime_value,
        "decimal": _is_valid_decimal_value,
        "flag": _is_valid_flag_value,
        "hash": _is_valid_hash_value,
        "integer": _is_valid_integer_value,
        "json": _is_valid_json_value,
        "latitude": _is_valid_latitude_value,
        "longitude": _is_valid_longitude_value,
        "multipolygon": _is_valid_multipolygon_value,
        "pattern": _is_valid_pattern_value,
        "point": _is_valid_point_value,
        "reference": _is_valid_reference_value,
        "url": _is_valid_url_value,
    }

    invalid_values = []

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            pytest.fail(f"CSV file is empty or missing a header: {file_path}")

        applicable_fields = []
        for field in reader.fieldnames:
            datatype = FIELD_DATATYPE_OVERRIDES.get(field, field_to_datatype_map.get(field))
            if datatype and datatype in validators:
                applicable_fields.append((field, datatype, validators[datatype]))

        if not applicable_fields:
            return

        for line_number, row in enumerate(reader, start=2):
            for field, datatype, validator in applicable_fields:
                value = (row.get(field) or "").strip()
                if not value:
                    continue

                if not validator(value):
                    invalid_values.append(
                        {
                            "line_number": line_number,
                            "field": field,
                            "datatype": datatype,
                            "value": value,
                            "file_ref": _format_line_reference(file_path, line_number),
                        }
                    )

    if invalid_values:
        invalid_lines = []
        for entry in invalid_values[:50]:
            invalid_lines.append(
                f"  - line {entry['line_number']} ({entry['file_ref']}): "
                f"{entry['field']}={entry['value']} (datatype={entry['datatype']})"
            )

        unique_invalid_values = sorted({entry["value"] for entry in invalid_values})
        checked_fields_summary = [f"{field} ({datatype})" for field, datatype, _ in applicable_fields]
        message = (
            f"Invalid datatype values found in {file_path}.\n"
            + f"Checked fields by datatype: {checked_fields_summary}.\n"
            + "Supported datatype checks: address, date, datetime, integer, decimal, flag, json, reference, curie, curie-list, url, hash, pattern, latitude, longitude, point, multipolygon.\n"
            + f"Summary: {len(invalid_values)} invalid value(s), {len(unique_invalid_values)} unique invalid value(s).\n"
            + "Invalid rows:\n"
            + "\n".join(invalid_lines)
            + ("\n  - ..." if len(invalid_values) > 50 else "")
            + f"\nInvalid values: {unique_invalid_values[:50]}"
            + ("..." if len(unique_invalid_values) > 50 else "")
        )
        pytest.fail(message)


@pytest.mark.parametrize(
    "old_entity_file",
    old_entity_files,
    ids=[_test_id(f) for f in old_entity_files],
)
def test_old_entity_entities_are_within_specification_entity_ranges(old_entity_file):
    collection_name = Path(old_entity_file).parent.name
    ranges = _ranges_for_collection_from_specification(collection_name)
    entity_to_org = _lookup_entity_to_organisation(collection_name)
    ignored_organisations = OLD_ENTITY_IGNORED_ORGANISATIONS.get(collection_name, set())

    if not ranges:
        pytest.skip(
            f"No entity ranges found for collection '{collection_name}' in specification/dataset.csv"
        )

    out_of_range = []

    def _parse_int(value):
        value = (value or "").strip()
        if not value:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def _in_any_range(entity_value):
        return any(min_val <= entity_value <= max_val for _, min_val, max_val in ranges)

    with open(old_entity_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for line_number, row in enumerate(reader, start=2):
            entity_value = _parse_int(row.get("entity"))
            old_entity_value = _parse_int(row.get("old-entity"))

            present_values = []
            if entity_value is not None:
                present_values.append(("entity", entity_value))
            if old_entity_value is not None:
                present_values.append(("old-entity", old_entity_value))

            if not present_values:
                continue

            checks = []
            for name, value in present_values:
                organisation = entity_to_org.get(str(value), "").strip()                
                if not organisation:
                    continue
                if organisation in ignored_organisations:
                    continue
                
                checks.append((name, value, organisation, _in_any_range(value)))

            if not checks:
                continue

            # If both columns are present, both must be in range.
            # If only one is present, that one must be in range.
            row_passes = all(in_range for _, _, _, in_range in checks)

            if not row_passes:
                out_of_range.append((line_number, checks))

    invalid_values = sorted(
        {
            value
            for _, checks in out_of_range
            for _, value, _, in_range in checks
            if not in_range
        }
    )

    if out_of_range:
        invalid_details = []
        for line_number, checks in out_of_range[:50]:
            ref = _format_line_reference(old_entity_file, line_number)
            failed_checks = [
                f"{field}={value}, organisation={organisation}"
                for field, value, organisation, in_range in checks
                if not in_range
            ]
            invalid_details.append(f"  - line {line_number} ({ref}): {', '.join(failed_checks)}")

        range_lines = [
            f"  - {dataset_name or '(no dataset)'}: {min_val}-{max_val}"
            for dataset_name, min_val, max_val in ranges[:50]
        ]

        message = (
            f"Entity values in {old_entity_file} are outside specification ranges for collection '{collection_name}'.\n"
            + "Allowed ranges:\n"
            + "\n".join(range_lines)
            + ("\n  - ..." if len(ranges) > 50 else "")
            + "\nInvalid rows:\n"
            + "\n".join(invalid_details)
            + ("\n  - ..." if len(out_of_range) > 50 else "")
            + f"\nInvalid values: {invalid_values[:50]}"
            + ("..." if len(invalid_values) > 50 else "")
        )
        pytest.fail(message)


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
lookup_files = _collect_files("lookup.csv")


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



@pytest.mark.parametrize(
    "lookup_file",
    lookup_files,
    ids=[_test_id(f) for f in lookup_files],
)
def test_lookup_entities_within_organisation_ranges(lookup_file):
    collection_name = Path(lookup_file).parent.name
    ignored_organisations = OLD_ENTITY_IGNORED_ORGANISATIONS.get(collection_name, set())

    lookup_dir = Path(lookup_file).parent
    entity_org_file = lookup_dir / "entity-organisation.csv"

    if not entity_org_file.exists():
        pytest.skip(f"No entity-organisation.csv found for {_test_id(lookup_file)}")

    ranges = []
    with open(entity_org_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                min_val = int((row.get("entity-minimum") or "").strip())
                max_val = int((row.get("entity-maximum") or "").strip())
                organisation = (row.get("organisation") or "").strip()
                ranges.append((min_val, max_val, organisation))
            except ValueError:
                continue

    if not ranges:
        pytest.skip(f"No valid ranges found in {entity_org_file}")

    out_of_range_entities = []
    with open(lookup_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for line_number, row in enumerate(reader, start=2):
            organisation = (row.get("organisation") or "").strip()
            if not organisation:
                continue
            if organisation in ignored_organisations:
                continue

            entity_str = (row.get("entity") or "").strip()
            if not entity_str:
                continue

            try:
                entity = int(entity_str)
            except ValueError:
                continue

            if not any(min_val <= entity <= max_val for min_val, max_val, _ in ranges):
                out_of_range_entities.append(
                    {
                        "line_number": line_number,
                        "entity": entity,
                        "organisation": organisation,
                        "reference": (row.get("reference") or "").strip(),
                        "file_ref": _format_line_reference(lookup_file, line_number),
                    }
                )

    unique_entities = sorted({entry["entity"] for entry in out_of_range_entities})

    if out_of_range_entities:
        invalid_rows_lines = []
        for entry in out_of_range_entities[:50]:
            invalid_rows_lines.append(
                "  - line "
                + str(entry["line_number"])
                + f" ({entry['file_ref']}): entity={entry['entity']}"
                + (
                    f", organisation={entry['organisation']}"
                    if entry["organisation"]
                    else ""
                )
                + (f", reference={entry['reference']}" if entry["reference"] else "")
            )

        range_lines = [
            f"  - {min_val}-{max_val}" + (f" (organisation={organisation})" if organisation else "")
            for min_val, max_val, organisation in ranges[:50]
        ]

        message = (
            f"Entities in {lookup_file} are outside ranges in {entity_org_file} for {_test_id(lookup_file)}.\n"
            + f"Summary: {len(out_of_range_entities)} invalid row(s), {len(unique_entities)} unique invalid value(s).\n"
            + "Allowed ranges:\n"
            + "\n".join(range_lines)
            + ("\n  - ..." if len(ranges) > 50 else "")
            + "\nInvalid rows:\n"
            + "\n".join(invalid_rows_lines)
            + ("\n  - ..." if len(out_of_range_entities) > 50 else "")
            + f"\nInvalid entity values: {unique_entities[:50]}"
            + ("..." if len(unique_entities) > 50 else "")
        )
        pytest.fail(message)

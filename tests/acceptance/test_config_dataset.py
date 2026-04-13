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
from digital_land.specification import Specification

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


def _run_checkpoint(dataset, file_path, rules, reference_file_path=None):
    reference_file_path = reference_file_path or file_path
    try:
        checkpoint = CsvCheckpoint(dataset=dataset, file_path=file_path)
    except Exception as e:
        pytest.fail(f"Failed to initiate checkpoint for CSV '{file_path}': {e}")

    checkpoint.load(rules)
    checkpoint.run()

    def _extract_line_numbers(details):
        if not isinstance(details, dict):
            return []

        line_numbers = []

        invalid_rows = details.get("invalid_rows")
        if isinstance(invalid_rows, list):
            for row in invalid_rows:
                if not isinstance(row, dict):
                    continue
                line_number = row.get("line_number")
                if isinstance(line_number, int):
                    line_numbers.append(line_number)
                elif isinstance(line_number, str) and line_number.isdigit():
                    line_numbers.append(int(line_number))

        return sorted(set(line_numbers))

    failed = [entry for entry in checkpoint.log.entries if not entry["passed"]]
    if failed:
        messages = []
        for entry in failed:
            messages.append(f"  - {entry['name']}: {entry['message']}")
            details = entry.get("details")
            if details:
                if isinstance(details, str):
                    try:
                        details = json.loads(details)
                    except json.JSONDecodeError:
                        messages.append(f"    {details}")
                        continue
                messages.append(f"    {json.dumps(details, indent=4)}")

                line_numbers = _extract_line_numbers(details)
                if line_numbers:
                    line_refs = [
                        _format_line_reference(reference_file_path, line_number)
                        for line_number in line_numbers[:50]
                    ]
                    messages.append(f"    references: {line_refs}")
        assert False, "\n".join(messages)


def _normalise_file(file_path, tmp_path):
    """Normalise line endings and encoding for consistent CSV parsing."""
    src = Path(file_path)
    tmp = Path(tmp_path) / src.name
    tmp.parent.mkdir(parents=True, exist_ok=True)

    with src.open("r", encoding="utf-8-sig", newline="") as fin, tmp.open(
        "w", encoding="utf-8", newline=""
    ) as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout, lineterminator="\n")
        for row in reader:
            writer.writerow(row)

    return str(tmp)


DATATYPE_CHECKPOINTS = {
    "integer": "expect_column_to_be_integer",
    "decimal": "expect_column_to_be_decimal",
    "flag": "expect_column_to_be_flag",
    "latitude": "expect_column_to_be_latitude",
    "longitude": "expect_column_to_be_longitude",
    "curie": "expect_column_to_be_curie",
    "curie-list": "expect_column_to_be_curie_list",
    "json": "expect_column_to_be_json",
    "date": "expect_column_to_be_date",
    "datetime": "expect_column_to_be_date",
    "pattern": "expect_column_to_be_pattern",
    "multipolygon": "expect_column_to_be_multipolygon",
    "point": "expect_column_to_be_point",
}


def _build_all_csv_rules(file_path, specification_dir):
    specification = Specification(specification_dir)
    field_datatype = specification.get_field_datatype_map()

    rules = [
        {
            "name": "all csv have no blank rows",
            "operation": "check_no_blank_rows",
            "parameters": {},
            "severity": "error",
        }
    ]

    with open(file_path, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        columns = [col.strip() for col in header]

    for column in columns:
        datatype = field_datatype.get(column)
        operation = DATATYPE_CHECKPOINTS.get(datatype)
        if operation:
            rules.append(
                {
                    "name": f"column '{column}' has valid {datatype} values",
                    "operation": operation,
                    "parameters": {"field": column},
                    "severity": "error",
                }
            )

    return rules


# TEST lookup.csv

lookup_files = _collect_files("lookup.csv")


@pytest.mark.parametrize(
    "file_path",
    lookup_files,
    ids=[_test_id(f) for f in lookup_files],
)
def test_lookup(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    lookup_dir = Path(file_path).parent
    entity_org_file = str(lookup_dir / "entity-organisation.csv")
    entity_org_file = _normalise_file(entity_org_file, tmp_path)
    file_path = _normalise_file(file_path, tmp_path)
    lookup_rules = [
        {
            "name": "lookup entities are within organisation ranges",
            "operation": "check_field_is_within_range_by_dataset_org",
            "parameters": {
                "field": "entity",
                "external_file": entity_org_file,
                "min_field": "entity-minimum",
                "max_field": "entity-maximum",
                "lookup_dataset_field": "prefix",
                "range_dataset_field": "dataset",
                "rules": {
                    "lookup_rules": [
                        {
                            "prefix": {
                                "op": "not in",
                                "value": [
                                    "statistical-geography",
                                    "conservation-area",
                                    "planning-condition",
                                    "battlefield",
                                    "planning-application-condition",
                                    "certificate-of-immunity",
                                    "heritage-at-risk",
                                    "scheduled-monument",
                                    "world-heritage-site"
                                ],
                            },
                            "organisation": {
                                "op": "!=",
                                "value": "government-organisation:D1342",
                            },
                        },
                        {
                            "prefix": {"op": "==", "value": "conservation-area"},
                            "organisation": {
                                "op": "not in",
                                "value": [
                                    "government-organisation:D1342",
                                    "government-organisation:PB1164",
                                ],
                            },
                        },
                    ]
                },
            },
            "severity": "error",
        },
    ]
    all_csv_rules = _build_all_csv_rules(file_path, specification_dir)
    _run_checkpoint(
        dataset="lookup",
        file_path=file_path,
        rules=lookup_rules + all_csv_rules,
        reference_file_path=source_file_path,
    )


# TEST CSV GROUPS

column_csv_files = _collect_files("column.csv")


@pytest.mark.parametrize(
    "file_path",
    column_csv_files,
    ids=[_test_id(f) for f in column_csv_files],
)
def test_column_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="column-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


combine_csv_files = _collect_files("combine.csv")


@pytest.mark.parametrize(
    "file_path",
    combine_csv_files,
    ids=[_test_id(f) for f in combine_csv_files],
)
def test_combine_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="combine-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


concat_csv_files = _collect_files("concat.csv")


@pytest.mark.parametrize(
    "file_path",
    concat_csv_files,
    ids=[_test_id(f) for f in concat_csv_files],
)
def test_concat_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="all-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


convert_csv_files = _collect_files("convert.csv")


@pytest.mark.parametrize(
    "file_path",
    convert_csv_files,
    ids=[_test_id(f) for f in convert_csv_files],
)
def test_convert_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="convert-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


default_csv_files = _collect_files("default.csv")


@pytest.mark.parametrize(
    "file_path",
    default_csv_files,
    ids=[_test_id(f) for f in default_csv_files],
)
def test_default_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="default-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


default_value_csv_files = _collect_files("default-value.csv")


@pytest.mark.parametrize(
    "file_path",
    default_value_csv_files,
    ids=[_test_id(f) for f in default_value_csv_files],
)
def test_default_value_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="default-value-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


endpoint_csv_files = _collect_files("endpoint.csv")


@pytest.mark.parametrize(
    "file_path",
    endpoint_csv_files,
    ids=[_test_id(f) for f in endpoint_csv_files],
)
def test_endpoint_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="endpoint-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


expect_csv_files = _collect_files("expect.csv")


@pytest.mark.parametrize(
    "file_path",
    expect_csv_files,
    ids=[_test_id(f) for f in expect_csv_files],
)
def test_expect_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="expect-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


filter_csv_files = _collect_files("filter.csv")


@pytest.mark.parametrize(
    "file_path",
    filter_csv_files,
    ids=[_test_id(f) for f in filter_csv_files],
)
def test_filter_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="filter-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


old_entity_csv_files = _collect_files("old-entity.csv")


@pytest.mark.parametrize(
    "file_path",
    old_entity_csv_files,
    ids=[_test_id(f) for f in old_entity_csv_files],
)
def test_old_entity_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="old-entity-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


old_resource_csv_files = _collect_files("old-resource.csv")


@pytest.mark.parametrize(
    "file_path",
    old_resource_csv_files,
    ids=[_test_id(f) for f in old_resource_csv_files],
)
def test_old_resource_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="old-resource-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


patch_csv_files = _collect_files("patch.csv")


@pytest.mark.parametrize(
    "file_path",
    patch_csv_files,
    ids=[_test_id(f) for f in patch_csv_files],
)
def test_patch_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="patch-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


skip_csv_files = _collect_files("skip.csv")


@pytest.mark.parametrize(
    "file_path",
    skip_csv_files,
    ids=[_test_id(f) for f in skip_csv_files],
)
def test_skip_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="skip-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


source_csv_files = _collect_files("source.csv")


@pytest.mark.parametrize(
    "file_path",
    source_csv_files,
    ids=[_test_id(f) for f in source_csv_files],
)
def test_source_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="source-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


transform_csv_files = _collect_files("transform.csv")


@pytest.mark.parametrize(
    "file_path",
    transform_csv_files,
    ids=[_test_id(f) for f in transform_csv_files],
)
def test_transform_csv(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    file_path = _normalise_file(file_path, tmp_path)
    _run_checkpoint(
        dataset="transform-csv",
        file_path=file_path,
        rules=_build_all_csv_rules(file_path, specification_dir),
        reference_file_path=source_file_path,
    )


# TEST OLD_ENTITY.CSV

old_entity_files = _collect_files("old-entity.csv")


@pytest.mark.parametrize(
    "file_path",
    old_entity_files,
    ids=[_test_id(f) for f in old_entity_files],
)
def test_old_entity(file_path, specification_dir):
    source_file_path = file_path
    all_csv_rules = _build_all_csv_rules(file_path, specification_dir)
    old_entity_rules = [
        {
            "name": "old-entity values are unique",
            "operation": "check_unique",
            "parameters": {"field": "old-entity"},
            "severity": "error",
        },
        {
            "name": "old-entity statuses only contains 301 or 410",
            "operation": "check_allowed_values",
            "parameters": {"field": "status", "allowed_values": ["301", "410"]},
            "severity": "error",
        },
    ]
    _run_checkpoint(
        dataset="old-entity",
        file_path=file_path,
        rules=old_entity_rules + all_csv_rules,
        reference_file_path=source_file_path,
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
def test_entity_organisation(file_path, tmp_path, specification_dir):
    source_file_path = file_path
    normalised = tmp_path / Path(file_path).name
    normalised.write_bytes(
        Path(file_path).read_bytes().replace(b"\r\n", b"\n").replace(b",\n", b"\n")
    )
    all_csv_rules = _build_all_csv_rules(file_path, specification_dir)
    _run_checkpoint(
        dataset="entity-organisation",
        file_path=str(normalised),
        rules=ENTITY_ORGANISATION_RULES + all_csv_rules,
        reference_file_path=source_file_path,
    )

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


def _run_checkpoint(dataset, file_path, rules):
    try:
        checkpoint = CsvCheckpoint(dataset=dataset, file_path=file_path)
    except Exception as e:
        pytest.fail(f"Failed to initiate checkpoint for CSV '{file_path}': {e}")

    checkpoint.load(rules)
    checkpoint.run()

    failed = [entry for entry in checkpoint.log.entries if not entry["passed"]]
    assert not failed, "\n".join(
        f"  - {entry['name']}: {entry['message']}" for entry in failed
    )

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

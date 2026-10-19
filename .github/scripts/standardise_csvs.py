import os
import csv
import sys

# Add parent directories to path to import from root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from create_collection import COLUMN_MAPPINGS

SORT_MAPPINGS = {
    "collection": {
        "endpoint.csv": ["entry-date", "endpoint"],
        "source.csv": ["entry-date", "endpoint"],
    },
    "pipeline": {
        "column.csv": ["dataset", "endpoint", "resource", "field"],
        "combine.csv": ["dataset", "endpoint", "field"],
        "concat.csv": ["dataset", "endpoint", "resource", "field"],
        "default-value.csv": ["dataset", "field"],
        "default.csv": ["dataset", "field", "default-field"],
        "entity-organisation.csv": ["dataset", "organisation", "entity-minimum"],
        "expect.csv": ["datasets", "operation", "organisations"],
        "filter.csv": ["dataset", "endpoint", "field"],
        "lookup.csv": ["prefix", "entity"],
        "old-entity.csv": ["old-entity"],
        "patch.csv": ["dataset", "endpoint", "field"],
        "skip.csv": ["dataset", "endpoint", "pattern"],
        "transform.csv": ["dataset", "replacement-field"],
    }
}

# Files where rows describing a numeric range should be merged when the ranges
# are adjacent (max + 1 == next min) or overlapping. Rows are only merged when
# every other column (e.g. dataset, organisation) is identical.
MERGE_RANGE_MAPPINGS = {
    "pipeline": {
        "entity-organisation.csv": {
            "min_col": "entity-minimum",
            "max_col": "entity-maximum",
        },
    }
}


def _sort_key(row, sort_cols):
    """Sort key that puts empty values last."""
    return tuple(
        (0, (row.get(col) or "").strip().lower()) if (row.get(col) or "").strip()
        else (1, "")
        for col in sort_cols
    )


def _merge_ranges(rows, expected_cols, min_col, max_col):
    """Merge neighbouring rows whose [min_col, max_col] integer ranges are
    adjacent (max + 1 == next min) or overlapping.

    Rows are grouped by every other column (e.g. dataset, organisation) and
    ordered numerically by min_col before merging, so the result does not
    depend on the incoming row order. Two rows are only merged when every
    other column is identical. Rows with non-integer range values are left
    untouched.
    """
    other_cols = [c for c in expected_cols if c not in (min_col, max_col)]

    def other_values(row):
        return tuple((row.get(col) or "").strip() for col in other_cols)

    def numeric_min(row):
        try:
            return (0, int(row.get(min_col)))
        except (TypeError, ValueError):
            return (1, 0)

    # Sort numerically: the caller's string sort puts "1000" before "200"
    rows = sorted(rows, key=lambda row: (other_values(row), numeric_min(row)))

    merged = []
    for row in rows:
        prev = merged[-1] if merged else None
        if prev is not None and other_values(prev) == other_values(row):
            try:
                prev_high = int(prev[max_col])
                low, high = int(row[min_col]), int(row[max_col])
                int(prev[min_col])
            except (KeyError, TypeError, ValueError):
                prev_high = None
            if prev_high is not None and low <= prev_high + 1:
                prev[max_col] = str(max(prev_high, high))
                continue
        merged.append(dict(row))
    return merged


def standardise_csv(file_path, expected_columns, sort_cols=None, merge_config=None):
    """Reorder and add missing columns to a CSV file, preserving line endings."""
    expected_cols = expected_columns.split(',')

    try:
        # Read existing data
        with open(file_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # Check for unexpected columns
        actual_cols = list(reader.fieldnames or [])
        unexpected = [col for col in actual_cols if col not in expected_cols]
        if unexpected:
            return f"✗ {file_path}: unexpected column(s) found that would be removed: {', '.join(unexpected)}"

        # Check for extra values beyond the header (e.g. stray trailing commas)
        for row in rows:
            if None in row:
                return f"✗ {file_path}: one or more rows have more values than columns in the header"

        # Sort rows if sort columns are specified
        if sort_cols:
            rows.sort(key=lambda row: _sort_key(row, sort_cols))

        # Merge adjacent/overlapping ranges (after sorting, so they are neighbours)
        if merge_config:
            before = len(rows)
            rows = _merge_ranges(rows, expected_cols, **merge_config)
            if sort_cols:
                rows.sort(key=lambda row: _sort_key(row, sort_cols))
            if len(rows) < before:
                print(f"  merged {before - len(rows)} range row(s) in {file_path}")

        # Write back with standard column order, row order, and CRLF line endings
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=expected_cols, restval='', lineterminator='\r\n')
            writer.writeheader()
            writer.writerows(rows)

        return None
    except Exception as e:
        return f"✗ {file_path}: {e}"


def standardise_folder(folder_type, folder_path):
    """Standardise all CSVs in a folder (collection or pipeline)."""
    if folder_type not in COLUMN_MAPPINGS:
        print(f"Unknown folder type: {folder_type}")
        return

    for filename, expected_columns in COLUMN_MAPPINGS[folder_type].items():
        file_path = os.path.join(folder_path, filename)
        if os.path.exists(file_path):
            sort_cols = SORT_MAPPINGS.get(folder_type, {}).get(filename)
            merge_config = MERGE_RANGE_MAPPINGS.get(folder_type, {}).get(filename)
            result = standardise_csv(file_path, expected_columns, sort_cols, merge_config)
            if result:
                print(result)
        else:
            print(f"⊘ {file_path} (not found)")


def main():
    """Standardise all CSVs in all datasets across pipeline and collection."""
    # Get the root directory (two levels up from this script)
    base_dir = os.path.join(os.path.dirname(__file__), '../..')

    errors = []

    for folder_type in ["collection", "pipeline"]:
        folder_path = os.path.join(base_dir, folder_type)

        if not os.path.exists(folder_path):
            print(f"⊘ {folder_type}/ not found")
            continue

        # Get all dataset folders in this folder_type
        dataset_folders = [d for d in os.listdir(folder_path)
                           if os.path.isdir(os.path.join(folder_path, d))]

        for dataset in dataset_folders:
            dataset_path = os.path.join(folder_path, dataset)
            print(f"\nStandardising {folder_type}/{dataset}...")

            for filename, expected_columns in COLUMN_MAPPINGS[folder_type].items():
                file_path = os.path.join(dataset_path, filename)
                if os.path.exists(file_path):
                    sort_cols = SORT_MAPPINGS.get(folder_type, {}).get(filename)
                    merge_config = MERGE_RANGE_MAPPINGS.get(folder_type, {}).get(filename)
                    result = standardise_csv(file_path, expected_columns, sort_cols, merge_config)
                    if result:
                        print(result)
                        errors.append(result)
                else:
                    print(f"⊘ {filename} (not found)")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()

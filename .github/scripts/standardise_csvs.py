import os
import csv
import sys

# Add parent directories to path to import from root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from create_collection import COLUMN_MAPPINGS

SORT_MAPPINGS = {
    "collection": {
        "endpoint.csv":     ["entry-date", "endpoint"],
        "source.csv":       ["entry-date", "endpoint"],
    },
    "pipeline": {
        "column.csv":              ["dataset", "endpoint", "resource", "field"],
        "combine.csv":             ["dataset", "endpoint", "field"],
        "concat.csv":              ["dataset", "endpoint", "resource", "field"],
        "default-value.csv":       ["dataset", "field"],
        "default.csv":             ["dataset", "field", "default-field"],
        "entity-organisation.csv": ["dataset", "organisation", "entity-minimum"],
        "expect.csv":              ["datasets", "operation", "organisations"],
        "filter.csv":              ["dataset", "endpoint", "field"],
        "lookup.csv":              ["prefix", "entity"],
        "old-entity.csv":          ["old-entity"],
        "patch.csv":               ["dataset", "endpoint", "field"],
        "skip.csv":                ["dataset", "endpoint", "pattern"],
        "transform.csv":           ["dataset", "replacement-field"],
    }
}

def _sort_key(row, sort_cols):
    """Sort key that puts empty values last."""
    return tuple(
        (0, (row.get(col) or "").strip().lower()) if (row.get(col) or "").strip()
        else (1, "")
        for col in sort_cols
    )

def standardise_csv(file_path, expected_columns, sort_cols=None):
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
            result = standardise_csv(file_path, expected_columns, sort_cols)
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
                    result = standardise_csv(file_path, expected_columns, sort_cols)
                    if result:
                        print(result)
                        errors.append(result)
                else:
                    print(f"⊘ {filename} (not found)")

    if errors:
        sys.exit(1)

if __name__ == "__main__":
    main()

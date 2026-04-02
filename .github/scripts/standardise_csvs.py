import os
import csv
import sys

# Add parent directories to path to import from root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from create_collection import COLUMN_MAPPINGS

def standardise_csv(file_path, expected_columns):
    """Reorder and add missing columns to a CSV file, preserving line endings."""
    expected_cols = expected_columns.split(',')

    try:
        # Detect original line ending
        with open(file_path, 'rb') as f:
            content = f.read()
            if b'\r\n' in content:
                line_ending = '\r\n'  # CRLF (Windows)
            else:
                line_ending = '\n'    # LF (Unix)

        # Read existing data
        with open(file_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        # Write back with standard column order and preserved line ending
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=expected_cols, restval='', lineterminator=line_ending)
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
            result = standardise_csv(file_path, expected_columns)
            if result:
                print(result)
        else:
            print(f"⊘ {file_path} (not found)")

def main():
    """Standardise all CSVs in all datasets across pipeline and collection."""
    # Get the root directory (two levels up from this script)
    base_dir = os.path.join(os.path.dirname(__file__), '../..')

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
                    result = standardise_csv(file_path, expected_columns)
                    if result:
                        print(result)
                else:
                    print(f"⊘ {filename} (not found)")

if __name__ == "__main__":
    main()

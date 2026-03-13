#!/usr/bin/env python3
"""
Deduplicate conservation area geographies.

Processes duplicate geometry checks and generates old-entity redirects
for conservation areas with:
- Complete matches (100% geometry overlap)
- Single matches with high name similarity (>85%)
"""

import csv
import urllib.request
from datetime import datetime
from pathlib import Path
from rapidfuzz import fuzz
import time
import tempfile

CHECKS_URL = 'https://files.planning.data.gov.uk/reporting/duplicate_entity_expectation.csv'
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
OLD_ENTITY_PATH = REPO_ROOT / 'pipeline' / 'conservation-area' / 'old-entity.csv'

# Network retry configuration
MAX_RETRIES = 3
TIMEOUT_SECONDS = 120  # 120 seconds for GitHub Actions environment
INITIAL_BACKOFF = 2  # seconds
CHUNK_SIZE = 104857600  # 100MB chunks


def stream_checks_data():
    """Download and stream the duplicate checks data from URL with retry logic."""
    print("Loading duplicate geometry checks...")
    # Increase field size limit for large geometry fields
    csv.field_size_limit(int(1e8))

    for attempt in range(1, MAX_RETRIES + 1):
        temp_file = None
        try:
            print(f"Attempt {attempt}/{MAX_RETRIES} to download duplicate checks data...")
            bytes_downloaded = 0

            # Download to a temporary file instead of memory
            temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv')
            temp_path = temp_file.name

            with urllib.request.urlopen(CHECKS_URL, timeout=TIMEOUT_SECONDS) as response:
                while True:
                    chunk = response.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    temp_file.write(chunk)
                    bytes_downloaded += len(chunk)
                    if bytes_downloaded % (500 * 1024 * 1024) == 0:  # Log every ~500MB
                        print(f"  Downloaded {bytes_downloaded / 1024 / 1024:.1f} MB...")

            temp_file.close()
            print(f"Successfully downloaded {bytes_downloaded / 1024 / 1024:.1f} MB to disk")

            # Now read the file and parse rows, keeping only needed columns
            print("Parsing records...")
            rows = []
            rows_processed = 0

            # Columns we actually need for deduplication
            needed_columns = {
                'message', 'dataset', 'entity_a', 'entity_b',
                'entity_a_name', 'entity_b_name', 'lookup-org-a', 'lookup-org-b', 'in-odp'
            }

            with open(temp_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Filter to only keep needed columns (excludes geometry fields)
                    filtered_row = {k: v for k, v in row.items() if k in needed_columns}
                    rows.append(filtered_row)
                    rows_processed += 1
                    if rows_processed % 10000 == 0:
                        print(f"  Processed {rows_processed} records...")

            print(f"Loaded {len(rows)} records")

            # Clean up temp file
            Path(temp_path).unlink()

            return rows

        except urllib.error.URLError as e:
            if temp_file:
                try:
                    temp_file.close()
                    Path(temp_file.name).unlink()
                except:
                    pass
            if attempt == MAX_RETRIES:
                print(f"Error: Failed to download after {MAX_RETRIES} attempts: {e}")
                raise
            backoff = INITIAL_BACKOFF * (2 ** (attempt - 1))
            print(f"Network error (attempt {attempt}): {e}")
            print(f"Retrying in {backoff} seconds...")
            time.sleep(backoff)
        except Exception as e:
            if temp_file:
                try:
                    temp_file.close()
                    Path(temp_file.name).unlink()
                except:
                    pass
            print(f"Error loading checks data: {e}")
            raise


def load_old_entity():
    """Load existing old-entity data."""
    print(f"Loading existing old-entity data from {OLD_ENTITY_PATH}...")

    rows = []
    with open(OLD_ENTITY_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Loaded {len(rows)} existing records")
    return rows


def extract_complete_matches(df):
    """Extract complete match duplicates and format for old-entity.csv."""
    print("\nFiltering for complete matches...")

    complete_matches = [row for row in df if row['message'] == 'complete_match'
                        and row['dataset'] == 'conservation-area'
                        and row.get('lookup-org-a') == 'government-organisation:PB1164'
                        and row.get('lookup-org-b') != 'government-organisation:PB1164']
    print(f"Found {len(complete_matches)} complete matches")

    # Format for old-entity.csv
    formatted = []
    today = datetime.now().strftime('%Y-%m-%d')

    for row in complete_matches:
        formatted.append({
            'old-entity': row['entity_a'],
            'status': '301',
            'entity': row['entity_b'],
            'end-date': '',
            'notes': 'Redirect old entity complete match to LPA entity',
            'entry-date': today,
            'start-date': ''
        })

    return formatted


def extract_single_matches(df):
    """Extract single matches with high name similarity and format for old-entity.csv."""
    print("\nFiltering for single matches...")

    # Filter for single matches in conservation-area
    single_matches = [row for row in df if row['message'] == 'single_match'
                      and row['dataset'] == 'conservation-area'
                      and row.get('lookup-org-a') == 'government-organisation:PB1164'
                      and row.get('lookup-org-b') != 'government-organisation:PB1164'
                      and row.get('in-odp', '').lower() == 'true']

    print(f"Found {len(single_matches)} single matches meeting criteria")

    # Calculate name similarity and filter for high matches
    formatted = []
    today = datetime.now().strftime('%Y-%m-%d')
    threshold = 85  # Similarity threshold (0-100)

    for row in single_matches:
        entity_a_name = str(row.get('entity_a_name', '')).lower()
        entity_b_name = str(row.get('entity_b_name', '')).lower()

        # Calculate similarity score using partial ratio (more lenient with additions/variations)
        similarity = fuzz.partial_ratio(entity_a_name, entity_b_name)

        # Only include if similarity is above threshold
        if similarity > threshold:
            formatted.append({
                'old-entity': row['entity_a'],
                'status': '301',
                'entity': row['entity_b'],
                'end-date': '',
                'notes': 'Redirect old entity single match to LPA entity',
                'entry-date': today,
                'start-date': ''
            })

    print(f"Found {len(formatted)} single matches with >{threshold}% name similarity")
    return formatted


def combine_data(old_entity, new_matches):
    """Combine old and new data."""
    print("\nCombining data...")

    # Convert old entity data to have same structure
    combined = []
    for row in old_entity:
        combined.append({
            'old-entity': row.get('old-entity', ''),
            'status': row.get('status', ''),
            'entity': row.get('entity', ''),
            'end-date': row.get('end-date', ''),
            'notes': row.get('notes', ''),
            'entry-date': row.get('entry-date', ''),
            'start-date': row.get('start-date', '')
        })

    # Add new matches
    combined.extend(new_matches)

    print(f"Total records after merge: {len(combined)}")
    return combined


def save_output(data):
    """Save the updated data to CSV."""
    print(f"\nSaving to {OLD_ENTITY_PATH}...")

    fieldnames = ['old-entity', 'status', 'entity', 'end-date', 'notes', 'entry-date', 'start-date']

    with open(OLD_ENTITY_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print("Done!")

    # Show preview of new redirects
    print("\nPreview of new redirects:")
    if len(data) > 10:
        for row in data[-10:]:
            print(f"  {row['old-entity']} → {row['entity']} (status: {row['status']})")


def main():
    """Main execution function."""
    try:
        checks_data = stream_checks_data()
        old_entity = load_old_entity()

        # Extract both complete and single matches
        complete_matches = extract_complete_matches(checks_data)
        single_matches = extract_single_matches(checks_data)

        # Combine both types of matches
        all_new_matches = complete_matches + single_matches
        print(f"\nTotal new redirects to add: {len(all_new_matches)}")

        combined = combine_data(old_entity, all_new_matches)
        save_output(combined)
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == '__main__':
    main()

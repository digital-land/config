#!/usr/bin/env python3
"""
Deduplicate conservation area geographies.

Processes duplicate geometry checks and generates old-entity redirects
for conservation areas with:
- Complete matches (100% geometry overlap)
- Single matches with high name similarity (>80%)
"""

import csv
import urllib.request
from datetime import datetime
from pathlib import Path
from io import StringIO
from difflib import SequenceMatcher

CHECKS_URL = 'https://files.planning.data.gov.uk/reporting/duplicate_entity_expectation.csv'
REPO_ROOT = Path(__file__).resolve().parent.parent
OLD_ENTITY_PATH = REPO_ROOT / 'pipeline' / 'conservation-area' / 'old-entity.csv'


def load_checks_data():
    """Load the duplicate checks data from URL."""
    print("Loading duplicate geometry checks...")
    # Increase field size limit for large geometry fields
    csv.field_size_limit(int(1e8))

    with urllib.request.urlopen(CHECKS_URL) as response:
        data = response.read().decode('utf-8')

    reader = csv.DictReader(StringIO(data))
    rows = list(reader)
    print(f"Loaded {len(rows)} records")
    return rows


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

    complete_matches = [row for row in df if row['message'] == 'complete_match' and row['dataset'] == 'conservation-area']
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
                      and row.get('in-odp', '').lower() == 'true']

    print(f"Found {len(single_matches)} single matches meeting criteria")

    # Calculate name similarity and filter for high matches
    formatted = []
    today = datetime.now().strftime('%Y-%m-%d')
    threshold = 80  # Similarity threshold (0-100)

    for row in single_matches:
        entity_a_name = str(row.get('entity_a_name', '')).lower()
        entity_b_name = str(row.get('entity_b_name', '')).lower()

        # Calculate similarity score
        similarity = SequenceMatcher(None, entity_a_name, entity_b_name).ratio() * 100

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
        checks_data = load_checks_data()
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

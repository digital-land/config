#!/usr/bin/env python3
"""
Deduplicate conservation area geographies.

Processes duplicate geometry checks and generates old-entity redirects
for conservation areas with:
- Complete matches (100% geometry overlap)
- Single matches with high name similarity (>85%)

Each old-entity is written once. A complete match means identical geometry, so when
a Historic England entity completely matches several LPA entities it is redirected
(301) to one of them rather than ended. The target is chosen in this order:
1. The LPA entity listed in ca-authoritative-entities.csv (the organisation that
   really owns the area). The other, non-authoritative LPA entities are redirected
   to it too.
2. Otherwise the lowest entity number, and the case is flagged for review.
"""

import csv
import os
import urllib.request
from datetime import datetime
from pathlib import Path
from rapidfuzz import fuzz
import time
import tempfile

CHECKS_URL = 'https://files.planning.data.gov.uk/reporting/duplicate_entity_expectation.csv'
HISTORIC_ENGLAND_ORG = 'government-organisation:PB1164'
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
OLD_ENTITY_PATH = REPO_ROOT / 'pipeline' / 'conservation-area' / 'old-entity.csv'
AUTHORITATIVE_PATH = Path(__file__).resolve().parent / 'ca-authoritative-entities.csv'

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
                'entity_a_name', 'entity_b_name', 'lookup_org_a', 'lookup_org_b', 'in_odp'
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


def historic_england_pairs(df, message):
    """Return conservation-area pairs of the given match type between a Historic
    England entity and a non-Historic England entity.

    Pairs can be reported either way round, so each pair is oriented so that the
    Historic England entity is always entity_a. Pairs reported in both orientations
    are only returned once.
    """
    pairs = []
    seen = set()

    for row in df:
        if row['message'] != message or row['dataset'] != 'conservation-area':
            continue

        org_a, org_b = row.get('lookup_org_a'), row.get('lookup_org_b')
        if org_a == HISTORIC_ENGLAND_ORG and org_b != HISTORIC_ENGLAND_ORG:
            oriented = row
        elif org_b == HISTORIC_ENGLAND_ORG and org_a != HISTORIC_ENGLAND_ORG:
            oriented = {
                **row,
                'entity_a': row['entity_b'],
                'entity_b': row['entity_a'],
                'entity_a_name': row.get('entity_b_name', ''),
                'entity_b_name': row.get('entity_a_name', ''),
                'lookup_org_a': org_b,
                'lookup_org_b': org_a,
            }
        else:
            continue

        key = (oriented['entity_a'], oriented['entity_b'])
        if key not in seen:
            seen.add(key)
            pairs.append(oriented)

    return pairs


def load_authoritative_entities(path=AUTHORITATIVE_PATH):
    """Load the reviewed list of authoritative LPA entity ranges.

    Each row says that entities min-max belong to the given organisation, and that
    it is the authoritative publisher where another organisation has published a
    copy of the same area.
    """
    if not Path(path).exists():
        return []

    ranges = []
    with open(path, 'r', encoding='utf-8', newline='') as f:
        for row in csv.DictReader(f):
            if row.get('dataset') != 'conservation-area':
                continue
            ranges.append((int(row['entity-minimum']), int(row['entity-maximum']), row['organisation']))
    return ranges


def is_authoritative(pair, authoritative):
    """True if the LPA entity (entity_b) of an oriented pair is in the authoritative list."""
    entity = int(pair['entity_b'])
    return any(low <= entity <= high and pair.get('lookup_org_b') == org
               for low, high, org in authoritative)


def choose_redirect_target(candidates, authoritative=()):
    """Pick the entity to redirect to when one Historic England entity completely
    matches several LPA entities. Returns (chosen pair, reason).

    A complete match means identical geometry, so any candidate is a correct redirect
    target, but the authoritative publisher's entity is the right one to keep.

    reason is:
    - 'only': a single match, nothing to choose.
    - 'authoritative': exactly one candidate is in the authoritative list.
    - 'lowest': no single authoritative candidate, so the lowest entity number (the
      earliest created) is used. It is deterministic, so the choice does not change
      between nightly runs, and the caller flags it for review.
    """
    if len(candidates) == 1:
        return candidates[0], 'only'

    authoritative_candidates = [c for c in candidates if is_authoritative(c, authoritative)]
    if len(authoritative_candidates) == 1:
        return authoritative_candidates[0], 'authoritative'

    pool = authoritative_candidates or candidates
    return min(pool, key=lambda row: int(row['entity_b'])), 'lowest'


def resolve_complete_matches(df, authoritative=()):
    """Work out the redirects for complete matches.

    Returns (historic_england_rows, duplicate_rows, flagged):
    - historic_england_rows: one 301 row per Historic England entity.
    - duplicate_rows: 301 rows redirecting the non-authoritative LPA entities to the
      authoritative one, only where an authoritative entity was identified.
    - flagged: Historic England entities with several matches and no single
      authoritative entity, for review.
    """
    print("\nFiltering for complete matches...")

    complete_matches = historic_england_pairs(df, 'complete_match')
    print(f"Found {len(complete_matches)} complete matches")

    matches_by_entity = {}
    for row in complete_matches:
        matches_by_entity.setdefault(row['entity_a'], []).append(row)

    today = datetime.now().strftime('%Y-%m-%d')
    historic_england_rows = []
    duplicates = {}
    flagged = []

    for old_ent, rows in matches_by_entity.items():
        chosen, reason = choose_redirect_target(rows, authoritative)
        others = sorted((r for r in rows if r is not chosen), key=lambda r: int(r['entity_b']))

        notes = f"Redirect old entity complete match to LPA entity: {chosen['lookup_org_b']}"
        if reason == 'authoritative':
            notes += f" (authoritative of {len(rows)} complete matches)"
        elif reason == 'lowest':
            notes += f" (1 of {len(rows)} complete matches; lowest entity chosen)"

        historic_england_rows.append({
            'old-entity': old_ent,
            'status': '301',
            'entity': chosen['entity_b'],
            'end-date': '',
            'notes': notes,
            'entry-date': today,
            'start-date': ''
        })

        if reason == 'authoritative':
            for other in others:
                if other['entity_b'] not in duplicates:
                    duplicates[other['entity_b']] = {
                        'old-entity': other['entity_b'],
                        'status': '301',
                        'entity': chosen['entity_b'],
                        'end-date': '',
                        'notes': f"Redirect non-authoritative duplicate to LPA entity: {chosen['lookup_org_b']}",
                        'entry-date': today,
                        'start-date': ''
                    }
        elif reason == 'lowest':
            flagged.append({
                'old-entity': old_ent,
                'chosen': chosen['entity_b'],
                'chosen_org': chosen['lookup_org_b'],
                'others': [(r['entity_b'], r['lookup_org_b']) for r in others],
            })

    print(f"{len(historic_england_rows)} Historic England entities to redirect")
    if duplicates:
        print(f"{len(duplicates)} non-authoritative LPA duplicates to redirect to their authoritative entity")
    return historic_england_rows, list(duplicates.values()), flagged


def extract_complete_matches(df, authoritative=()):
    """Complete match redirects for Historic England entities, one row per entity."""
    return resolve_complete_matches(df, authoritative)[0]


def report_flagged_matches(flagged):
    """Flag Historic England entities that match several LPA entities with no
    authoritative one, so someone can decide which organisation owns the area.

    Prints a GitHub Actions warning and, when running in Actions, adds a table to the
    job summary. If a decision is made, add the authoritative entities to
    ca-authoritative-entities.csv.
    """
    if not flagged:
        return

    lines = [f"{len(flagged)} Historic England entities completely match several LPA entities "
             "with no authoritative LPA listed in ca-authoritative-entities.csv; "
             "redirected to the lowest entity number. Review which organisation owns the area:"]
    for item in flagged:
        others = ', '.join(f"{ent} ({org})" for ent, org in item['others'])
        lines.append(f"  {item['old-entity']} -> {item['chosen']} ({item['chosen_org']}); also matches {others}")
    print("\n" + "\n".join(lines))
    print(f"::warning title=CA complete matches need an authoritative LPA::{len(flagged)} Historic England "
          "entities match several LPA entities, see the job summary")

    summary_path = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary_path:
        with open(summary_path, 'a', encoding='utf-8') as f:
            f.write("### Conservation areas needing an authoritative LPA\n\n")
            f.write("Each Historic England entity below completely matches several LPA entities. "
                    "It was redirected to the lowest entity number. If another organisation owns the area, "
                    "add its entities to `.github/scripts/ca-authoritative-entities.csv`.\n\n")
            f.write("| Historic England entity | Redirected to | Also matches |\n|---|---|---|\n")
            for item in flagged:
                others = ', '.join(f"{ent} ({org})" for ent, org in item['others'])
                f.write(f"| {item['old-entity']} | {item['chosen']} ({item['chosen_org']}) | {others} |\n")


def extract_single_matches(df):
    """Extract single matches with high name similarity and format for old-entity.csv."""
    print("\nFiltering for single matches...")

    # Filter for single matches in conservation-area
    single_matches = [row for row in historic_england_pairs(df, 'single_match')
                      if row.get('in_odp', '').lower() == 'true']

    print(f"Found {len(single_matches)} single matches meeting criteria")

    # Calculate name similarity and filter for high matches
    today = datetime.now().strftime('%Y-%m-%d')
    threshold = 85  # Similarity threshold (0-100)

    # First pass: collect all matches above threshold with similarity scores
    high_similarity_matches = []
    for row in single_matches:
        entity_a_name = str(row.get('entity_a_name', '')).lower()
        entity_b_name = str(row.get('entity_b_name', '')).lower()

        # Calculate similarity score using partial ratio (more lenient with additions/variations)
        similarity = fuzz.partial_ratio(entity_a_name, entity_b_name)

        # Only include if similarity is above threshold
        if similarity > threshold:
            high_similarity_matches.append(row)

    # Identify which entity_a values appear multiple times (splits)
    entity_a_counts = {}
    for row in high_similarity_matches:
        entity_a = row['entity_a']
        entity_a_counts[entity_a] = entity_a_counts.get(entity_a, 0) + 1

    # Second pass: separate into regular redirects and split cases
    formatted = []
    split_entities = set()

    for row in high_similarity_matches:
        entity_a = row['entity_a']

        if entity_a_counts[entity_a] > 1:
            # This entity_a splits into multiple entity_b - only add once
            if entity_a not in split_entities:
                formatted.append({
                    'old-entity': entity_a,
                    'status': '410',
                    'entity': '',
                    'end-date': '',
                    'notes': 'Matches with multiple entities',
                    'entry-date': today,
                    'start-date': ''
                })
                split_entities.add(entity_a)
        else:
            # Regular single match - add as 301 redirect
            formatted.append({
                'old-entity': entity_a,
                'status': '301',
                'entity': row['entity_b'],
                'end-date': '',
                'notes': f"Redirect old entity single match to LPA entity: {row['lookup_org_b']}",
                'entry-date': today,
                'start-date': ''
            })

    print(f"Found {len(high_similarity_matches)} single matches with >{threshold}% name similarity")
    print(f"  - {len([x for x in formatted if x['status'] == '301'])} regular redirects (status 301)")
    print(f"  - {len([x for x in formatted if x['status'] == '410'])} split cases (status 410)")
    return formatted


def drop_superseded_single_matches(complete_matches, single_matches):
    """Drop single matches for entities that already have a complete match.

    A complete match is identical geometry and always gives a clean redirect, so it
    wins over a partial match (which could otherwise add a second row, or a 410).
    """
    complete_old_entities = {m['old-entity'] for m in complete_matches}
    kept = [m for m in single_matches if m['old-entity'] not in complete_old_entities]
    dropped = len(single_matches) - len(kept)
    if dropped:
        print(f"Dropped {dropped} single matches for entities that already have a complete match")
    return kept


def check_old_entities_unique(data):
    """Fail before writing if any old-entity appears more than once.

    old-entity.csv must have unique old-entity values (checked by
    tests/acceptance/test_config_dataset.py), so refuse to write a file that would
    break it rather than committing it from the pipeline.
    """
    counts = {}
    for row in data:
        counts[row['old-entity']] = counts.get(row['old-entity'], 0) + 1
    duplicates = sorted(k for k, v in counts.items() if v > 1)
    if duplicates:
        raise ValueError(
            f"{len(duplicates)} old-entity values appear more than once, refusing to write "
            f"{OLD_ENTITY_PATH.name}: {', '.join(duplicates[:20])}"
        )


def filter_conflicting_matches(old_entity, new_matches):
    """Filter out new matches that would create circular references or duplicates.

    Skips a new match if:
    - Its old-entity is already a SOURCE in old-entity.csv (would create a duplicate row).
    - Its old-entity is already a TARGET of an existing redirect (would create a circular
      reference). e.g. if a new redirect A -> B would be added, but A is already a target
      of an existing redirect in old-entity, skip it to preserve the existing A->B, C->B pattern.
    """
    print("\nFiltering for conflicts with existing redirects...")

    existing_sources = set(row['old-entity'] for row in old_entity if row.get('old-entity'))
    existing_targets = set(
        row['entity'] for row in old_entity
        if row.get('status') == '301' and row.get('entity')
    )

    # Filter new matches: skip if the old-entity is already a target
    filtered = []
    skipped_count = 0
    skipped_matches = []

    for match in new_matches:
        old_ent = match['old-entity']
        if old_ent in existing_sources:
            skipped_count += 1
            skipped_matches.append((old_ent, match['entity'], 'already a source'))
        elif old_ent in existing_targets:
            skipped_count += 1
            skipped_matches.append((old_ent, match['entity'], 'already a target'))
        else:
            filtered.append(match)

    if skipped_count > 0:
        print(f"Skipped {skipped_count} new matches:")
        for old_ent, target, reason in skipped_matches:
            print(f"  {old_ent} → {target} ({reason} in existing redirects)")

    return filtered


def filter_authoritative_duplicates(old_entity, duplicates, new_matches):
    """Filter the non-authoritative duplicate redirects.

    These do not use filter_conflicting_matches: that skips any entity that is already
    the target of another redirect, but a duplicate is expected to have redirects
    pointing at it, and resolve_redirect_chains then points those at the authoritative
    entity. A duplicate is only skipped if:
    - it is already a source (it would create a duplicate row), or is a source in the
      new matches, or
    - the authoritative entity already redirects to it (it would create a loop).
    """
    existing_sources = {row['old-entity'] for row in old_entity if row.get('old-entity')}
    new_sources = {m['old-entity'] for m in new_matches}
    existing_redirects = {row['old-entity']: row['entity'] for row in old_entity
                          if row.get('status') == '301' and row.get('entity')}

    kept = []
    for dup in duplicates:
        if dup['old-entity'] in existing_sources or dup['old-entity'] in new_sources:
            print(f"  Skipped duplicate {dup['old-entity']} -> {dup['entity']} (already a source)")
        elif existing_redirects.get(dup['entity']) == dup['old-entity']:
            print(f"  Skipped duplicate {dup['old-entity']} -> {dup['entity']} (would create a loop)")
        else:
            kept.append(dup)
    return kept


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


def resolve_redirect_chains(data):
    """Resolve redirect chains by consolidating to final destinations.

    If entity A redirects to B, and B redirects to C, modify A to point
    directly to C. This results in the A->B, C->B pattern where multiple
    sources point to the same final destination.
    Circular references are skipped to avoid self-loops.
    """
    print("\nResolving redirect chains...")

    # Build a map of old-entity -> entity for status 301 redirects with row indices
    redirect_map = {}
    row_indices = {}
    for idx, row in enumerate(data):
        if row['status'] == '301' and row['entity']:
            old_entity = row['old-entity']
            target_entity = row['entity']
            redirect_map[old_entity] = target_entity
            row_indices[old_entity] = idx

    # Find entities involved in chains (both source and target)
    target_entities = set(redirect_map.values())
    chained_entities = set(redirect_map.keys()) & target_entities

    if not chained_entities:
        print("No redirect chains found")
        return data

    print(f"Found {len(chained_entities)} entities in redirect chains")

    today = datetime.now().strftime('%Y-%m-%d')
    modifications_count = 0

    # For each source, follow the chain to find the final destination
    for source in redirect_map:
        current = source
        visited = set()

        # Follow the chain to find where it ends
        while current in redirect_map and current not in visited:
            visited.add(current)
            current = redirect_map[current]

        # Modify only if:
        # 1. We followed a chain (current != immediate target)
        # 2. The final destination is not the source itself (no self-loops)
        if current != redirect_map[source] and current != source:
            idx = row_indices[source]
            data[idx]['entity'] = current
            data[idx]['notes'] = 'Consolidated redirect to final entity'
            data[idx]['entry-date'] = today
            modifications_count += 1

    print(f"Consolidated {modifications_count} redirects to final destinations")
    print(f"Total records after chain resolution: {len(data)}")
    return data


def save_output(data):
    """Save the updated data to CSV."""
    print(f"\nSaving to {OLD_ENTITY_PATH}...")

    fieldnames = ['old-entity', 'status', 'entity', 'notes', 'end-date', 'entry-date', 'start-date']

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
        authoritative = load_authoritative_entities()
        complete_matches, authoritative_duplicates, flagged = resolve_complete_matches(
            checks_data, authoritative)
        report_flagged_matches(flagged)
        single_matches = extract_single_matches(checks_data)

        # Combine both types of matches, preferring complete matches
        single_matches = drop_superseded_single_matches(complete_matches, single_matches)
        all_new_matches = complete_matches + single_matches
        print(f"\nTotal new redirects to add: {len(all_new_matches)}")

        # Filter out matches that would conflict with existing redirects
        all_new_matches = filter_conflicting_matches(old_entity, all_new_matches)
        print(f"After filtering conflicts: {len(all_new_matches)} matches remain")

        # Redirect non-authoritative LPA duplicates to the authoritative entity
        authoritative_duplicates = filter_authoritative_duplicates(
            old_entity, authoritative_duplicates, all_new_matches)
        all_new_matches = all_new_matches + authoritative_duplicates

        combined = combine_data(old_entity, all_new_matches)
        combined = resolve_redirect_chains(combined)
        check_old_entities_unique(combined)
        save_output(combined)
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == '__main__':
    main()

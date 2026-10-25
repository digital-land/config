#!/usr/bin/env python3
"""
Deduplicate conservation area geographies.

Processes duplicate geometry checks and generates old-entity redirects
for conservation areas with:
- Complete matches (100% geometry overlap)
- Single matches with high name similarity (>85%)

Each old-entity is written once. A complete match means identical geometry, so when
a Historic England entity completely matches several LPA entities it is redirected
(301) rather than ended, but only when it is clear which one is right: exactly one of
the matched entities is inside its own organisation's LPA boundary (the
count_lpa_boundary expectation, as flagged in the duplicate report by
reporting-task). The matched entities outside their boundary are copies, so they are
redirected to it as well.

If that cannot be told (several or none are inside, or the flag is missing), nothing
is written for that Historic England entity. Every other redirect is still written,
and the script then exits non-zero so the pipeline job fails and Slack is alerted.
Those cases need a manual decision: add a row to old-entity.csv by hand.
"""

import csv
import os
import sys
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
                'entity_a_name', 'entity_b_name', 'lookup_org_a', 'lookup_org_b', 'in_odp',
                'entity_a_out_of_bounds', 'entity_b_out_of_bounds'
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
                'entity_a_out_of_bounds': row.get('entity_b_out_of_bounds'),
                'entity_b_out_of_bounds': row.get('entity_a_out_of_bounds'),
            }
        else:
            continue

        key = (oriented['entity_a'], oriented['entity_b'])
        if key not in seen:
            seen.add(key)
            pairs.append(oriented)

    return pairs


def parse_flag(value):
    """Read a True/False flag from the duplicate report. Blank or missing is unknown (None)."""
    text = str(value if value is not None else '').strip().lower()
    if text == 'true':
        return True
    if text == 'false':
        return False
    return None


def choose_redirect_target(candidates):
    """Pick the entity to redirect to when one Historic England entity completely
    matches several LPA entities. Returns (chosen pair or None, reason).

    A complete match means identical geometry, so redirecting to a matching entity loses
    nothing, but only the one in the right place should be kept. The report flags each
    entity that is outside its own organisation's LPA boundary (entity_b_out_of_bounds).

    reason is:
    - 'only': a single match, nothing to choose.
    - 'in-bounds': every candidate's flag is known and exactly one is inside its boundary.
    - 'ambiguous': otherwise (several or no candidates inside, or a flag is missing). No
      choice is made. Guessing, for example by entity number, risks redirecting to the
      wrong organisation's copy.
    """
    if len(candidates) == 1:
        return candidates[0], 'only'

    flags = [parse_flag(c.get('entity_b_out_of_bounds')) for c in candidates]
    if all(flag is not None for flag in flags):
        in_bounds = [c for c, flag in zip(candidates, flags) if flag is False]
        if len(in_bounds) == 1:
            return in_bounds[0], 'in-bounds'

    return None, 'ambiguous'


def resolve_complete_matches(df):
    """Work out the redirects for complete matches.

    Returns (historic_england_rows, duplicate_rows, ambiguous):
    - historic_england_rows: one 301 row per Historic England entity that can be resolved.
    - duplicate_rows: 301 rows redirecting the matched entities that are outside their
      LPA boundary to the one that is inside, only where one was identified.
    - ambiguous: Historic England entities that match several LPA entities where it cannot
      be told which is right. Nothing is written for these.
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
    ambiguous = []

    for old_ent, rows in matches_by_entity.items():
        chosen, reason = choose_redirect_target(rows)

        if reason == 'ambiguous':
            ambiguous.append({
                'old-entity': old_ent,
                'matches': sorted(
                    ((r['entity_b'], r['lookup_org_b'], parse_flag(r.get('entity_b_out_of_bounds')))
                     for r in rows),
                    key=lambda m: int(m[0])
                ),
            })
            continue

        notes = f"Redirect old entity complete match to LPA entity: {chosen['lookup_org_b']}"
        if reason == 'in-bounds':
            notes += f" (the only one of {len(rows)} complete matches inside its LPA boundary)"

        historic_england_rows.append({
            'old-entity': old_ent,
            'status': '301',
            'entity': chosen['entity_b'],
            'end-date': '',
            'notes': notes,
            'entry-date': today,
            'start-date': ''
        })

        if reason == 'in-bounds':
            for other in rows:
                if other is not chosen and other['entity_b'] not in duplicates:
                    duplicates[other['entity_b']] = {
                        'old-entity': other['entity_b'],
                        'status': '301',
                        'entity': chosen['entity_b'],
                        'end-date': '',
                        'notes': f"Redirect duplicate outside its LPA boundary to LPA entity: {chosen['lookup_org_b']}",
                        'entry-date': today,
                        'start-date': ''
                    }

    print(f"{len(historic_england_rows)} Historic England entities to redirect")
    if duplicates:
        print(f"{len(duplicates)} duplicates outside their LPA boundary to redirect to the entity inside it")
    if ambiguous:
        print(f"{len(ambiguous)} Historic England entities cannot be resolved and will not be redirected")
    return historic_england_rows, list(duplicates.values()), ambiguous


def extract_complete_matches(df):
    """Complete match redirects for Historic England entities, one row per resolved entity."""
    return resolve_complete_matches(df)[0]


def report_ambiguous_matches(ambiguous):
    """Report Historic England entities that could not be resolved.

    Prints a GitHub Actions error and, when running in Actions, adds a table to the job
    summary. The caller then exits non-zero so the job fails and the pipeline's Slack alert
    fires. Nothing was written for these entities, so the committed data is safe, but they
    will be reported on every run until someone adds a row to old-entity.csv by hand.
    """
    if not ambiguous:
        return

    def describe(match):
        entity, org, out_of_bounds = match
        position = {True: 'outside its LPA boundary', False: 'inside its LPA boundary'}.get(
            out_of_bounds, 'boundary unknown')
        return f"{entity} ({org}, {position})"

    print(f"\n{len(ambiguous)} Historic England entities completely match several LPA entities and the "
          "LPA boundary check cannot tell which one is right. No redirect was written for them:")
    for item in ambiguous:
        print(f"  {item['old-entity']} matches " + "; ".join(describe(m) for m in item['matches']))
    print("Decide which entity is right and add a row to pipeline/conservation-area/old-entity.csv by hand. "
          "This step fails on every run until then.")
    print(f"::error title=CA duplicates need a manual decision::{len(ambiguous)} Historic England entities "
          "match several LPA entities and could not be resolved, see the job summary")

    summary_path = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary_path:
        with open(summary_path, 'a', encoding='utf-8') as f:
            f.write("### Conservation areas needing a manual decision\n\n")
            f.write("Each Historic England entity below completely matches several LPA entities and the "
                    "LPA boundary check cannot tell which one is right (it needs exactly one inside its "
                    "boundary). No redirect was written, so the data committed is safe, but this job fails "
                    "on every run until a row is added to `pipeline/conservation-area/old-entity.csv` "
                    "by hand.\n\n")
            f.write("| Historic England entity | Matches |\n|---|---|\n")
            for item in ambiguous:
                f.write(f"| {item['old-entity']} | " + "<br>".join(describe(m) for m in item['matches']) + " |\n")


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


def drop_superseded_single_matches(complete_matches, single_matches, ambiguous_entities=()):
    """Drop single matches for entities that already have a complete match.

    A complete match is identical geometry and always gives a clean redirect, so it
    wins over a partial match (which could otherwise add a second row, or a 410).
    Single matches are also dropped for entities left unresolved, so nothing is
    written for an entity that needs a manual decision.
    """
    skip = {m['old-entity'] for m in complete_matches} | set(ambiguous_entities)
    kept = [m for m in single_matches if m['old-entity'] not in skip]
    dropped = len(single_matches) - len(kept)
    if dropped:
        print(f"Dropped {dropped} single matches for entities that have a complete match or need a manual decision")
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


def filter_out_of_bounds_duplicates(old_entity, duplicates, new_matches):
    """Filter the redirects for duplicates outside their LPA boundary.

    These do not use filter_conflicting_matches: that skips any entity that is already
    the target of another redirect, but a duplicate is expected to have redirects
    pointing at it, and resolve_redirect_chains then repoints those at the entity inside
    the boundary. A duplicate is only skipped if:
    - it is already a source (it would create a duplicate row), or is a source in the
      new matches, or
    - the entity inside the boundary already redirects to it (it would create a loop).
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
    """Main execution function. Returns the process exit code."""
    try:
        checks_data = stream_checks_data()
        old_entity = load_old_entity()

        # Extract both complete and single matches
        complete_matches, out_of_bounds_duplicates, ambiguous = resolve_complete_matches(checks_data)
        single_matches = extract_single_matches(checks_data)

        # Combine both types of matches, preferring complete matches
        single_matches = drop_superseded_single_matches(
            complete_matches, single_matches, {a['old-entity'] for a in ambiguous})
        all_new_matches = complete_matches + single_matches
        print(f"\nTotal new redirects to add: {len(all_new_matches)}")

        # Filter out matches that would conflict with existing redirects
        all_new_matches = filter_conflicting_matches(old_entity, all_new_matches)
        print(f"After filtering conflicts: {len(all_new_matches)} matches remain")

        # Redirect duplicates outside their LPA boundary to the entity inside it
        out_of_bounds_duplicates = filter_out_of_bounds_duplicates(
            old_entity, out_of_bounds_duplicates, all_new_matches)
        all_new_matches = all_new_matches + out_of_bounds_duplicates

        combined = combine_data(old_entity, all_new_matches)
        combined = resolve_redirect_chains(combined)
        check_old_entities_unique(combined)
        save_output(combined)
    except Exception as e:
        print(f"Error: {e}")
        raise

    # Everything safe has been written. Fail the job if anything needs a manual decision
    report_ambiguous_matches(ambiguous)
    return 1 if ambiguous else 0


if __name__ == '__main__':
    sys.exit(main())

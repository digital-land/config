#!/usr/bin/env python3
"""
Redirect MHCLG-seeded plan data (local-plan, waste-plan, minerals-plan) and
plan-timetable data that duplicates data since provided by the LPA.

Unlike retire-mhclg-plan-data.py (which retires *fake template* placeholders,
identified structurally by a `{slug}-new-local-plan` reference), this script
targets *real* seeded data: rows MHCLG researched and entered on an LPA's
behalf (quality=some) before the LPA supplied their own authoritative data
(quality=authoritative). LPAs often rename or re-word plans when submitting,
so duplicates can't be spotted by reference or entity range - this compares
published content instead.

Two passes:

1. local-plan / waste-plan / minerals-plan: a seeded row is only redirected
   when it is an UNAMBIGUOUS duplicate of exactly one authoritative row from
   the same local planning authority - identical document-url, or identical
   name/description text (case-insensitive). If a seeded row matches more
   than one distinct authoritative row (e.g. an LPA reuses a generic name
   like "Local Plan" across an adopted and an emerging version), it is
   skipped and flagged for manual review rather than guessed at.

2. plan-timetable: once a plan reference is confirmed retired (by pass 1, or
   previously) - regardless of which of the three plan datasets it belongs
   to, since plan-timetable's `plan` column references all of them - only
   the seeded plan-timetable rows for that same plan that have an EXACT
   matching authoritative event (same organisation-entity, plan-event, and
   date) are redirected. Seeded milestones with no matching authoritative
   event are left alone - the plan being retired doesn't mean every
   milestone about it is known to be superseded. Dropping the date
   requirement was tried and rejected: plan-event labels (plan-adopted,
   submit-plan-for-examination, etc.) are a small vocabulary reused across a
   council's old and new plan cycles, so a date-less match can pair up two
   unrelated real-world events. Near-misses on date alone (same event, no
   exact date match) are logged as possible matches for manual review, but
   never auto-redirected.

Both passes use status 301 (redirect), with `entity` set to the matching
authoritative entity.

Each redirected entity is added to old-entity.csv with today's date.
"""

import csv
import io
import sys
import logging
import urllib.request
from collections import defaultdict
from datetime import date
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PIPELINE_DIR = REPO_ROOT / 'pipeline' / 'local-plan'
LOOKUP_PATH = PIPELINE_DIR / 'lookup.csv'
OLD_ENTITY_PATH = PIPELINE_DIR / 'old-entity.csv'

MHCLG_ORG = 'government-organisation:D1342'
FAKE_TEMPLATE_SUFFIX = '-new-local-plan'

# All managed in this same pipeline/local-plan/ lookup.csv and old-entity.csv,
# distinguished by `prefix`, and all published with the same quality=some
# (MHCLG-seeded) / quality=authoritative (LPA-submitted) structure.
PLAN_DATASET_URLS = {
    'local-plan': 'https://files.planning.data.gov.uk/dataset/local-plan.csv',
    'waste-plan': 'https://files.planning.data.gov.uk/dataset/waste-plan.csv',
    'minerals-plan': 'https://files.planning.data.gov.uk/dataset/minerals-plan.csv',
}
PUBLISHED_PLAN_TIMETABLE_URL = 'https://files.planning.data.gov.uk/dataset/plan-timetable.csv'

DATE_FIELDS = ['event-date', 'actual-date', 'start-date', 'predicted-date']


def read_csv_file(path):
    """Read a CSV file and return a list of dicts."""
    with open(path, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def fetch_csv(url):
    """Fetch a CSV from a URL and return a list of dicts."""
    with urllib.request.urlopen(url) as response:
        content = response.read().decode('utf-8')
    return list(csv.DictReader(io.StringIO(content)))


def label(row):
    """Best-effort human-readable text for a row, for duplicate comparison."""
    return (row.get('name') or row.get('description') or '').strip().lower()


def event_date(row):
    """First populated date field on a plan-timetable row."""
    for field in DATE_FIELDS:
        if row.get(field):
            return row[field]
    return ''


# --- Pass 1: local-plan / waste-plan / minerals-plan -----------------------

def find_seeded_plan_entities(lookup_rows, prefix):
    """Find MHCLG-submitted entities of the given plan prefix that are real
    seeded data.

    Excludes fake template placeholders (identified by the {slug}-new-local-plan
    reference pattern used elsewhere in this repo - only ever seen on
    local-plan, but harmless to check for all three), leaving only entities
    MHCLG researched and entered with real plan content on the LPA's behalf.
    """
    seeded = {}
    for row in lookup_rows:
        if row['organisation'] != MHCLG_ORG or row['prefix'] != prefix:
            continue
        if row['reference'].endswith(FAKE_TEMPLATE_SUFFIX):
            continue
        seeded[int(row['entity'])] = row['reference']
    return seeded


def find_plan_duplicates(seeded_entities, published_by_entity, entity_org_from_lookup):
    """Match seeded (quality=some) rows to authoritative rows from the same LPA.

    Returns (confirmed, ambiguous, skipped):
    - confirmed: {entity: (org_entity, matched_entity, match_type)} - safe to redirect
    - ambiguous: {entity: [matched_entity, ...]} - multiple distinct candidates, needs a human
    - skipped: {entity: reason} - not comparable (missing/changed/no org) or no match found
    """
    authoritative_by_org_entity = defaultdict(list)
    for row in published_by_entity.values():
        if row.get('quality') != 'authoritative':
            continue
        org_entity = row.get('organisation-entity', '').strip()
        if not org_entity:
            continue
        submitting_org = entity_org_from_lookup.get(int(row['entity']))
        if submitting_org == MHCLG_ORG:
            continue
        authoritative_by_org_entity[org_entity].append(row)

    confirmed = {}
    ambiguous = {}
    skipped = {}

    for entity, reference in seeded_entities.items():
        row = published_by_entity.get(entity)
        if row is None:
            skipped[entity] = 'not found in published dataset (already retired?)'
            continue
        if row.get('quality') != 'some':
            skipped[entity] = f"quality is '{row.get('quality')}', not 'some'"
            continue

        org_entity = row.get('organisation-entity', '').strip()
        if not org_entity:
            skipped[entity] = 'no organisation-entity to match against'
            continue

        candidates = authoritative_by_org_entity.get(org_entity, [])
        if not candidates:
            skipped[entity] = 'no authoritative data yet'
            continue

        seeded_url = row.get('document-url', '').strip()
        seeded_label = label(row)

        matched_entities = {}
        for candidate in candidates:
            candidate_url = candidate.get('document-url', '').strip()
            candidate_label = label(candidate)

            url_match = bool(seeded_url) and seeded_url == candidate_url
            name_match = bool(seeded_label) and seeded_label == candidate_label

            if url_match or name_match:
                match_type = 'url+name' if (url_match and name_match) else ('url' if url_match else 'name')
                matched_entities[int(candidate['entity'])] = match_type

        if not matched_entities:
            skipped[entity] = 'no matching authoritative row'
        elif len(matched_entities) == 1:
            matched_entity, match_type = next(iter(matched_entities.items()))
            confirmed[entity] = (org_entity, matched_entity, match_type)
        else:
            ambiguous[entity] = sorted(matched_entities.keys())

    return confirmed, ambiguous, skipped


# --- Pass 2: plan-timetable ------------------------------------------------

def find_retired_plan_references(old_entity_rows, lookup_rows, prefixes, extra_references=()):
    """Find plan references already retired as MHCLG-seeded duplicates.

    Combines references already in old-entity.csv (from a previous run, or
    retired for any other reason) with `extra_references` just confirmed in
    this run. Excludes fake template placeholders - only real seeded data
    retired as a content duplicate is a valid base for cascading. Covers all
    of `prefixes` (local-plan, waste-plan, minerals-plan) since
    plan-timetable's `plan` column references all three.
    """
    retired_entities = set(int(r['old-entity']) for r in old_entity_rows)
    reference_by_entity = {
        int(r['entity']): r['reference']
        for r in lookup_rows
        if r['prefix'] in prefixes and r['organisation'] == MHCLG_ORG
    }

    references = set(extra_references)
    for entity in retired_entities:
        reference = reference_by_entity.get(entity)
        if reference and not reference.endswith(FAKE_TEMPLATE_SUFFIX):
            references.add(reference)
    return references


def find_matching_timetable_events(retired_references, lookup_rows, old_entity_rows, published_by_entity):
    """Find seeded plan-timetable rows with an exact matching authoritative event.

    Scoped to plan-timetable rows belonging to a plan already confirmed
    retired (via its `plan` column, matched exactly - not a `reference`
    string prefix, which can also match an unrelated plan for the same
    council when a retired reference has no year/version suffix). Within
    that scope, only rows with an exact (organisation-entity, plan-event,
    date) match to an authoritative row are redirected; everything else is
    left alone, since a retired plan doesn't imply every milestone about it
    is known to be superseded.

    Returns (confirmed, ambiguous):
    - confirmed: {entity: matched_authoritative_entity} - safe to redirect
    - ambiguous: {entity: [matched_entity, ...]} - more than one equally
      specific authoritative match, needs a human
    """
    already_retired = set(int(r['old-entity']) for r in old_entity_rows)

    authoritative_by_org_entity = defaultdict(list)
    for row in published_by_entity.values():
        if row.get('quality') != 'authoritative':
            continue
        org_entity = row.get('organisation-entity', '').strip()
        if org_entity:
            authoritative_by_org_entity[org_entity].append(row)

    confirmed = {}
    ambiguous = {}
    for row in lookup_rows:
        if row['prefix'] != 'plan-timetable' or row['organisation'] != MHCLG_ORG:
            continue
        entity = int(row['entity'])
        if entity in already_retired:
            continue

        published_row = published_by_entity.get(entity)
        if published_row is None:
            continue
        if published_row.get('quality') != 'some':
            continue
        if published_row.get('plan', '').strip() not in retired_references:
            continue

        org_entity = published_row.get('organisation-entity', '').strip()
        plan_event = published_row.get('plan-event', '').strip()
        date_value = event_date(published_row)
        if not (org_entity and plan_event and date_value):
            continue

        matched_ids = sorted(set(
            int(candidate['entity'])
            for candidate in authoritative_by_org_entity.get(org_entity, [])
            if candidate.get('plan-event', '').strip() == plan_event
            and event_date(candidate) == date_value
        ))

        if not matched_ids:
            continue
        if len(matched_ids) > 1:
            ambiguous[entity] = matched_ids
            continue
        confirmed[entity] = matched_ids[0]

    return confirmed, ambiguous


def find_possible_timetable_matches(retired_references, lookup_rows, old_entity_rows,
                                     published_by_entity, already_matched):
    """Loosely match (organisation-entity, plan-event) ignoring date, for rows
    the strict match left alone. Logging only - never redirected automatically,
    since dropping the date requirement is known to produce false positives
    (the same plan-event label is reused across a council's old and new plan
    cycles, so a date-less match doesn't mean the same real-world event).
    Surfaced so a human can manually confirm and redirect genuine near-misses
    (e.g. a milestone resubmitted a day or two off its original date).

    Returns {entity: [(candidate_entity, candidate_date), ...]}.
    """
    already_retired = set(int(r['old-entity']) for r in old_entity_rows)

    authoritative_by_org_entity = defaultdict(list)
    for row in published_by_entity.values():
        if row.get('quality') != 'authoritative':
            continue
        org_entity = row.get('organisation-entity', '').strip()
        if org_entity:
            authoritative_by_org_entity[org_entity].append(row)

    possible = {}
    for row in lookup_rows:
        if row['prefix'] != 'plan-timetable' or row['organisation'] != MHCLG_ORG:
            continue
        entity = int(row['entity'])
        if entity in already_retired or entity in already_matched:
            continue

        published_row = published_by_entity.get(entity)
        if published_row is None:
            continue
        if published_row.get('quality') != 'some':
            continue
        if published_row.get('plan', '').strip() not in retired_references:
            continue

        org_entity = published_row.get('organisation-entity', '').strip()
        plan_event = published_row.get('plan-event', '').strip()
        if not (org_entity and plan_event):
            continue

        candidates = [
            (int(candidate['entity']), event_date(candidate))
            for candidate in authoritative_by_org_entity.get(org_entity, [])
            if candidate.get('plan-event', '').strip() == plan_event
        ]
        if candidates:
            possible[entity] = candidates

    return possible


# --- Saving ----------------------------------------------------------------

def save_redirected_entities(records, old_entity_rows):
    """Append redirect records to old-entity.csv.

    `records` is a list of (entity, target_entity, notes) tuples; all are
    written with status=301 since we only ever redirect to a known successor.
    """
    logger.info(f"\n=== Saving {len(records)} entities to old-entity.csv ===")

    if not records:
        logger.warning("No entities to redirect")
        return []

    existing = set(int(r['old-entity']) for r in old_entity_rows)
    to_add = [r for r in records if r[0] not in existing]
    duplicates = len(records) - len(to_add)
    if duplicates:
        logger.warning(f"⚠ {duplicates} entities are already in old-entity.csv (skipping)")

    if not to_add:
        logger.info("No new entities to add")
        return []

    fieldnames = ['old-entity', 'status', 'entity', 'notes', 'end-date', 'entry-date', 'start-date']
    with open(OLD_ENTITY_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        for entity_id, target_entity, notes in sorted(to_add):
            writer.writerow({
                'old-entity': entity_id,
                'status': 301,
                'entity': target_entity,
                'notes': notes,
                'end-date': '',
                'entry-date': date.today().isoformat(),
                'start-date': '',
            })

    logger.info(f"✓ Added {len(to_add)} rows to old-entity.csv")
    return to_add


def main():
    for path in [LOOKUP_PATH, OLD_ENTITY_PATH]:
        if not path.exists():
            logger.error(f"Required file not found: {path}")
            sys.exit(1)

    logger.info("Loading CSV files...")
    lookup_rows = read_csv_file(LOOKUP_PATH)
    old_entity_rows = read_csv_file(OLD_ENTITY_PATH)
    logger.info(f"Loaded lookup.csv ({len(lookup_rows)} rows)")
    logger.info(f"Loaded old-entity.csv ({len(old_entity_rows)} rows)")

    # Not filtered by prefix - entity ids are unique across datasets, and this
    # is reused below to look up the submitting LPA for every redirect target
    # when building old-entity.csv notes.
    entity_org_from_lookup = {
        int(r['entity']): r['organisation']
        for r in lookup_rows
    }

    # --- Pass 1: local-plan / waste-plan / minerals-plan ---
    plan_records = []
    seeded_by_prefix = {}
    published_by_prefix = {}
    confirmed_by_prefix = {}
    ambiguous_by_prefix = {}
    extra_references = set()

    for prefix in PLAN_DATASET_URLS:
        logger.info(f"\n=== Pass 1: {prefix} ===")
        seeded = find_seeded_plan_entities(lookup_rows, prefix)
        logger.info(f"Found {len(seeded)} MHCLG-seeded {prefix} entities")

        logger.info(f"Fetching published {prefix}.csv...")
        published = {int(r['entity']): r for r in fetch_csv(PLAN_DATASET_URLS[prefix])}
        logger.info(f"Loaded {len(published)} published {prefix} rows")

        confirmed, ambiguous, skipped = find_plan_duplicates(seeded, published, entity_org_from_lookup)
        logger.info(f"Confirmed duplicates: {len(confirmed)}")
        logger.info(f"Ambiguous (needs manual review): {len(ambiguous)}")
        logger.info(f"No match / not comparable: {len(skipped)}")

        if ambiguous:
            logger.warning(f"\nAmbiguous {prefix} matches skipped (multiple candidate authoritative rows):")
            for entity, candidates in sorted(ambiguous.items()):
                logger.warning(f"  entity {entity} ({seeded[entity]}) -> candidates {candidates}")

        for entity, (org_entity, matched_entity, _match_type) in confirmed.items():
            plan_records.append((
                entity,
                matched_entity,
                f"Redirecting MHCLG data to authoritative entity "
                f"({entity_org_from_lookup.get(matched_entity, 'unknown')})-{prefix}",
            ))

        seeded_by_prefix[prefix] = seeded
        published_by_prefix[prefix] = published
        confirmed_by_prefix[prefix] = confirmed
        ambiguous_by_prefix[prefix] = ambiguous
        extra_references |= {seeded[e] for e in confirmed}

    # --- Pass 2: plan-timetable ---
    logger.info("\n=== Pass 2: plan-timetable ===")
    retired_references = find_retired_plan_references(
        old_entity_rows, lookup_rows, PLAN_DATASET_URLS.keys(), extra_references=extra_references)
    logger.info(f"Found {len(retired_references)} retired plan references to match against")

    confirmed_tt = {}
    ambiguous_tt = {}
    possible_tt = {}
    timetable_published = {}
    if retired_references:
        logger.info("Fetching published plan-timetable.csv...")
        timetable_published = {int(r['entity']): r for r in fetch_csv(PUBLISHED_PLAN_TIMETABLE_URL)}
        logger.info(f"Loaded {len(timetable_published)} published plan-timetable rows")

        confirmed_tt, ambiguous_tt = find_matching_timetable_events(
            retired_references, lookup_rows, old_entity_rows, timetable_published)
        logger.info(f"Confirmed matching events: {len(confirmed_tt)}")
        logger.info(f"Ambiguous (needs manual review): {len(ambiguous_tt)}")

        if ambiguous_tt:
            logger.warning("\nAmbiguous plan-timetable matches skipped (multiple candidate authoritative events):")
            for entity, candidates in sorted(ambiguous_tt.items()):
                logger.warning(f"  entity {entity} -> candidates {candidates}")

        already_matched = set(confirmed_tt) | set(ambiguous_tt)
        possible_tt = find_possible_timetable_matches(
            retired_references, lookup_rows, old_entity_rows, timetable_published, already_matched)
        logger.info(f"Possible matches, date differs (needs manual review): {len(possible_tt)}")

        if possible_tt:
            logger.warning(
                "\nPossible plan-timetable matches - same event, no exact date match "
                "(logged only, not redirected):")
            for entity, candidates in sorted(possible_tt.items()):
                own_date = event_date(timetable_published[entity])
                candidate_str = ', '.join(f"{c} ({d or 'no date'})" for c, d in candidates)
                logger.warning(f"  entity {entity} (date {own_date or 'none'}) -> candidates {candidate_str}")

    timetable_records = [
        (
            entity,
            matched_entity,
            f"Redirecting MHCLG data to authoritative entity "
            f"({entity_org_from_lookup.get(matched_entity, 'unknown')})-plan-timetable",
        )
        for entity, matched_entity in confirmed_tt.items()
    ]

    # --- Save ---
    all_records = plan_records + timetable_records
    entities_added = save_redirected_entities(all_records, old_entity_rows)
    if not entities_added and not possible_tt:
        logger.warning("No entities redirected")
        sys.exit(0)

    added_by_entity = {e: (target, notes) for e, target, notes in entities_added}

    print(f"Total entities redirected: {len(entities_added)}")
    print("")

    for prefix in PLAN_DATASET_URLS:
        confirmed = confirmed_by_prefix[prefix]
        seeded = seeded_by_prefix[prefix]
        published = published_by_prefix[prefix]
        added = {e: v for e, v in added_by_entity.items() if e in confirmed}
        if added:
            print(f"**{prefix} seeded duplicates** ({len(added)} entities):")
            for entity_id in sorted(added):
                target, _ = added[entity_id]
                row = published.get(entity_id, {})
                name = row.get('name') or row.get('description') or seeded[entity_id]
                print(f"- entity {entity_id} ({name}) -> 301 to entity {target}")
            print("")

    tt_added = {e: v for e, v in added_by_entity.items() if e in confirmed_tt}
    if tt_added:
        print(f"**plan-timetable matching events** ({len(tt_added)} entities):")
        for entity_id in sorted(tt_added):
            target, _ = tt_added[entity_id]
            row = timetable_published.get(entity_id, {})
            plan_event = row.get('plan-event', '')
            print(f"- entity {entity_id} ({plan_event}) -> 301 to entity {target}")
        print("")

    for prefix in PLAN_DATASET_URLS:
        ambiguous = ambiguous_by_prefix[prefix]
        if ambiguous:
            seeded = seeded_by_prefix[prefix]
            published = published_by_prefix[prefix]
            print(f"**{prefix} needs manual review** ({len(ambiguous)} entities matched more than one candidate):")
            for entity, candidates in sorted(ambiguous.items()):
                row = published.get(entity, {})
                name = row.get('name') or row.get('description') or seeded[entity]
                print(f"- entity {entity} ({name}) -> candidates {candidates}")
            print("")

    if ambiguous_tt:
        print(f"**plan-timetable needs manual review** ({len(ambiguous_tt)} entities matched more than one candidate):")
        for entity, candidates in sorted(ambiguous_tt.items()):
            print(f"- entity {entity} -> candidates {candidates}")
        print("")

    if possible_tt:
        print(
            f"**plan-timetable possible matches, date differs** ({len(possible_tt)} entities - "
            "same event, no exact date match, not redirected):")
        for entity, candidates in sorted(possible_tt.items()):
            own_date = event_date(timetable_published[entity])
            candidate_str = ', '.join(f"{c} ({d or 'no date'})" for c, d in candidates)
            print(f"- entity {entity} (date {own_date or 'none'}) -> candidates {candidate_str}")
        print("")

    logger.info("\n✓ Redirection completed successfully")


if __name__ == '__main__':
    main()

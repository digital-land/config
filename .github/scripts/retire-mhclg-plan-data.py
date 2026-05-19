#!/usr/bin/env python3
"""
Retire fake MHCLG template data for local plans and plan timetables.

When LPAs provide their own data, this script removes the pre-seeded MHCLG data
that was added as placeholders. MHCLG seeded data is identified by:
- Entity ranges of 23 entities (difference of 22) for plan timetables
- Reference matching {slug}-new-local-plan for local plans

Each retired entity is added to old-entity.csv with status 410 and today's date.

Datasets processed:
- local-plan
- plan-timetable
"""

import csv
import io
import urllib.request
import sys
import logging
from datetime import date
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PIPELINE_DIR = REPO_ROOT / 'pipeline' / 'local-plan'
LOOKUP_PATH = PIPELINE_DIR / 'lookup.csv'
ENTITY_ORG_PATH = PIPELINE_DIR / 'entity-organisation.csv'
OLD_ENTITY_PATH = PIPELINE_DIR / 'old-entity.csv'

MHCLG_ORG = 'government-organisation:D1342'

# Threshold entity numbers: MHCLG fake template data ends at these values
MHCLG_THRESHOLDS = {
    'plan-timetable': 5109686,
    'local-plan': 4220966,
}

# Entity range for MHCLG seeded plan-timetable data (inclusive difference = 22, count = 23)
MHCLG_ENTITY_RANGE = 22


def read_csv_file(path):
    """Read a CSV file and return a list of dicts."""
    with open(path, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def authority_to_slug(name):
    """Convert authority name to slug format."""
    if not name:
        return ''
    slug = name.lower().strip()
    slug = slug.replace('&', 'and')
    slug = slug.replace('–', '-').replace('—', '-').replace('/', '-')
    slug = slug.replace(' ', '-')
    slug = ''.join(c for c in slug if c.isalnum() or c == '-')
    while '--' in slug:
        slug = slug.replace('--', '-')
    return slug.strip('-')


def load_organisation_mapping():
    """Load mapping from organisation CURIE codes to names."""
    logger.info("Loading organisation mapping from planning.data.gov.uk...")
    url = 'https://files.planning.data.gov.uk/organisation-collection/dataset/organisation.csv'
    with urllib.request.urlopen(url) as response:
        content = response.read().decode('utf-8')

    reader = csv.DictReader(io.StringIO(content))
    mapping = {}
    for row in reader:
        mapping[row['organisation']] = row['name']

    logger.info(f"Loaded {len(mapping)} organisations")
    return mapping


def retire_plan_timetable_data(lookup_rows, entity_org_rows):
    """Retire MHCLG seeded data for plan-timetable dataset. Returns set of entities."""
    logger.info("\n=== Processing plan-timetable dataset ===")

    threshold = MHCLG_THRESHOLDS['plan-timetable']
    prefix = 'plan-timetable'

    # Step 1: Find LPAs that provided data
    lpa_rows = [
        r for r in lookup_rows
        if r['organisation'] != MHCLG_ORG and r['prefix'] == prefix
    ]

    if not lpa_rows:
        logger.warning(f"No LPA data found for {prefix}")
        return set()

    # Validate: all LPA entities should be above threshold
    lpa_entities = [int(r['entity']) for r in lpa_rows]
    min_lpa = min(lpa_entities)
    max_lpa = max(lpa_entities)
    logger.info(f"LPA entity range: {min_lpa} - {max_lpa}")

    if min_lpa <= threshold:
        raise ValueError(
            f"ERROR: Some LPA entities are not above threshold ({threshold}). "
            f"Found {min_lpa}. This indicates corrupted or invalid data."
        )
    logger.info(f"✓ All LPA entities > {threshold}")

    # Step 2: Get min/max entity per organisation from LPA data
    org_ranges = {}
    for row in lpa_rows:
        org = row['organisation']
        entity = int(row['entity'])
        if org not in org_ranges:
            org_ranges[org] = {'min': entity, 'max': entity}
        else:
            org_ranges[org]['min'] = min(org_ranges[org]['min'], entity)
            org_ranges[org]['max'] = max(org_ranges[org]['max'], entity)

    logger.info(f"Found {len(org_ranges)} LPAs with authoritative data")

    # Step 3: Filter entity-organisation to this dataset and orgs with data
    entity_org_filtered = [
        r for r in entity_org_rows
        if r['dataset'] == prefix and r['organisation'] in org_ranges
    ]

    # Step 4: Anti-join - find ranges NOT matching the LPA's authoritative data
    lpa_range_keys = {
        (org, data['min'], data['max'])
        for org, data in org_ranges.items()
    }

    mhclg_to_retire = []
    for row in entity_org_filtered:
        org = row['organisation']
        entity_min = int(row['entity-minimum'])
        entity_max = int(row['entity-maximum'])
        if (org, entity_min, entity_max) not in lpa_range_keys:
            mhclg_to_retire.append((org, entity_min, entity_max))

    # Step 5: Filter to ranges with exactly 23 entities (MHCLG template size)
    mhclg_to_retire = [
        (org, emin, emax) for org, emin, emax in mhclg_to_retire
        if (emax - emin) == MHCLG_ENTITY_RANGE
    ]
    logger.info(f"Found {len(mhclg_to_retire)} MHCLG-seeded entity ranges to retire")

    if not mhclg_to_retire:
        logger.warning("No MHCLG entity ranges to retire for plan-timetable")
        return set()

    # Step 6: Expand ranges to individual entities and verify they are MHCLG
    entities_to_retire = set()

    for org, entity_min, entity_max in mhclg_to_retire:
        mhclg_entities = [
            int(r['entity']) for r in lookup_rows
            if r['organisation'] == MHCLG_ORG
            and r['prefix'] == prefix
            and int(r['entity']) >= entity_min
            and int(r['entity']) <= entity_max
        ]

        expected_count = entity_max - entity_min + 1
        if len(mhclg_entities) != expected_count:
            logger.warning(
                f"  {org}: Expected {expected_count} entities in range "
                f"[{entity_min}, {entity_max}], found {len(mhclg_entities)}"
            )

        entities_to_retire.update(mhclg_entities)

    logger.info(f"✓ Verified and queued {len(entities_to_retire)} plan-timetable entities for retirement")
    return entities_to_retire


def retire_local_plan_data(lookup_rows, entity_org_rows):
    """Retire MHCLG seeded data for local-plan dataset. Returns set of entities."""
    logger.info("\n=== Processing local-plan dataset ===")

    threshold = MHCLG_THRESHOLDS['local-plan']
    prefix = 'local-plan'

    # Step 1: Find LPAs that provided data
    lpa_rows = [
        r for r in lookup_rows
        if r['organisation'] != MHCLG_ORG and r['prefix'] == prefix
    ]

    if not lpa_rows:
        logger.warning(f"No LPA data found for {prefix}")
        return set()

    # Validate: all LPA entities should be above threshold
    lpa_entities = [int(r['entity']) for r in lpa_rows]
    min_lpa = min(lpa_entities)
    max_lpa = max(lpa_entities)
    logger.info(f"LPA entity range: {min_lpa} - {max_lpa}")

    if min_lpa <= threshold:
        raise ValueError(
            f"ERROR: Some LPA entities are not above threshold ({threshold}). "
            f"Found {min_lpa}. This indicates corrupted or invalid data."
        )
    logger.info(f"✓ All LPA entities > {threshold}")

    # Step 2: Load organisation mapping to generate fake plan references
    org_mapping = load_organisation_mapping()
    lpa_orgs = set(r['organisation'] for r in lpa_rows)

    # Validation #4: Fail if any LPA org name could not be resolved
    unresolved_orgs = [org for org in lpa_orgs if org not in org_mapping]
    if unresolved_orgs:
        raise ValueError(
            f"ERROR: Could not resolve organisation names for: {', '.join(unresolved_orgs)}. "
            "Cannot generate fake plan references without names."
        )

    fake_plan_references = {}
    for org in lpa_orgs:
        slug = authority_to_slug(org_mapping[org])
        reference = f"{slug}-new-local-plan"
        fake_plan_references[org] = reference
        logger.info(f"  {org}: {reference}")

    logger.info(f"Generated {len(fake_plan_references)} fake plan references")

    # Step 3: Find MHCLG entities that match these fake plan references
    mhclg_entities = set()
    for row in lookup_rows:
        if (row['organisation'] == MHCLG_ORG
                and row['prefix'] == prefix
                and row['reference'] in fake_plan_references.values()):
            entity = int(row['entity'])
            if entity > threshold:
                raise ValueError(
                    f"ERROR: Found MHCLG entity {entity} above threshold {threshold}. "
                    "This indicates data corruption."
                )
            mhclg_entities.add(entity)

    logger.info(f"Found {len(mhclg_entities)} MHCLG local plan entities to retire")

    # Validation #3: Cross-check each entity falls within an entity-organisation range for its LPA
    entity_org_ranges = [
        (r['organisation'], int(r['entity-minimum']), int(r['entity-maximum']))
        for r in entity_org_rows
        if r['dataset'] == prefix
    ]

    for entity in mhclg_entities:
        # Find which LPA this entity's reference belongs to
        reference = None
        for row in lookup_rows:
            if (row['prefix'] == prefix
                    and int(row['entity']) == entity
                    and row['organisation'] == MHCLG_ORG):
                reference = row['reference']
                break

        # Find the LPA org that generated this reference
        lpa_org = None
        for org, ref in fake_plan_references.items():
            if ref == reference:
                lpa_org = org
                break

        if not lpa_org:
            raise ValueError(
                f"ERROR: Entity {entity} (ref={reference}) does not map back to any LPA organisation."
            )

        # Verify entity falls within an entity-organisation range for this LPA
        in_range = any(
            org == lpa_org and emin <= entity <= emax
            for org, emin, emax in entity_org_ranges
        )
        if not in_range:
            raise ValueError(
                f"ERROR: Entity {entity} (ref={reference}) does not fall within any "
                f"entity-organisation range for {lpa_org}."
            )

    logger.info(f"✓ All entities cross-checked against entity-organisation.csv")
    logger.info(f"✓ Queued {len(mhclg_entities)} local-plan entities for retirement")
    return mhclg_entities


def save_retired_entities(entities_to_retire, old_entity_rows):
    """Append retired entities to old-entity.csv."""
    logger.info(f"\n=== Saving {len(entities_to_retire)} entities to old-entity.csv ===")

    if not entities_to_retire:
        logger.warning("No entities to retire")
        return

    # Check for duplicates (entities already retired)
    existing = set(int(r['old-entity']) for r in old_entity_rows)
    duplicates = entities_to_retire & existing

    if duplicates:
        logger.warning(f"⚠ {len(duplicates)} entities are already in old-entity.csv (skipping)")
        entities_to_add = entities_to_retire - duplicates
    else:
        entities_to_add = entities_to_retire

    if not entities_to_add:
        logger.info("No new entities to add")
        return

    # Read existing file content and append new rows
    fieldnames = ['old-entity', 'status', 'entity', 'notes', 'end-date', 'entry-date', 'start-date']

    with open(OLD_ENTITY_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        for entity_id in sorted(entities_to_add):
            writer.writerow({
                'old-entity': entity_id,
                'status': 410,
                'entity': '',
                'notes': 'Retiring fake MHCLG template data',
                'end-date': '',
                'entry-date': date.today().isoformat(),
                'start-date': '',
            })

    logger.info(f"✓ Added {len(entities_to_add)} rows to old-entity.csv")
    logger.info(f"  Total old-entity entries: {len(old_entity_rows) + len(entities_to_add)}")


def main():
    """Main entry point."""
    # Validate files exist
    for path in [LOOKUP_PATH, ENTITY_ORG_PATH, OLD_ENTITY_PATH]:
        if not path.exists():
            logger.error(f"Required file not found: {path}")
            sys.exit(1)

    logger.info("Loading CSV files...")
    lookup_rows = read_csv_file(LOOKUP_PATH)
    entity_org_rows = read_csv_file(ENTITY_ORG_PATH)
    old_entity_rows = read_csv_file(OLD_ENTITY_PATH)
    logger.info(f"Loaded lookup.csv ({len(lookup_rows)} rows)")
    logger.info(f"Loaded entity-organisation.csv ({len(entity_org_rows)} rows)")
    logger.info(f"Loaded old-entity.csv ({len(old_entity_rows)} rows)")

    timetable_entities = retire_plan_timetable_data(lookup_rows, entity_org_rows)
    local_plan_entities = retire_local_plan_data(lookup_rows, entity_org_rows)

    all_entities = timetable_entities | local_plan_entities

    logger.info(f"\n=== Summary ===")
    logger.info(f"plan-timetable: {len(timetable_entities)} entities")
    logger.info(f"local-plan: {len(local_plan_entities)} entities")
    logger.info(f"Total: {len(all_entities)} entities")

    if not all_entities:
        logger.warning("No entities to retire")
        sys.exit(0)

    save_retired_entities(all_entities, old_entity_rows)
    logger.info("\n✓ Retirement completed successfully")


if __name__ == '__main__':
    main()

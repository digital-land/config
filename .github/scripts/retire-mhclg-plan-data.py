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

# Entity ranges for MHCLG fake template data
MHCLG_RANGES = {
    'plan-timetable': (5101702, 5109686),
    'local-plan': (4220656, 4220966),
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


def constituent_orgs(org, group_constituents):
    """Resolve a local-planning-group joint-committee org to its constituent authorities.

    MHCLG seeded fake template data per individual constituent authority, not under
    the joint group's own code, so retiring a joint group's new data may require
    retiring each constituent's template. Non-group orgs are returned unchanged.
    """
    return set(group_constituents.get(org, [org]))


def fetch_organisation_data():
    """Load organisation names and joint-group membership from planning.data.gov.uk.

    Returns (name_by_org, group_constituents): a mapping of organisation CURIE to
    name, and a mapping of local-planning-group CURIE to its constituent authority
    CURIEs (from the 'organisations' column, which is authoritative — constituents
    aren't always local authorities, some are national park authorities or
    development corporations, so this can't be inferred from the code alone).
    """
    logger.info("Loading organisation mapping from planning.data.gov.uk...")
    url = 'https://files.planning.data.gov.uk/organisation-collection/dataset/organisation.csv'
    with urllib.request.urlopen(url) as response:
        content = response.read().decode('utf-8')

    reader = csv.DictReader(io.StringIO(content))
    name_by_org = {}
    group_constituents = {}
    for row in reader:
        name_by_org[row['organisation']] = row['name']
        constituents = row.get('organisations', '')
        if constituents:
            group_constituents[row['organisation']] = [
                c.strip() for c in constituents.split(';') if c.strip()
            ]

    logger.info(f"Loaded {len(name_by_org)} organisations ({len(group_constituents)} joint groups)")
    return name_by_org, group_constituents


def retire_plan_timetable_data(lookup_rows, entity_org_rows, group_constituents):
    """Retire MHCLG seeded data for plan-timetable dataset. Returns (entities, orgs)."""
    logger.info("\n=== Processing plan-timetable dataset ===")

    mhclg_range_min, mhclg_range_max = MHCLG_RANGES['plan-timetable']
    prefix = 'plan-timetable'

    # Step 1: Find LPAs that provided data
    all_lpa_rows = [
        r for r in lookup_rows
        if r['organisation'] != MHCLG_ORG and r['prefix'] == prefix
    ]

    if not all_lpa_rows:
        logger.warning(f"No LPA data found for {prefix}")
        return set(), set()

    # Separate LPAs that created new data (outside MHCLG range) from those that
    # updated MHCLG data in-place (within MHCLG range). In-place updates don't
    # need retirement since the MHCLG data was overwritten, not duplicated.
    def is_in_mhclg_range(entity):
        return mhclg_range_min <= entity <= mhclg_range_max

    lpa_rows = [r for r in all_lpa_rows if not is_in_mhclg_range(int(r['entity']))]

    updated_in_place_orgs = set(
        r['organisation'] for r in all_lpa_rows if is_in_mhclg_range(int(r['entity']))
    ) - set(r['organisation'] for r in lpa_rows)

    if updated_in_place_orgs:
        logger.info(
            f"Skipping {len(updated_in_place_orgs)} LPAs that updated MHCLG data in-place "
            f"(no retirement needed): {', '.join(sorted(updated_in_place_orgs))}"
        )

    if not lpa_rows:
        logger.info("No LPAs with new data outside MHCLG range — nothing to retire")
        return set(), set()

    min_lpa = min(int(r['entity']) for r in lpa_rows)
    max_lpa = max(int(r['entity']) for r in lpa_rows)
    logger.info(f"LPA entity range: {min_lpa} - {max_lpa}")
    logger.info(f"✓ All LPA entities outside MHCLG range ({mhclg_range_min}-{mhclg_range_max})")

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

    logger.info(f"Found {len(org_ranges)} LPAs with new authoritative data")

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

    # Completeness check: every LPA with data should have a MHCLG template range.
    # Joint local-planning-group orgs submit data under a combined code, but MHCLG
    # seeded fake data per constituent authority under the authority's own code. If
    # every constituent is already covered — by its own direct submission above, or
    # by a template found here — the group itself doesn't need one of its own.
    orgs_with_retirement = set(org for org, _, _ in mhclg_to_retire)
    orgs_missing = set(org_ranges.keys()) - orgs_with_retirement

    still_missing = set()
    for org in sorted(orgs_missing):
        constituents = constituent_orgs(org, group_constituents)
        if constituents == {org}:
            still_missing.add(org)
            continue

        all_covered = True
        for constituent in constituents:
            if constituent in orgs_with_retirement:
                continue
            match = next(
                (r for r in entity_org_rows
                 if r['dataset'] == prefix and r['organisation'] == constituent
                 and int(r['entity-maximum']) - int(r['entity-minimum']) == MHCLG_ENTITY_RANGE),
                None
            )
            if match:
                mhclg_to_retire.append((org, int(match['entity-minimum']), int(match['entity-maximum'])))
            else:
                all_covered = False

        if all_covered:
            orgs_with_retirement.add(org)
        else:
            still_missing.add(org)

    orgs_missing = still_missing
    if orgs_missing:
        raise ValueError(
            f"ERROR: No MHCLG template range found for: {', '.join(sorted(orgs_missing))}. "
            "These LPAs provided data but no fake template was identified to retire."
        )
    logger.info(f"✓ All {len(org_ranges)} LPAs have a matching MHCLG template range")

    if not mhclg_to_retire:
        logger.info("No MHCLG entity ranges to retire for plan-timetable")
        return set(), set()

    # Step 6: Expand ranges to individual entities and verify they are MHCLG
    entity_to_org = {}

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

        for e in mhclg_entities:
            entity_to_org[e] = (org, prefix)

    # No-overlap check: ensure no entity being retired is also LPA authoritative data
    lpa_entity_set = set(int(r['entity']) for r in lpa_rows)
    overlap = set(entity_to_org.keys()) & lpa_entity_set
    if overlap:
        raise ValueError(
            f"ERROR: Entities {sorted(overlap)} are in BOTH the retirement list and "
            "LPA authoritative data. Aborting to prevent data loss."
        )
    logger.info(f"✓ No overlap with LPA authoritative data")

    logger.info(f"✓ Verified and queued {len(entity_to_org)} plan-timetable entities for retirement")
    return entity_to_org, set(org_ranges.keys())


def retire_local_plan_data(lookup_rows, entity_org_rows, org_mapping, group_constituents):
    """Retire MHCLG seeded data for local-plan dataset. Returns (entities, orgs)."""
    logger.info("\n=== Processing local-plan dataset ===")

    mhclg_range_min, mhclg_range_max = MHCLG_RANGES['local-plan']
    prefix = 'local-plan'

    # Step 1: Find LPAs that provided data
    all_lpa_rows = [
        r for r in lookup_rows
        if r['organisation'] != MHCLG_ORG and r['prefix'] == prefix
    ]

    if not all_lpa_rows:
        logger.warning(f"No LPA data found for {prefix}")
        return set(), set()

    # Separate LPAs that created new data (outside MHCLG range) from those that
    # updated MHCLG data in-place (within MHCLG range). In-place updates don't
    # need retirement since the MHCLG data was overwritten, not duplicated.
    def is_in_mhclg_range(entity):
        return mhclg_range_min <= entity <= mhclg_range_max

    lpa_rows = [r for r in all_lpa_rows if not is_in_mhclg_range(int(r['entity']))]

    updated_in_place_orgs = set(
        r['organisation'] for r in all_lpa_rows if is_in_mhclg_range(int(r['entity']))
    ) - set(r['organisation'] for r in lpa_rows)

    if updated_in_place_orgs:
        logger.info(
            f"Skipping {len(updated_in_place_orgs)} LPAs that updated MHCLG data in-place "
            f"(no retirement needed): {', '.join(sorted(updated_in_place_orgs))}"
        )

    if not lpa_rows:
        logger.info("No LPAs with new data outside MHCLG range — nothing to retire")
        return set(), set()

    min_lpa = min(int(r['entity']) for r in lpa_rows)
    max_lpa = max(int(r['entity']) for r in lpa_rows)
    logger.info(f"LPA entity range: {min_lpa} - {max_lpa}")
    logger.info(f"✓ All LPA entities outside MHCLG range ({mhclg_range_min}-{mhclg_range_max})")

    # Step 2: Generate fake plan references from LPA organisation names
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
    # Build reverse mapping: reference -> org
    ref_to_org = {ref: org for org, ref in fake_plan_references.items()}

    entity_to_org = {}
    for row in lookup_rows:
        if (row['organisation'] == MHCLG_ORG
                and row['prefix'] == prefix
                and row['reference'] in fake_plan_references.values()):
            entity = int(row['entity'])
            if not is_in_mhclg_range(entity):
                raise ValueError(
                    f"ERROR: Found MHCLG entity {entity} outside expected range "
                    f"({mhclg_range_min}-{mhclg_range_max}). This indicates data corruption."
                )
            entity_to_org[entity] = (ref_to_org[row['reference']], prefix)

    # Completeness check: every LPA with data should have a MHCLG template entity.
    # Joint local-planning-group orgs submit data under a combined code, but MHCLG
    # seeded fake data (and named it) per constituent authority. If every constituent
    # is already covered — by its own direct submission above, or by a template found
    # here — the group itself doesn't need one of its own.
    orgs_with_retirement = set(org for org, _ in entity_to_org.values())
    orgs_missing = lpa_orgs - orgs_with_retirement

    still_missing = set()
    for org in sorted(orgs_missing):
        constituents = constituent_orgs(org, group_constituents)
        if constituents == {org}:
            still_missing.add(org)
            continue

        unresolved_constituents = [c for c in constituents if c not in org_mapping]
        if unresolved_constituents:
            still_missing.add(org)
            continue

        all_covered = True
        for constituent in constituents:
            if constituent in orgs_with_retirement:
                continue
            slug = authority_to_slug(org_mapping[constituent])
            reference = f"{slug}-new-local-plan"
            match = next(
                (r for r in lookup_rows
                 if r['organisation'] == MHCLG_ORG and r['prefix'] == prefix
                 and r['reference'] == reference),
                None
            )
            if not match:
                all_covered = False
                continue
            entity = int(match['entity'])
            if not is_in_mhclg_range(entity):
                raise ValueError(
                    f"ERROR: Found MHCLG entity {entity} outside expected range "
                    f"({mhclg_range_min}-{mhclg_range_max}). This indicates data corruption."
                )
            entity_to_org[entity] = (org, prefix)
            ref_to_org[reference] = org

        if all_covered:
            orgs_with_retirement.add(org)
        else:
            still_missing.add(org)

    orgs_missing = still_missing
    if orgs_missing:
        raise ValueError(
            f"ERROR: No MHCLG template entity found for: {', '.join(sorted(orgs_missing))}. "
            "These LPAs provided data but no fake template was identified to retire."
        )
    logger.info(f"✓ All {len(lpa_orgs)} LPAs have a matching MHCLG template entity")

    mhclg_entities = set(entity_to_org.keys())
    logger.info(f"Found {len(mhclg_entities)} MHCLG local plan entities to retire")

    # No-overlap check: ensure no entity being retired is also LPA authoritative data
    lpa_entity_set = set(int(r['entity']) for r in lpa_rows)
    overlap = mhclg_entities & lpa_entity_set
    if overlap:
        raise ValueError(
            f"ERROR: Entities {sorted(overlap)} are in BOTH the retirement list and "
            "LPA authoritative data. Aborting to prevent data loss."
        )
    logger.info(f"✓ No overlap with LPA authoritative data")

    # Cross-check each entity falls within an entity-organisation range for its LPA
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
        lpa_org = ref_to_org.get(reference)

        if not lpa_org:
            raise ValueError(
                f"ERROR: Entity {entity} (ref={reference}) does not map back to any LPA organisation."
            )

        # Verify entity falls within an entity-organisation range for this LPA
        # (or one of its constituent authorities, for joint local-planning-groups)
        in_range = any(
            org in constituent_orgs(lpa_org, group_constituents) and emin <= entity <= emax
            for org, emin, emax in entity_org_ranges
        )
        if not in_range:
            raise ValueError(
                f"ERROR: Entity {entity} (ref={reference}) does not fall within any "
                f"entity-organisation range for {lpa_org}."
            )

    logger.info(f"✓ All entities cross-checked against entity-organisation.csv")
    logger.info(f"✓ Queued {len(mhclg_entities)} local-plan entities for retirement")
    return entity_to_org, lpa_orgs


def save_retired_entities(entity_to_org, old_entity_rows):
    """Append retired entities to old-entity.csv."""
    logger.info(f"\n=== Saving {len(entity_to_org)} entities to old-entity.csv ===")

    if not entity_to_org:
        logger.warning("No entities to retire")
        return

    # Check for duplicates (entities already retired)
    existing = set(int(r['old-entity']) for r in old_entity_rows)
    duplicates = set(entity_to_org.keys()) & existing

    if duplicates:
        logger.warning(f"⚠ {len(duplicates)} entities are already in old-entity.csv (skipping)")
        entities_to_add = {e: org for e, org in entity_to_org.items() if e not in duplicates}
    else:
        entities_to_add = entity_to_org

    if not entities_to_add:
        logger.info("No new entities to add")
        return

    # Read existing file content and append new rows
    fieldnames = ['old-entity', 'status', 'entity', 'notes', 'end-date', 'entry-date', 'start-date']

    with open(OLD_ENTITY_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        for entity_id in sorted(entities_to_add):
            org, dataset = entities_to_add[entity_id]
            writer.writerow({
                'old-entity': entity_id,
                'status': 410,
                'entity': '',
                'notes': f'Retiring fake MHCLG template data for {org}-{dataset}',
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

    org_mapping, group_constituents = fetch_organisation_data()

    timetable_entity_org, timetable_orgs = retire_plan_timetable_data(
        lookup_rows, entity_org_rows, group_constituents)
    local_plan_entity_org, local_plan_orgs = retire_local_plan_data(
        lookup_rows, entity_org_rows, org_mapping, group_constituents)

    all_entity_org = {**timetable_entity_org, **local_plan_entity_org}

    logger.info(f"\n=== Summary ===")
    logger.info(f"plan-timetable: {len(timetable_entity_org)} entities")
    logger.info(f"local-plan: {len(local_plan_entity_org)} entities")
    logger.info(f"Total: {len(all_entity_org)} entities")

    if not all_entity_org:
        logger.warning("No entities to retire")
        sys.exit(0)

    save_retired_entities(all_entity_org, old_entity_rows)
    logger.info("\n✓ Retirement completed successfully")

    # Print summary to stdout for use in PR body
    print(f"Total entities retired: {len(all_entity_org)}")
    print("")
    if timetable_orgs:
        print(f"**plan-timetable** ({len(timetable_entity_org)} entities):")
        for org in sorted(timetable_orgs):
            print(f"- {org}")
        print("")
    if local_plan_orgs:
        print(f"**local-plan** ({len(local_plan_entity_org)} entities):")
        for org in sorted(local_plan_orgs):
            print(f"- {org}")
        print("")


if __name__ == '__main__':
    main()

import csv
import math
import sys
from time import perf_counter, sleep
import click
import requests
import pandas as pd
import shutil
import logging
import traceback
import subprocess
import re

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict
from io import StringIO
from digital_land.commands import check_and_assign_entities
from digital_land.specification import Specification

from tqdm import tqdm
from urllib.request import urlretrieve
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor

import shapely.wkt
from shapely import STRtree
from shapely.errors import ShapelyError
from rapidfuzz import fuzz, process

logger = logging.getLogger(__name__)

# duplicate_entity_all_fields (see _duplicate_entities_error_rows) only catches reference
# churn when every other field is byte-identical, which misses it when a boundary gets
# nudged or a name gets lightly edited at the same time as the reference. These thresholds
# control the fuzzy/similarity companion check (see _possible_reference_churn_fuzzy_match_rows)
# that catches those near-misses - straw man starting values, not calibrated against real
# data the way the growth/retention thresholds were, so treat as a first guess to revisit.
GEOMETRY_OVERLAP_THRESHOLD = 0.9  # intersection-over-union, 0-1
NAME_SIMILARITY_THRESHOLD = 90  # rapidfuzz token_sort_ratio, 0-100

# Statuses that are recorded and reported but don't stop a resource's other, valid
# entities from being promoted - everything else blocks. Centralised here rather than
# checked inline so a new non-blocking status only needs adding in one place.
NON_BLOCKING_STATUSES = {'warn', 'skipped'}

# Replaces the old growth-only large_number_of_new_entities check entirely (see ticket
# section 3): growth divided by the *current* resource's size never matched what the old
# error message claimed (the *previous* version), and tested against live data it added no
# discriminating power once retention was accounted for - every case it caught, retention
# caught too, and raising it only ever downgraded already-low-retention resources rather
# than flagging anything new. A resource is safe to auto-assign when at least this
# percentage of the previous resource's entities are still present; below it, manual
# review is required. 90 is a starting value, open to recalibration once more real
# examples are seen.
RETENTION_THRESHOLD = 90


@dataclass(frozen=True)
class ValidationConfig:
    """
    The tunable thresholds and blocking policy _collect_validation_rows (and the
    orchestrator deciding what to do with its findings) run against. Bundled into one
    object - rather than each function reading the module constants above directly -
    so every dependency a validation function has is visible in its own signature, and
    a future threshold tweak touches one call site instead of several. The module
    constants above remain the documented, calibration-tracked defaults; this is just
    the vehicle for passing (and, in tests, overriding) them.
    """

    geometry_overlap_threshold: float = GEOMETRY_OVERLAP_THRESHOLD
    name_similarity_threshold: float = NAME_SIMILARITY_THRESHOLD
    retention_threshold: float = RETENTION_THRESHOLD
    non_blocking_statuses: frozenset = field(
        default_factory=lambda: frozenset(NON_BLOCKING_STATUSES)
    )


# Historic England (PB1164) is deliberately recorded as the organisation against
# conservation-area entities alongside the owning local authority (see test_lookup in
# config-manager's test_config_dataset.py, which explicitly allows this), but doesn't
# get its own entity-organisation.csv range - HE doesn't own a number range on this
# dataset, the local authority does.
ENTITY_ORGANISATION_RANGE_EXCLUSIONS = {
    ("conservation-area", "government-organisation:PB1164"),
}


def run_command(cmd, capture_output=True, check=True):
    """Run a shell command and return stdout when requested."""
    try:
        result = subprocess.run(
            cmd,
            text=True,
            capture_output=capture_output,
            check=False,
        )
    except FileNotFoundError:
        raise RuntimeError(f"Required command not found on PATH: {cmd[0]}")

    if check and result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        detail = stderr or stdout or "command failed"
        raise RuntimeError(f"{' '.join(cmd)} -> {detail}")

    return (result.stdout or "").strip() if capture_output else ""


def commit_to_main(triggered_by, success_count, scope, batch_size=0, start_batch=1):
    if batch_size > 0:
        commit_label = f"{scope} - Batch assign entities update (batch {start_batch}, {success_count} successful resource(s))"
    else:
        commit_label = f"{scope} - Batch assign entities update ({success_count} successful resource(s))"

    run_command(["git", "config", "user.name", "github-actions-bot"])
    run_command(["git", "config", "user.email", "noreply@github.com"])

    run_command(["git", "add", "pipeline/"])
    run_command(["git", "add", "collection/"], check=False)

    staged_changes = subprocess.run(
        ["git", "diff", "--staged", "--quiet"],
        check=False,
    ).returncode != 0

    if not staged_changes:
        print("No staged changes after batch assignment; skipping commit")
        return

    run_command(["git", "commit", "-m", commit_label])
    run_command(["git", "pull", "--rebase", "origin", "main"])
    run_command(["git", "push", "origin", "HEAD:main"])
    print(f"Committed and pushed to main: {commit_label}")

def download_file(url, output_path, raise_error=False, max_retries=5):
    """Downloads a file using urllib and saves it to the output directory. msj151225"""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    retries = 0
    while retries < max_retries:
        try:
            urlretrieve(url, output_path)
            break
        except Exception as e:
            if raise_error:
                raise e
            else:
                logger.error(f"error downloading file from url {url}: {e}")
        retries += 1


def download_urls(url_map, max_threads=4):
    """Downloads multiple files concurrently using threads. msj151225" """
    with ThreadPoolExecutor(max_threads) as executor:
        futures = {
            executor.submit(download_file, url, output_path): url
            for url, output_path in url_map.items()
        }
        results = []
        for future in tqdm(futures, desc="Downloading files"):
            try:
                results.append(future.result())
            except Exception as e:
                logger.error(f"Error during download: {e}")
        return results

def fetch_csv_with_retry(url, max_retries=5, backoff_seconds=10):
    """Fetch a CSV URL with retries, since datasette intermittently returns a
    CloudFront gateway-timeout HTML page instead of CSV under load."""
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            return pd.read_csv(StringIO(response.text), dtype=str)
        except (requests.RequestException, pd.errors.ParserError) as e:
            last_error = e
            logger.warning(f"Attempt {attempt}/{max_retries} failed fetching {url}: {e}")
            if attempt < max_retries:
                sleep(backoff_seconds * attempt)
    raise RuntimeError(f"Failed to fetch CSV from {url} after {max_retries} attempts: {last_error}")


def get_old_resource_hashes_batch(endpoints: list) -> Dict[str, str]:
    """
    Fetch old resource hashes for multiple endpoints at once using a single SQL query.
    Returns a dictionary mapping endpoint -> resource_hash
    """
    if not endpoints:
        return {}

    DATASETTE_BASE_URL = "https://datasette.planning.data.gov.uk/digital-land.csv"
    
    # Build the endpoint list for the SQL query
    endpoint_list = ', '.join([f"'{ep}'" for ep in endpoints])
    
    # Pick the resource with the most recent resource_end_date (the resource immediately
    # superseded by whatever is now current), not the lowest rowid. rowid usually tracks
    # recency closely but isn't guaranteed to - resource_end_date can be non-monotonic
    # relative to rowid for a small number of endpoints, and ORDER BY rowid alone makes
    # SQLite's row choice on resource_end_date ties implementation-defined.
    query = f"""SELECT endpoint, resource
FROM (
    SELECT endpoint, resource, resource_end_date,
    ROW_NUMBER() OVER (
        PARTITION BY endpoint
        ORDER BY resource_end_date DESC, rowid DESC
    ) AS rn
    FROM reporting_historic_endpoints
    WHERE resource_end_date IS NOT NULL and resource_end_date != ''
    AND endpoint IN ({endpoint_list})
)
WHERE rn = 1
ORDER BY endpoint"""
    
    params = urlencode({"sql": query})
    url = f"{DATASETTE_BASE_URL}?{params}"

    try:
        # fetch_csv_with_retry rather than a bare requests.get - this is called once per
        # whole run, so a single transient Datasette blip here (not uncommon under load,
        # see fetch_csv_with_retry's own docstring) would otherwise make every endpoint
        # look like it has no history, blocking every resource in the batch rather than
        # just this one call.
        result_df = fetch_csv_with_retry(url)
        # Create a mapping of endpoint -> resource
        endpoint_resource_map = dict(zip(result_df['endpoint'], result_df['resource']))
        return endpoint_resource_map
    except Exception as e:
        logger.error(f"Error fetching old resource hashes: {e}")
        return {}


def get_endpoints_with_any_history(endpoints: list) -> Optional[set]:
    """
    Which of these endpoints have at least one SUPERSEDED resource (resource_end_date set)
    in reporting_historic_endpoints - distinguishes a genuinely brand-new endpoint (nothing
    to compare against, ever) from one that has real history but get_old_resource_hashes_batch
    simply failed to resolve a baseline for.

    Deliberately excludes rows with a blank resource_end_date: every endpoint being
    processed here is, by definition, actively being crawled right now, so it always has at
    least its own current (not-yet-superseded) row - counting that as "history" would make
    this check true for almost everything and defeat the point of the distinction.

    Returns None (not an empty set) if the query itself fails, so a network blip can't be
    mistaken for "no endpoints have history" - the caller should then fail safe and treat
    every endpoint as if it has history, same as today's behaviour.
    """
    if not endpoints:
        return set()

    DATASETTE_BASE_URL = "https://datasette.planning.data.gov.uk/digital-land.csv"
    endpoint_list = ', '.join([f"'{ep}'" for ep in endpoints])
    query = f"""SELECT DISTINCT endpoint
FROM reporting_historic_endpoints
WHERE resource_end_date IS NOT NULL AND resource_end_date != ''
AND endpoint IN ({endpoint_list})"""

    params = urlencode({"sql": query})
    url = f"{DATASETTE_BASE_URL}?{params}"

    try:
        # fetch_csv_with_retry rather than a bare requests.get - see
        # get_old_resource_hashes_batch for why this matters at this call frequency.
        result_df = fetch_csv_with_retry(url)
        return set(result_df['endpoint'].dropna())
    except Exception as e:
        logger.error(f"Error fetching endpoint history presence: {e}")
        return None


def get_old_resource_df_from_hash(resource_hash: str, collection_name: str, dataset: str, max_retries=5, backoff_seconds=10):
    """
    Fetch the transformed CSV for an old resource given its hash.
    """
    try:
        transformed_url = (
            f"https://files.planning.data.gov.uk/{collection_name}-collection/transformed/{dataset}/{resource_hash}.csv"
        )
        # Retried, same as fetch_csv_with_retry - this feeds every duplicate/retention
        # check against the previous resource, so a transient blip here would otherwise
        # silently fail this one resource over to previous_resource_not_found (blocking)
        # rather than actually comparing against its real history.
        last_error = None
        transformed_response = None
        for attempt in range(1, max_retries + 1):
            try:
                transformed_response = requests.get(transformed_url, timeout=120)
                transformed_response.raise_for_status()
                break
            except requests.RequestException as e:
                last_error = e
                logger.warning(f"Attempt {attempt}/{max_retries} failed fetching {transformed_url}: {e}")
                if attempt < max_retries:
                    sleep(backoff_seconds * attempt)
        if transformed_response is None:
            raise RuntimeError(f"Failed to fetch {transformed_url} after {max_retries} attempts: {last_error}")

        # Save the old transformed resource to resource/old before reading
        old_resource_dir = Path("resource") / "old"
        old_resource_dir.mkdir(parents=True, exist_ok=True)
        old_file_path = old_resource_dir / f"{resource_hash}.csv"
        old_file_path.write_bytes(transformed_response.content)


        return pd.read_csv(old_file_path, dtype=str, low_memory=False)
    except Exception as e:
        logger.error(f"Error downloading old resource {resource_hash}: {e}")
        return None


def get_invalid_uri_entities(cache_dir: Path, resource: str) -> set:
    """
    Read the per-resource issue log produced by check_and_assign_entities and return
    the set of entities affected by an 'invalid URI' issue, so only those entities can
    be excluded from promotion rather than blocking the whole resource.

    The issue log's entity column is backfilled from entry-number once entity
    assignment completes (digital_land.pipeline.main.Pipeline.save_logs calls
    IssueLog.apply_entity_map before saving), so this is only populated for rows that
    both had an invalid URI and resolved to an entity - a row that failed both checks
    won't appear here, but it also wouldn't be promotable either way.
    """
    issue_path = cache_dir / "assign_entities" / "issue" / f"{resource}.csv"
    if not issue_path.exists():
        return set()
    try:
        issue_df = pd.read_csv(issue_path, dtype=str)
    except Exception as e:
        logger.error(f"Error reading issue log {issue_path}: {e}")
        return set()
    if "issue-type" not in issue_df.columns or "entity" not in issue_df.columns:
        return set()
    invalid_uri_rows = issue_df[issue_df["issue-type"] == "invalid URI"]
    return set(invalid_uri_rows["entity"].dropna()) - {""}


def _make_fingerprints(df, except_fields=["reference", "entry-date"], only_fields=None):
    """
    Create a fingerprint for each entity based options specified.
    By default, the fingerprint is based on all fields except reference and entry-date, but this can be customised by specifying different fields to exclude or include.
        except_fields: list of fields to exclude from the fingerprint (default: ["reference", "entry-date"])
        only_fields: if specified, only include these fields in the fingerprint (overrides except_fields)
    """
    
    tmp = df[~df['field'].isin(except_fields)].copy()
    if only_fields:
        tmp = tmp[tmp['field'].isin(only_fields)]
    tmp['f_field'] = tmp['field'].astype(str).str.strip().str.lower()
    tmp['f_value'] = tmp['value'].fillna('').astype(str).str.strip().str.lower()
    fp = (
        tmp.groupby('entity')[['f_field', 'f_value']]
        .apply(lambda g: '|'.join(sorted(g['f_field'] + '::' + g['f_value'])))
        .reset_index()
    )
    fp = fp.rename(columns={0: 'fingerprint'})
    # Extract actual field values as columns
    field_values = df[df['field'].isin(['organisation', 'reference', 'prefix'])][['entity', 'field', 'value']].drop_duplicates()
    field_pivot = field_values.pivot_table(index='entity', columns='field', values='value', aggfunc='first').reset_index()
    fp = fp.merge(field_pivot, on='entity', how='left')
    # ensure expected columns exist to avoid KeyError
    for _col in ('organisation', 'reference', 'prefix'):
        if _col not in fp.columns:
            fp[_col] = ''
    return fp


def _missing_metadata_frame(df):
    field_values = df[
        df['field'].isin(['organisation', 'reference', 'prefix'])
    ][['entity', 'field', 'value']].drop_duplicates()
    frame = field_values.pivot_table(index='entity', columns='field', values='value', aggfunc='first').reset_index()
    for column in ('entity', 'organisation', 'reference', 'prefix'):
        if column not in frame.columns:
            frame[column] = ''
    return frame


def _duplicate_error_rows(matches, dataset, resource, error_code, message_factory):
    if matches.empty:
        return []

    matches = matches.copy()
    matches['entity_list'] = (
        matches.groupby('fingerprint')['entity_old']
        .transform(lambda x: ', '.join(x.astype(str)))
    )
    matches = matches.drop_duplicates('fingerprint')

    return [
        {
            'dataset': dataset,
            'resource': resource,
            'organisation': match_row.get('organisation_new', ''),
            'reference': match_row.get('reference_new', ''),
            'status': 'blocked',
            'error_code': error_code,
            'message': message_factory(match_row),
        }
        for _, match_row in matches.iterrows()
    ]


def _duplicate_entities_error_rows(old_df, new_df, dataset, resource):
    """
        Check for duplicate entities based on all fields except reference and entry-date. 
        
        If a new entity has the same values for all other fields as an old entity, flag as a potential duplicate.
    """
    matches = new_df.merge(old_df, on='fingerprint', how='inner', suffixes=('_new', '_old'))
    return _duplicate_error_rows(
        matches,
        dataset,
        resource,
        'duplicate_entity_all_fields',
        lambda match_row: (
            f"Matches existing entity(s) {match_row['entity_list']} "
            f"{re.sub(r'[^|]*multipolygon[^|]*', '<multipolygon>', match_row['fingerprint'])}."
        ),
    )


def _extract_field_values(df, fields):
    """Pivot specific long-format fields out into named columns, one row per entity."""
    field_values = df[df['field'].isin(fields)][['entity', 'field', 'value']].drop_duplicates()
    frame = field_values.pivot_table(index='entity', columns='field', values='value', aggfunc='first').reset_index()
    for field in fields:
        if field not in frame.columns:
            frame[field] = None
    return frame


def _parse_geometry(wkt_value):
    if not wkt_value or not isinstance(wkt_value, str):
        return None
    try:
        geom = shapely.wkt.loads(wkt_value)
        if geom.is_empty or not geom.is_valid:
            return None
        return geom
    except (ShapelyError, ValueError):
        return None


def _possible_reference_churn_fuzzy_match_rows(old_df, new_df, dataset, resource, config):
    """
        Companion to duplicate_entity_all_fields: that check requires every other field to
        be byte-identical, so it misses a new entity that's probably the same real-world
        feature as an old one but with a slightly adjusted boundary or a lightly-edited name
        alongside the reference change. Lower confidence than an exact match by design (a
        similarity score, not an equality), so this is 'warn' rather than blocking.

        Geometry candidates are narrowed with an STRtree envelope query before computing the
        precise intersection-over-union, and name comparisons run as one vectorised rapidfuzz
        call rather than a nested loop - both make this cheap enough for large datasets, but
        the geometry side is still the most expensive part of this check at real scale.
    """
    old_fields = _extract_field_values(old_df, ['geometry', 'name'])
    new_fields = _extract_field_values(new_df, ['geometry', 'name'])

    if old_fields.empty or new_fields.empty:
        return []

    old_fields = old_fields.reset_index(drop=True)
    old_fields['_geom'] = old_fields['geometry'].apply(_parse_geometry)
    old_geoms = old_fields[old_fields['_geom'].notna()].reset_index(drop=True)
    tree = STRtree(old_geoms['_geom'].tolist()) if len(old_geoms) else None

    old_names = old_fields['name'].fillna('').astype(str).tolist()
    new_names = new_fields['name'].fillna('').astype(str).tolist()
    name_scores = (
        process.cdist(new_names, old_names, scorer=fuzz.token_sort_ratio)
        if any(old_names) and any(new_names)
        else None
    )

    rows = []
    for i, (_, new_row) in enumerate(new_fields.iterrows()):
        new_geom = _parse_geometry(new_row.get('geometry'))

        geom_entity, geom_overlap = None, 0.0
        if new_geom is not None and tree is not None:
            for idx in tree.query(new_geom):
                old_geom = old_geoms['_geom'].iloc[idx]
                if not new_geom.intersects(old_geom):
                    continue
                union_area = new_geom.union(old_geom).area
                if union_area == 0:
                    continue
                overlap = new_geom.intersection(old_geom).area / union_area
                if overlap > geom_overlap:
                    geom_overlap = overlap
                    geom_entity = old_geoms['entity'].iloc[idx]

        name_entity, name_score = None, 0.0
        if name_scores is not None:
            best_idx = name_scores[i].argmax()
            name_score = name_scores[i][best_idx]
            if name_score > 0:
                name_entity = old_fields['entity'].iloc[best_idx]

        geom_hit = geom_overlap >= config.geometry_overlap_threshold
        name_hit = name_score >= config.name_similarity_threshold
        if not (geom_hit or name_hit):
            continue

        reasons = []
        if geom_hit:
            reasons.append(f"geometry overlap {geom_overlap:.0%} with entity {geom_entity}")
        if name_hit:
            reasons.append(f"name similarity {name_score:.0f}/100 with entity {name_entity}")

        rows.append(
            {
                'dataset': dataset,
                'resource': resource,
                'organisation': '',
                'reference': '',
                'status': 'warn',
                'error_code': 'possible_reference_churn_fuzzy_match',
                'message': (
                    f"Entity {new_row['entity']} is a possible match for an existing entity "
                    f"({'; '.join(reasons)}) - review before treating as genuinely new."
                ),
            }
        )

    return rows


def _duplicate_reference_organisation_error_rows(old_df, new_df, dataset, resource):
    """
        Collapses what used to be a separate duplicate_prefix_reference_organisation check:
        within a single dataset's transformed output, prefix is always constant and equal to
        the dataset, so adding it to the fingerprint never changes which rows match - the two
        checks were provably identical in every call this script makes.
    """
    matches = new_df.merge(old_df, on='fingerprint', how='inner', suffixes=('_new', '_old'))
    return _duplicate_error_rows(
        matches,
        dataset,
        resource,
        'duplicate_reference_organisation',
        lambda match_row: (
            f"Entity exists with the same reference and organisation"
            f" {match_row['entity_list']} {match_row['fingerprint']}."
        ),
    )


def _duplicate_reference_organisation_in_new_resource_error_rows(df, dataset, resource):
    """
        Check for duplicate reference + entry-date combinations within the current resource,
        matching the pipeline's own "reference values are not unique" issue scope (reference +
        entry-date), rather than reference + organisation.
    """
    metadata = _missing_metadata_frame(df)
    if 'entry-date' in df.columns:
        entry_dates = df.groupby('entity')['entry-date'].first().reset_index()
        duplicates = metadata.merge(entry_dates, on='entity', how='left')
    else:
        duplicates = metadata.copy()
        duplicates['entry-date'] = ''
    duplicates['reference'] = duplicates['reference'].fillna('').astype(str).str.strip().str.lower()
    duplicates['entry-date'] = duplicates['entry-date'].fillna('').astype(str).str.strip()
    duplicates['fingerprint'] = 'reference::' + duplicates['reference'] + '|entry-date::' + duplicates['entry-date']

    duplicates = duplicates[
        duplicates.duplicated('fingerprint', keep=False)
    ].drop_duplicates('fingerprint')

    if duplicates.empty:
        return []

    return [
        {
            'dataset': dataset,
            'resource': resource,
            'organisation': dup_row.get('organisation', ''),
            'reference': dup_row.get('reference', ''),
            'status': 'blocked',
            'error_code': 'duplicate_reference_organisation_in_new_resource',
            'message': f"Duplicate reference and entry-date found in resource {dup_row['fingerprint']}.",
        }
        for _, dup_row in duplicates.iterrows()
    ]


def _missing_organisation_error_rows(df, dataset, resource):
    """ 
        Non-blocking: a missing organisation doesn't stop an entity being assigned
        (PrintLookupPhase only requires prefix + reference), unlike a missing reference.
        Still logged so it stays visible, per the pipeline's own "missing value" (error,
        not critical) treatment of this condition.
    """
    missing = df[(df['organisation'].isna()) | (df['organisation'] == '')]
    return [
        {
            'dataset': dataset,
            'resource': resource,
            'organisation': missing_row.get('organisation', ''),
            'reference': missing_row.get('reference', ''),
            'status': 'warn',
            'error_code': 'missing_organisation',
            'message': (
                f"Missing organisation for entity {missing_row.get('entity')} "
                f"with reference {missing_row.get('reference')} in current transformed rows."
            ),
        }
        for _, missing_row in missing.iterrows()
    ]


def _contiguous_ranges_by_org(new_entities, entity_org_map):
    """Group newly created entity numbers into contiguous per-organisation ranges.

    Entities for different organisations can be interleaved within a single
    resource (e.g. a multi-authority endpoint), so a single min/max span across
    all new entities would wrongly claim entity numbers belonging to another
    organisation. A run only continues while both the organisation and the
    entity number stay consecutive.

    Returns a list of (organisation, min_entity, max_entity) tuples.
    """
    ranges = []
    run_org = run_start = run_end = None
    for entity in sorted(new_entities):
        org_value = entity_org_map.get(entity, "")
        if run_org == org_value and run_end is not None and entity == run_end + 1:
            run_end = entity
        else:
            if run_org is not None:
                ranges.append((run_org, run_start, run_end))
            run_org, run_start, run_end = org_value, entity, entity
    if run_org is not None:
        ranges.append((run_org, run_start, run_end))
    return ranges


def _missing_reference_error_rows(df, dataset, resource):
    missing = df[(df['reference'].isna()) | (df['reference'] == '')]
    return [
        {
            'dataset': dataset,
            'resource': resource,
            'organisation': missing_row.get('organisation', ''),
            'reference': missing_row.get('reference', ''),
            'status': 'blocked',
            'error_code': 'missing_reference',
            'message': (
                f"Missing reference for entity {missing_row.get('entity')} "
                f"with organisation {missing_row.get('organisation')} in current transformed rows."
            ),
        }
        for _, missing_row in missing.iterrows()
    ]


def _collect_validation_rows(current_resource_df, old_resource_df, dataset, resource, old_resource_hash, config, organisation_name='', endpoint_has_history=True):
    """
    Decide whether this resource is safe to auto-assign, and answer "why was this
    resource not assigned" when it isn't.

    Pure: takes the current transformed frame, the previous one (or None if no baseline
    could be resolved), identifying/labelling info for the output rows, and a config of
    tunable thresholds - and returns findings with no side effects or I/O. Every check
    here is its own standalone, equally pure function; this one just runs them in
    sequence and assembles their results.

    :param current_resource_df: this resource's current transformed rows.
    :param old_resource_df: the previous version's transformed rows, or None if no
        baseline could be resolved (distinct from a baseline that resolved to zero rows
        - see endpoint_has_history).
    :param dataset: dataset (pipeline) name, for labelling output rows.
    :param resource: resource hash, for labelling output rows.
    :param old_resource_hash: the previous resource's hash, for labelling output rows.
    :param config: ValidationConfig - the thresholds this run's checks apply.
    :param organisation_name: the endpoint's registered organisation, for labelling
        output rows.
    :param endpoint_has_history: whether this endpoint has ever had an earlier,
        superseded resource - distinguishes "real history exists but we failed to
        resolve a baseline from it" (blocking) from "this is genuinely brand new, there
        is nothing to compare against" (not blocking).
    :return: (validation_rows, old_entities, new_entity_ids) - the findings, the
        previous resource's entity set, and this resource's newly-added entity set.
    """
    validation_rows = []
    current_entities = set(current_resource_df['entity'])

    if old_resource_df is None:
        old_entities = set()
        new_entity_ids = current_entities
    else:
        old_entities = set(old_resource_df['entity'])
        new_entity_ids = current_entities - old_entities

    if old_resource_df is None:
        if endpoint_has_history:
            # Real history exists but we failed to resolve a baseline from it - stay
            # cautious, same as today: we can't rule out duplicate/growth risk, we just
            # failed to check it, which is different from there being nothing to check.
            validation_rows.append(
                {
                    'dataset': dataset,
                    'resource': resource,
                    'organisation': organisation_name,
                    'reference': '',
                    'status': 'blocked',
                    'error_code': 'previous_resource_not_found',
                    'message': 'Previous resource not found.',
                }
            )
        else:
            # Genuinely brand-new endpoint - every entity is new by definition, so there's
            # no duplicate-against-history risk to check for, just nothing to compare
            # against. Logged so it's visible, but doesn't block the first assignment.
            validation_rows.append(
                {
                    'dataset': dataset,
                    'resource': resource,
                    'organisation': organisation_name,
                    'reference': '',
                    'status': 'warn',
                    'error_code': 'new_endpoint_no_history',
                    'message': 'No previous resource exists for this endpoint - treating all entities as new.',
                }
            )
    elif len(old_entities) == 0:
        validation_rows.append(
            {
                'dataset': dataset,
                'resource': resource,
                'organisation': organisation_name,
                'reference': '',
                'status': 'blocked',
                'error_code': 'previous_resource_empty',
                'message': f'Previous resource has no entities {old_resource_hash}.',
            }
        )
    
    if len(new_entity_ids) == 0:
        # Pure no-op, not a failure - nothing to promote either way, so this doesn't
        # block (see ValidationConfig.non_blocking_statuses).
        validation_rows.append(
            {
                'dataset': dataset,
                'resource': resource,
                'organisation': organisation_name,
                'reference': '',
                'status': 'skipped',
                'error_code': 'current_resource_no_new_entities',
                'message': 'Current resource contains no new entities for assignment.',
            }
        )

    # The following checks require previous resource entities/facts to compare against
    if len(old_entities) > 0 and len(new_entity_ids) > 0:
        new_resource_only_df = current_resource_df[current_resource_df['entity'].isin(new_entity_ids)]

        # Retention: what proportion of the previous resource's entities are still present
        # in the current one. Low retention is the churn/reference-change signal (old
        # entities disappearing); growth alone isn't, which is why it's not checked here.
        retained_count = len(old_entities & current_entities)
        retention_pct = (retained_count / len(old_entities)) * 100
        if retention_pct < config.retention_threshold:
            validation_rows.append(
                {
                    'dataset': dataset,
                    'resource': resource,
                    'organisation': organisation_name,
                    'reference': '',
                    'status': 'blocked',
                    'error_code': 'low_entity_retention',
                    'message': (
                        f"Only {retention_pct:.1f}% of the previous resource's entities "
                        f"({retained_count} of {len(old_entities)}) are still present in the "
                        f"current resource - below the {config.retention_threshold}% "
                        f"retention threshold. Old resource hash: {old_resource_hash}."
                    ),
                }
            )

        # Check for duplicate entities (all fields except reference and entry-date) between the new resource and old resource
        validation_rows.extend(
            _duplicate_entities_error_rows(
                _make_fingerprints(old_resource_df),
                _make_fingerprints(new_resource_only_df),
                dataset,
                resource,
            )
        )

        # Companion to the exact-match check above: catches a likely-same entity that the
        # exact match misses because a boundary or name also shifted slightly. Non-blocking.
        validation_rows.extend(
            _possible_reference_churn_fuzzy_match_rows(
                old_resource_df,
                new_resource_only_df,
                dataset,
                resource,
                config,
            )
        )

        # Check for duplicate entities based on reference and organisation fields only between the new resource and old resource
        validation_rows.extend(
            _duplicate_reference_organisation_error_rows(
                _make_fingerprints(old_resource_df, except_fields=[], only_fields=['organisation', 'reference']),
                _make_fingerprints(new_resource_only_df, except_fields=[], only_fields=['organisation', 'reference']),
                dataset,
                resource,
            )
        )

    #Check for duplicate reference and organisation combinations within the current resource itself.
    validation_rows.extend(
        _duplicate_reference_organisation_in_new_resource_error_rows(
            current_resource_df,
            dataset,
            resource,
        )
    )
    
    #Create a df to check for missing metadata fields in the current resource
    metadata_frame = _missing_metadata_frame(current_resource_df)
    
    # Check for missing organisation or reference fields in the current resource
    validation_rows.extend(_missing_organisation_error_rows(metadata_frame, dataset, resource))
    
    # Check for missing reference fields in the current resource
    validation_rows.extend(_missing_reference_error_rows(metadata_frame, dataset, resource))
    return validation_rows, old_entities, new_entity_ids


def _fetch_resource(resource_path, download_link, resource):
    """
    Ensure resource_path exists locally, downloading it from download_link if not.
    :return: None if the resource is available (already local, or just downloaded);
        otherwise the download error, so the caller can record the failure and skip it.
    """
    if resource_path.is_file():
        return None
    print(f"Resource  file not found locally, attempting to download from {download_link}")
    try:
        response = requests.get(download_link)
        response.raise_for_status()
        resource_path.write_bytes(response.content)
        print(f"Downloaded: {resource}")
        return None
    except requests.RequestException as e:
        print(f"Failed to download: {resource}")
        print(f"Error: {e}")
        return str(e)


def _run_assignment(resource_path, resource, endpoint, collection_name, dataset, organisation_name, scope, cache_dir, input_path, lookup_path, skip_checks, endpoint_resource_map):
    """
    Snapshot this dataset's existing entities, run digital-land-python's assignment
    pipeline (check_and_assign_entities) for this resource, then load the before/after
    resources validation needs.

    :return: (pre_dataset_entities, old_resource_df, old_resource_hash,
        current_resource_df). pre_dataset_entities is this dataset's entity set *before*
        assignment, needed later to work out which entities promotion actually added.
        old_resource_df is the previous version's transformed rows, or None if there's
        no history for this endpoint or the fetch failed to resolve a baseline.
    """
    pre_lookup_df = pd.read_csv(lookup_path, dtype=str)
    pre_dataset_entities = set(
        pre_lookup_df[pre_lookup_df["prefix"] == dataset]["entity"].dropna().astype(int)
    )
    check_and_assign_entities(
        [resource_path],
        [endpoint],
        collection_name,
        dataset,
        # Blank organisation for single-source is too coarse for most datasets
        # (the endpoint's registered org isn't the right org per-entity), but
        # for now only listed-building actually needs that suppression.
        [""] if scope == "single-source" and dataset == "listed-building" else [organisation_name],
        Path(f"collection/{collection_name}"),
        cache_dir.joinpath("organisation.csv"),
        Path("specification"),
        Path(f"pipeline/{collection_name}"),
        input_path,
        prompt_user=False,
    )

    old_resource_df = None
    old_resource_hash = None
    if not skip_checks and endpoint in endpoint_resource_map:
        old_resource_hash = endpoint_resource_map[endpoint]
        print("=====")
        print(" collection || dataset || old_resource_hash || endpoint")
        print(f" {collection_name} || {dataset} || {old_resource_hash} || {endpoint}")
        old_resource_df = get_old_resource_df_from_hash(old_resource_hash, collection_name, dataset)

    current_resource_df = pd.read_csv(cache_dir / "assign_entities" / "transformed" / f"{resource}.csv", dtype=str)

    return pre_dataset_entities, old_resource_df, old_resource_hash, current_resource_df


def _run_validation(current_resource_df, old_resource_df, old_resource_hash, dataset, resource, organisation_name, endpoint, endpoints_with_history, skip_checks, validation_config, cache_dir, invalid_uri_issues):
    """
    Decide whether this resource is safe to promote: the current-resource-empty guard,
    then (unless skip_checks) the pure validation checks plus the invalid-URI-issue
    lookup that _collect_validation_rows can't do itself (it reads the issue log off
    disk, so it isn't side-effect-free).

    :return: (output_rows, invalid_uri_entities, new_entities). output_rows is every
        finding collected for this resource - the caller decides whether any of them
        block promotion (see ValidationConfig.non_blocking_statuses). invalid_uri_entities
        are the specific entities, if any, to exclude from promotion rather than
        blocking the whole resource.
    """
    output_rows = []
    invalid_uri_entities = set()

    if not skip_checks and len(current_resource_df) == 0:
        # Signals an upstream/pipeline problem, not a data-quality issue with any
        # actual rows - 'failed' so it doesn't sit in the same reviewer queue as
        # genuine duplicate/quality blocks (those need a data decision; this needs
        # an engineer to check source/pipeline health). Still blocks promotion.
        output_rows.append(
            {
                "dataset": dataset,
                "resource": resource,
                "organisation": organisation_name,
                "reference": "",
                "status": "failed",
                "error_code": "current_resource_empty",
                "message": "Current resource has no entities for assignment.",
            }
        )
        return output_rows, invalid_uri_entities, set()

    if skip_checks:
        return output_rows, invalid_uri_entities, set(current_resource_df['entity'])

    validation_rows, old_entities, new_entities = _collect_validation_rows(
        current_resource_df,
        old_resource_df,
        dataset,
        resource,
        old_resource_hash,
        validation_config,
        organisation_name=organisation_name,
        endpoint_has_history=endpoint in endpoints_with_history,
    )
    current_entities = set(current_resource_df['entity'])
    print(f"Old entities count: {len(old_entities)}, current entities count: {len(current_entities)}, New entities count: {len(new_entities)}")
    print(f"Last 5 old entities IDs: {sorted(old_entities)[-5:] if old_entities else 'N/A'}")
    print(f"Last 5 current entities IDs: {sorted(current_entities)[-5:] if current_entities else 'N/A'}")
    print(f"Last 5 new entities IDs: {sorted(new_entities)[-5:] if new_entities else 'N/A'}")
    output_rows.extend(validation_rows)

    # Non-blocking rows (e.g. missing_organisation) are logged but don't
    # block other checks from running or promotion from happening.
    if not any(r.get('status') not in validation_config.non_blocking_statuses for r in output_rows):
        iui = invalid_uri_issues[
            (invalid_uri_issues["resource"] == resource)
            & (invalid_uri_issues["dataset"] == dataset)
        ]
        if len(iui) > 0:
            # Scope to genuinely NEW entities only. The issue log covers every
            # entity touched by this resource, including pre-existing ones that
            # were correctly re-matched to an existing lookup entry (e.g. a
            # multi-authority resource resending an organisation's data under a
            # reference that already resolves) - excluding those from promotion
            # wouldn't just skip creating something new, it would strip their
            # existing, already-valid lookup.csv row entirely.
            invalid_uri_entities = get_invalid_uri_entities(cache_dir, resource) & new_entities
            if invalid_uri_entities:
                # Specific entities identified - exclude just those from
                # promotion (see promotion step below) rather than blocking
                # every other, valid entity in the resource.
                output_rows.append(
                    {
                        "dataset": dataset,
                        "resource": resource,
                        "organisation": organisation_name,
                        "reference": "",
                        "status": "warn",
                        "error_code": "invalid_uri_issue",
                        "message": (
                            f"Excluded {len(invalid_uri_entities)} "
                            f"entit{'y' if len(invalid_uri_entities) == 1 else 'ies'} with invalid "
                            f"URIs from promotion, pending manual review: "
                            f"{sorted(invalid_uri_entities)}."
                        ),
                    }
                )
            else:
                # Datasette's resource-level summary flagged this resource, but
                # the local issue log couldn't resolve it to specific entities -
                # fail safe to blocking the whole resource rather than silently
                # promoting everything.
                output_rows.append(
                    {
                        "dataset": dataset,
                        "resource": resource,
                        "organisation": organisation_name,
                        "reference": "",
                        "status": "blocked",
                        "error_code": "invalid_uri_issue",
                        "message": "Resource has known issues with invalid URIs that require manual review.",
                    }
                )

    return output_rows, invalid_uri_entities, new_entities


def _promote(cache_dir, collection_name, dataset, organisation_name, scope, lookup_path, invalid_uri_entities, pre_dataset_entities):
    """
    Copy the newly-assigned entities from the cache lookup.csv into the real one
    (excluding any invalid_uri_entities, so they're left unresolved and resurface next
    run until the underlying URI is fixed upstream), backfill any missing per-row
    organisation (missing_organisation is non-blocking, so such a row would otherwise
    sit with a permanently blank organisation), and append new entity-number ranges to
    entity-organisation.csv.
    """
    cache_lookup_path = cache_dir / "assign_entities" / collection_name / "pipeline" / "lookup.csv"
    real_lookup_path = Path("pipeline") / collection_name / "lookup.csv"
    if invalid_uri_entities:
        # lookup.csv uses CRLF line endings throughout the repo - pandas defaults
        # to LF, which would rewrite every untouched line and bury the real change
        # in a full-file diff, so the line terminator is set explicitly to match.
        cache_lookup_df = pd.read_csv(cache_lookup_path, dtype=str)
        cache_lookup_df = cache_lookup_df[~cache_lookup_df["entity"].isin(invalid_uri_entities)]
        cache_lookup_df.to_csv(real_lookup_path, index=False, lineterminator="\r\n")
    else:
        shutil.copy(cache_lookup_path, real_lookup_path)

    # After successful entity assignment and duplicate checks append entity range(s) to entity-organisation.csv.
    # A single resource can carry rows for more than one organisation (e.g. a
    # multi-authority endpoint), so entities must be grouped by their *actual*
    # organisation rather than assumed to all belong to `organisation_name` -
    # otherwise entities for other organisations end up with no registered range.
    post_lookup_df = pd.read_csv(lookup_path, dtype=str)
    post_dataset_df = post_lookup_df[post_lookup_df["prefix"] == dataset].dropna(subset=["entity"])
    post_entity_org = (
        post_dataset_df.assign(entity=post_dataset_df["entity"].astype(int))
        .drop_duplicates("entity")
        .set_index("entity")["organisation"]
        .to_dict()
    )
    new_dataset_entities = set(post_entity_org) - pre_dataset_entities
    # A new entity whose own row had no organisation (missing_organisation, now
    # non-blocking) otherwise sits in lookup.csv with a permanently blank
    # organisation - backfill it with the endpoint's registered organisation, the
    # best available source of truth when the per-row value is missing. Mirrors the
    # single-source listed-building suppression below: that case still gets no
    # organisation on purpose, so it's left blank.
    if new_dataset_entities and not (scope == "single-source" and dataset == "listed-building"):
        blank_new_entities = {
            entity
            for entity, org in post_entity_org.items()
            if entity in new_dataset_entities and not (isinstance(org, str) and org)
        }
        if blank_new_entities:
            blank_mask = (post_lookup_df["prefix"] == dataset) & post_lookup_df[
                "entity"
            ].astype(str).isin({str(entity) for entity in blank_new_entities})
            post_lookup_df.loc[blank_mask, "organisation"] = organisation_name
            # lookup.csv uses CRLF line endings throughout the repo - pandas defaults
            # to LF, which would rewrite every untouched line and bury the real
            # change in a full-file diff, so the line terminator is set explicitly.
            post_lookup_df.to_csv(real_lookup_path, index=False, lineterminator="\r\n")
            post_entity_org.update({entity: organisation_name for entity in blank_new_entities})
    # entity-organisation.csv always wants a real organisation too, as a safety net
    # for any entity the backfill above didn't cover (e.g. the listed-building
    # suppression, which leaves lookup.csv's row genuinely blank on purpose).
    entity_org_for_ranges = {
        # pd.read_csv(..., dtype=str) still converts a blank cell to float('nan'),
        # not "" - NaN is truthy in Python, so `org if org else ...` alone would
        # silently keep it and write the literal text "nan" into the CSV.
        entity: (org if isinstance(org, str) and org else organisation_name)
        for entity, org in post_entity_org.items()
    }
    # Mirror the blank-organisation suppression above: listed-building still gets
    # no registered range on single-source runs, but every other dataset now gets
    # a real per-entity organisation and needs its range recorded to match.
    if new_dataset_entities and not (scope == "single-source" and dataset == "listed-building"):
        entity_org_file = Path("pipeline") / collection_name / "entity-organisation.csv"
        for org_value, min_entity, max_entity in _contiguous_ranges_by_org(new_dataset_entities, entity_org_for_ranges):
            if (dataset, org_value) in ENTITY_ORGANISATION_RANGE_EXCLUSIONS:
                continue
            with open(entity_org_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([dataset, min_entity, max_entity, org_value])
                print(f"\033[95mAppended entity range {min_entity}-{max_entity} for {org_value} to {entity_org_file}\033[0m")


def _append_output_rows(output_df, output_rows):
    """Accumulate a resource's findings into the running summary frame."""
    return pd.concat([output_df, pd.DataFrame(output_rows)], ignore_index=True)


def _cleanup_resource_dir(resource_dir, successful_resources):
    """Delete successfully-processed resource files (and .gfs sidecars), then the
    resource directory itself if it's now empty."""
    for resource_path in successful_resources:
        try:
            if resource_path.exists():
                resource_path.unlink()
            gfs_path = resource_path.with_suffix(".gfs")
            if gfs_path.exists():
                gfs_path.unlink()
        except OSError as e:
            print(f"Failed to remove {resource_path} or its .gfs file: {e}")
    try:
        if not any(resource_dir.iterdir()):
            resource_dir.rmdir()
    except OSError as e:
        print(f"Failed to remove the resources directory: {e}")


def process_csv(scope, resource_dir, issue_summary_df, cache_dir, skip_checks=False, invalid_uri_issues=None, batch_size=0, start_batch=1, validation_config=None):
    """
    Uses provided file path to automatically process and assign unknown entities
    """
    validation_config = validation_config or ValidationConfig()
    resource_dir = Path(resource_dir)
    cache_dir = Path(cache_dir)
    failed_downloads = []
    successful_resources = []
    output_df = pd.DataFrame(columns=["dataset", "resource", "organisation", "reference", "status", "entities_created", "error_code", "message"])
    
    # Batch fetch all old resource hashes at once to reduce API calls
    unique_endpoints = issue_summary_df['endpoint'].unique().tolist()
    print(f"Fetching old resource hashes for {len(unique_endpoints)} unique endpoints...")
    endpoint_resource_map = get_old_resource_hashes_batch(unique_endpoints)
    print(f"Successfully retrieved {len(endpoint_resource_map)} old resource hashes")

    # Which endpoints have no history at all (genuinely brand new) vs real history that
    # just didn't resolve to a baseline above (a real fetch failure). None means the query
    # itself failed - fail safe by treating every endpoint as if it has history, same as
    # today's behaviour, rather than risk mistaking a network blip for "brand new".
    endpoints_with_history = get_endpoints_with_any_history(unique_endpoints)
    if endpoints_with_history is None:
        print("Could not determine which endpoints have history; treating all as having history")
        endpoints_with_history = set(unique_endpoints)

    try:
        pbar = tqdm(issue_summary_df.iterrows(), total=issue_summary_df.shape[0], desc="Processing resources")
        for row_number, row in pbar:
            start_time = perf_counter()
            collection_name = row["collection"]
            resource = row["resource"]
            endpoint = row["endpoint"]
            dataset = row["pipeline"]
            organisation_name = row["organisation"]
            download_link = row["download_link"]
            resource_path = Path(row["resource_path"])

            print("********************************************************************************************************************************")
            print("********************************************************************************************************************************")
            print(f"Collection_name > {collection_name}")
            print(f"Resource hash > {resource}")
            print(f"Endpoint hash > {endpoint}")
            print(f"Download_link > {download_link }")
            print(f"Resource path > {resource_path}")

            download_error = _fetch_resource(resource_path, download_link, resource)
            if download_error is not None:
                failed_downloads.append((resource, download_error))
                continue

            print(f"Processing resource: {resource}")

            input_path = cache_dir / "assign_entities" / "transformed" / f"{resource}.csv"
            lookup_path = Path("pipeline") / collection_name / "lookup.csv"
            try:
                pre_dataset_entities, old_resource_df, old_resource_hash, current_resource_df = _run_assignment(
                    resource_path,
                    resource,
                    endpoint,
                    collection_name,
                    dataset,
                    organisation_name,
                    scope,
                    cache_dir,
                    input_path,
                    lookup_path,
                    skip_checks,
                    endpoint_resource_map,
                )

                output_rows, invalid_uri_entities, new_entities = _run_validation(
                    current_resource_df,
                    old_resource_df,
                    old_resource_hash,
                    dataset,
                    resource,
                    organisation_name,
                    endpoint,
                    endpoints_with_history,
                    skip_checks,
                    validation_config,
                    cache_dir,
                    invalid_uri_issues,
                )

                if any(r.get('status') not in validation_config.non_blocking_statuses for r in output_rows):
                    output_df = _append_output_rows(output_df, output_rows)
                    continue

                output_rows.append(
                    {
                        "dataset": dataset,
                        "resource": resource,
                        "organisation": organisation_name,
                        "reference": "",
                        "status": "assigned",
                        "entities_created": len(new_entities),
                        "error_code": "",
                        "message": f"Entities assigned successfully. [{sorted(new_entities)[-5:]}]",
                    }
                )
                output_df = _append_output_rows(output_df, output_rows)

                _promote(
                    cache_dir,
                    collection_name,
                    dataset,
                    organisation_name,
                    scope,
                    lookup_path,
                    invalid_uri_entities,
                    pre_dataset_entities,
                )
                print(f"\nEntities assigned successfully for resource: {resource}. ")
                successful_resources.append(resource_path)

            except Exception as e:
                # An unhandled exception is a pipeline/engineering problem, not a data-quality
                # finding a reviewer can action - 'failed', same as current_resource_empty,
                # rather than 'blocked'.
                print(f"Failed to assign entities for resource: {resource}")
                logging.error(f"Error: {str(e)}", exc_info=True)
                output_df = pd.concat([output_df, pd.DataFrame([{
                    "dataset": dataset,
                    "resource": resource,
                    "status": "failed",
                    "error_code": type(e).__name__,
                    "message": str(e)
                }])], ignore_index=True)
            finally:
                print(f"\nCompleted processing for resource: {resource} in {perf_counter() - start_time:.2f} seconds.")
    finally:
        # entities_created is only ever set on the 'assigned' success row - every other
        # row dict (missing_organisation, duplicate checks, current_resource_empty, the
        # exception handler, etc.) omits the key, so pd.concat fills it with NaN rather
        # than a count of 0. Must happen before the CSV is written below, not after -
        # the written file is what Manage and the GitHub Actions artifact actually see.
        output_df["entities_created"] = output_df["entities_created"].fillna(0).astype(int)

        summary_filename = (
            f"batch_assign_summary_{scope}_batch_{start_batch}.csv"
            if batch_size > 0
            else f"batch_assign_summary_{scope}.csv"
        )
        output_df.to_csv(summary_filename, index=False)
        _cleanup_resource_dir(resource_dir, successful_resources)

    # Summary of results
    print("\n--- Summary Report ---")
    if failed_downloads:
        print("\nFailed Downloads:")
        for resource, error in failed_downloads:
            print(f"Resource: {resource} - Error: {error}")
    if not failed_downloads and output_df.empty:
        print("All operations completed successfully.")
    return failed_downloads, output_df


def get_scope(value, scope_dict):
    for scope, datasets in scope_dict.items():
        if value in datasets:
            return scope
    return "single-source"


def ensure_specification_dir(specification_dir: Path = Path("specification")) -> Path:
    specification_dir.mkdir(parents=True, exist_ok=True)
    Specification.download(specification_dir)
    return specification_dir

def run_batch_assign_entities(
    scope: str = 'odp',
    cache_dir: Path = Path("var/cache/"),
    resources: Optional[str] = None,
    skip_checks: bool = False,
    triggered_by: Optional[str] = None,
    commit: bool = True,
    batch_size: int = 0,
    start_batch: int = 1,
):
    endpoint_issue_summary_path = "https://datasette.planning.data.gov.uk/performance/endpoint_dataset_issue_type_summary.csv?_sort=rowid&issue_type__exact=unknown+entity&_size=max"

    issue_summary_df = fetch_csv_with_retry(endpoint_issue_summary_path)

    invalid_uri_issues_path = "https://datasette.planning.data.gov.uk/performance/endpoint_dataset_issue_type_summary.csv?_sort=rowid&issue_type__exact=invalid+URI&_size=max"
    invalid_uri_issues = fetch_csv_with_retry(invalid_uri_issues_path)
    invalid_uri_issues.to_csv("invalid_uri_issues.csv", index=False)
    
    specification_dir = ensure_specification_dir()
    provision_rule_df = pd.read_csv(specification_dir / "provision-rule.csv",dtype=str)
    scope_dict = {
        "odp": provision_rule_df.loc[
            provision_rule_df["project"] == "open-digital-planning", "dataset"
        ].tolist(),
        "mandated": provision_rule_df.loc[
            (provision_rule_df["provision-reason"] == "statutory")
            | (
                (provision_rule_df["provision-reason"] == "encouraged")
                & (provision_rule_df["role"] == "local-planning-authority")
            ),
            "dataset",
        ].tolist(),
    }

    issue_summary_df["scope"] = issue_summary_df["dataset"].apply(lambda x: get_scope(x, scope_dict))
    issue_summary_df.to_csv("issue_summary_full.csv", index=False)

    if triggered_by:
        print(f"Triggered by: {triggered_by}")
    print("issue_summary.csv downloaded successfully")
    
    # Build url_map from the CSV data
    resource_dir = Path("./resource")
    resource_dir.mkdir(exist_ok=True)
    
    issue_summary_df=issue_summary_df.loc[
        (issue_summary_df["issue_type"].str.lower() == "unknown entity") &
        (issue_summary_df["scope"].str.lower() == scope) &
        (issue_summary_df["dataset"].str.lower() != "title-boundary") &
        # planning-application collection is paused, so it shouldn't be batch-assigned
        (issue_summary_df["dataset"].str.lower() != "planning-application")
    ]

    if resources:
        resource_set = {r.strip() for r in resources.split(",") if r.strip()}
        issue_summary_df = issue_summary_df[issue_summary_df["resource"].isin(resource_set)]

    # Deterministic ordering so batch numbers are stable across runs
    issue_summary_df = issue_summary_df.sort_values("resource").reset_index(drop=True)

    if batch_size > 0:
        total_batches = math.ceil(len(issue_summary_df) / batch_size)
        start_idx = (start_batch - 1) * batch_size
        if start_idx >= len(issue_summary_df):
            print(
                f"Batch {start_batch} is out of range — "
                f"only {total_batches} batch(es) exist for scope '{scope}'. Exiting."
            )
            sys.exit(2)
        issue_summary_df = issue_summary_df.iloc[start_idx : start_idx + batch_size]
        print(
            f"Processing batch {start_batch} of {total_batches} "
            f"({len(issue_summary_df)} resources)"
        )

    if issue_summary_df.empty:
        print(f"No resources found with unknown entity issues for scope '{scope}'. Exiting.")
        return
    
    issue_summary_df[["download_link", "resource_path"]] = issue_summary_df.apply(
        lambda row: pd.Series({
            "download_link": f"https://files.planning.data.gov.uk/{row['collection']}-collection/collection/resource/{row['resource']}",
            "resource_path": str(resource_dir / row["resource"])
        }), axis=1
    )
    
    issue_summary_df.to_csv("issue_summary.csv", index=False)
    url_map = dict(zip(issue_summary_df["download_link"], issue_summary_df["resource_path"]))
    
    # Add organisation.csv to download
    cache_dir_path = Path(cache_dir)
    cache_dir_path.mkdir(parents=True, exist_ok=True)
    url_map["https://files.planning.data.gov.uk/organisation-collection/dataset/organisation.csv"] = str(cache_dir_path / "organisation.csv")
    
    download_urls(url_map, max_threads=4)

    try:
        failed_downloads, output_df = process_csv(
            scope,
            resource_dir,
            issue_summary_df,
            cache_dir,
            skip_checks,
            invalid_uri_issues,
            batch_size=batch_size,
            start_batch=start_batch
        )
        # 'failed' (e.g. current_resource_empty) is a blocking outcome like 'blocked', just a
        # distinct label for cases that need an engineer rather than a data reviewer.
        error_count = len(output_df[output_df['status'].isin(['blocked', 'failed'])])
        success_count = len(output_df[output_df['status'] == 'assigned'])

        print(f"\nTotal failed downloads: {len(failed_downloads)}")
        print(f"Total failed assign-entities operations: {error_count}")
        print(f"Total successful assign-entities operations: {success_count}")

        if success_count > 0 and commit:
            commit_to_main(
                triggered_by=triggered_by,
                success_count=success_count,
                scope=scope,
                batch_size=batch_size,
                start_batch=start_batch,
            )
        elif success_count > 0 and not commit:
            print("Successful assignments completed; --no-commit set, skipping commit to main")
        else:
            print("No successful assignments; skipping commit to main")
    except Exception as e:
        print(f"Error running batch assign entities: {e}")
        traceback.print_exc()
        raise e
    
@click.command(help="Automatically assign entities for resources with unknown entity issues based on issue summary from performance dataset. This script will download the relevant resources, attempt to assign entities, and handle any errors that occur during the process.")
@click.option(
    "--scope",
    default='odp',
    show_default=True,
    type=click.Choice(["odp", "mandated", "single-source"], case_sensitive=False),
    help="The scope of datasets to process; must be one of 'odp', 'mandated', or 'single-source'",
)
@click.option(
    "--triggered-by",
    required=False,
    type=str,
    help="Identifier for the actor/system that triggered this run.",
)
@click.option(
    "--resources",
    required=False,
    type=str,
    help="Comma-separated list of resource hashes to process.",
)
@click.option(
    "--skip-checks",
    is_flag=True,
    default=False,
    help="Skip validation checks and directly assign entities.",
)
@click.option(
    "--commit/--no-commit",
    default=True,
    show_default=True,
    help="Commit changes to main when there are successful assignments.",
)
@click.option(
    "--batch-size",
    default=0,
    type=int,
    show_default=True,
    help="Number of resources to process per run. 0 = process all (default).",
)
@click.option(
    "--start-batch",
    default=1,
    type=int,
    show_default=True,
    help="1-indexed batch number to start from. Use with --batch-size to resume a failed run.",
)

def main(
    scope: str = 'odp',
    cache_dir: str = "var/cache/",
    resources: Optional[str] = None,
    skip_checks: bool = False,
    triggered_by: Optional[str] = None,
    commit: bool = True,
    batch_size: int = 0,
    start_batch: int = 1,
) -> None:
    # Print input options so the command and options used are visible
    print("Input options:")
    print(f"  scope={scope}")
    print(f"  cache_dir={cache_dir}")
    print(f"  resources={resources}")
    print(f"  skip_checks={skip_checks}")
    print(f"  triggered_by={triggered_by}")
    print(f"  commit={commit}")
    print(f"  batch_size={batch_size}")
    print(f"  start_batch={start_batch}")

    cache_dir = Path(cache_dir)
    run_batch_assign_entities(
        scope=scope,
        cache_dir=cache_dir,
        resources=resources,
        skip_checks=skip_checks,
        triggered_by=triggered_by,
        commit=commit,
        batch_size=batch_size,
        start_batch=start_batch,
    )

if __name__ == "__main__":
    main()
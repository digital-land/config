import csv
import math
import sys
from time import perf_counter
import click
import requests
import pandas as pd
import shutil
import logging
import traceback
import subprocess
import re

from pathlib import Path
from typing import Optional, Dict
from io import StringIO
from digital_land.commands import check_and_assign_entities
from digital_land.specification import Specification

from tqdm import tqdm
from urllib.request import urlretrieve
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


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
    
    query = f"""SELECT endpoint, resource
FROM (
    SELECT endpoint, resource, resource_end_date,
    ROW_NUMBER() OVER (
        PARTITION BY endpoint
        ORDER BY rowid
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
        response = requests.get(url)
        response.raise_for_status()
        result_df = pd.read_csv(StringIO(response.text), dtype=str, low_memory=False)
        
        # Create a mapping of endpoint -> resource
        endpoint_resource_map = dict(zip(result_df['endpoint'], result_df['resource']))
        return endpoint_resource_map
    except Exception as e:
        logger.error(f"Error fetching old resource hashes: {e}")
        return {}


def get_old_resource_df_from_hash(resource_hash: str, collection_name: str, dataset: str):
    """
    Fetch the transformed CSV for an old resource given its hash.
    """
    try:
        transformed_url = (
            f"https://files.planning.data.gov.uk/{collection_name}-collection/transformed/{dataset}/{resource_hash}.csv"
        )
        transformed_response = requests.get(transformed_url)
        transformed_response.raise_for_status()
        
        # Save the old transformed resource to resource/old before reading
        old_resource_dir = Path("resource") / "old"
        old_resource_dir.mkdir(parents=True, exist_ok=True)
        old_file_path = old_resource_dir / f"{resource_hash}.csv"
        old_file_path.write_bytes(transformed_response.content)
        
        
        return pd.read_csv(old_file_path, dtype=str, low_memory=False)
    except Exception as e:
        logger.error(f"Error downloading old resource {resource_hash}: {e}")
        return None


                            
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
            'status': 'error',
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


def _duplicate_prefix_reference_organisation_error_rows(old_df, new_df, dataset, resource):
    """
        Check for duplicate entities based on prefix, reference and organisation fields only.
    """
    
    matches = new_df.merge(old_df, on='fingerprint', how='inner', suffixes=('_new', '_old'))
    return _duplicate_error_rows(
        matches,
        dataset,
        resource,
        'duplicate_prefix_reference_organisation',
        lambda match_row: (
            f"Entity exists with the same prefix, reference and organisation"
            f" {match_row['entity_list']} {match_row['fingerprint']}."
        ),
    )


def _duplicate_reference_organisation_error_rows(old_df, new_df, dataset, resource):
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
    duplicates = _make_fingerprints(df, except_fields=[], only_fields=['organisation', 'reference'])
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
            'status': 'error',
            'error_code': 'duplicate_reference_organisation_in_new_resource',
            'message': f"Duplicate reference and organisation found in resource {dup_row['fingerprint']}.",
        }
        for _, dup_row in duplicates.iterrows()
    ]


def _missing_organisation_error_rows(df, dataset, resource):
    missing = df[(df['organisation'].isna()) | (df['organisation'] == '')]
    return [
        {
            'dataset': dataset,
            'resource': resource,
            'organisation': missing_row.get('organisation', ''),
            'reference': missing_row.get('reference', ''),
            'status': 'error',
            'error_code': 'missing_organisation',
            'message': (
                f"Missing organisation for entity {missing_row.get('entity')} "
                f"with reference {missing_row.get('reference')} in current transformed rows."
            ),
        }
        for _, missing_row in missing.iterrows()
    ]


def _missing_reference_error_rows(df, dataset, resource):
    missing = df[(df['reference'].isna()) | (df['reference'] == '')]
    return [
        {
            'dataset': dataset,
            'resource': resource,
            'organisation': missing_row.get('organisation', ''),
            'reference': missing_row.get('reference', ''),
            'status': 'error',
            'error_code': 'missing_reference',
            'message': (
                f"Missing reference for entity {missing_row.get('entity')} "
                f"with organisation {missing_row.get('organisation')} in current transformed rows."
            ),
        }
        for _, missing_row in missing.iterrows()
    ]


def _collect_validation_rows(current_resource_df, old_resource_df, dataset, resource, new_entity_threshold, old_resource_hash,organisation_name=''):
    validation_rows = []
    current_entities = set(current_resource_df['entity'])

    if old_resource_df is None:
        old_entities = set()
        new_entity_ids = current_entities
    else:
        old_entities = set(old_resource_df['entity'])
        new_entity_ids = current_entities - old_entities
    
    print(f"Old entities count: {len(old_entities)}, current entities count: {len(current_entities)}, New entities count: {len(new_entity_ids)}")
    print(f"Last 5 old entities IDs: {sorted(old_entities)[-5:] if old_entities else 'N/A'}")
    print(f"Last 5 current entities IDs: {sorted(current_entities)[-5:] if current_entities else 'N/A'}")
    print(f"Last 5 new entities IDs: {sorted(new_entity_ids)[-5:] if new_entity_ids else 'N/A'}")

    if old_resource_df is None:
        validation_rows.append(
            {
                'dataset': dataset,
                'resource': resource,
                'organisation': organisation_name,
                'reference': '',
                'status': 'error',
                'error_code': 'previous_resource_not_found',
                'message': 'Previous resource not found.',
            }
        )
    elif len(old_entities) == 0:
        validation_rows.append(
            {
                'dataset': dataset,
                'resource': resource,
                'organisation': organisation_name,
                'reference': '',
                'status': 'error',
                'error_code': 'previous_resource_empty',
                'message': f'Previous resource has no entities {old_resource_hash}.',
            }
        )
    
    if len(new_entity_ids) == 0:
        validation_rows.append(
            {
                'dataset': dataset,
                'resource': resource,
                'organisation': organisation_name,
                'reference': '',
                'status': 'error',
                'error_code': 'current_resource_no_new_entities',
                'message': 'Current resource contains no new entities for assignment.',
            }
        )

    # The following checks require previous resource entities/facts to compare against
    if len(old_entities) > 0 and len(new_entity_ids) > 0:
        new_resource_only_df = current_resource_df[current_resource_df['entity'].isin(new_entity_ids)]

        # If the number of new entities exceeds the threshold compared to the previous resource, flag for review
        if len(new_entity_ids) > (new_entity_threshold / 100) * len(current_entities):
            validation_rows.append(
                {
                    'dataset': dataset,
                    'resource': resource,
                    'organisation': organisation_name,
                    'reference': '',
                    'status': 'error',
                    'error_code': 'large_number_of_new_entities',
                    'message': (
                        f"Resource contains a large number of new entities ({len(new_entity_ids)}) "
                        f"compared to the previous version ({len(old_entities)}). "
                        f"Old resource hash: {old_resource_hash}."
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
        
        # Check for duplicate entities based on prefix, reference and organisation fields only between the new resource and old resource
        validation_rows.extend(
            _duplicate_prefix_reference_organisation_error_rows(
                _make_fingerprints(old_resource_df, except_fields=[], only_fields=['prefix', 'organisation', 'reference']),
                _make_fingerprints(new_resource_only_df, except_fields=[], only_fields=['prefix', 'organisation', 'reference']),
                dataset,
                resource,
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


def process_csv(scope, resource_dir, issue_summary_df, cache_dir, new_entity_threshold=10, skip_checks=False, invalid_uri_issues=None, batch_size=0, start_batch=1):
    """
    Uses provided file path to automatically process and assign unknown entities
    """
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

            if not resource_path.is_file():
                try:
                    print(f"Resource  file not found locally, attempting to download from {download_link}")
                    response = requests.get(download_link)
                    response.raise_for_status()
                    resource_path.write_bytes(response.content)
                    print(f"Downloaded: {resource}")
                except requests.RequestException as e:
                    print(f"Failed to download: {resource}")
                    print(f"Error: {e}")
                    failed_downloads.append((resource, str(e)))
                continue

            print(f"Processing resource: {resource}")
            collection_path = Path(f"collection/{collection_name}")

            input_path = cache_dir / "assign_entities" / "transformed" / f"{resource}.csv"
            lookup_path = Path("pipeline") / collection_name / "lookup.csv"
            try:
                # Snapshot existing entities for this org before assignment
                pre_lookup_df = pd.read_csv(lookup_path,dtype=str)
                pre_org_entities = set(
                    pre_lookup_df[
                        (pre_lookup_df["prefix"] == dataset) &
                        (pre_lookup_df["organisation"] == organisation_name)
                    ]["entity"].dropna().astype(int)
                )
                check_and_assign_entities(
                    [resource_path],
                    [endpoint],
                    collection_name,
                    dataset,
                    [organisation_name],
                    collection_path,
                    cache_dir.joinpath("organisation.csv"),
                    Path("specification"),
                    Path(f"pipeline/{collection_name}"),
                    input_path,
                    prompt_user=False,
                )

                output_rows = []

                def add_output_log(rows):
                    if rows:
                        output_rows.extend(rows)

                # get old transformed resource using pre-fetched resource hash when checks are enabled
                old_resource_df = None
                old_resource_hash = None
                if not skip_checks and endpoint in endpoint_resource_map:
                    old_resource_hash = endpoint_resource_map[endpoint]

                    print(f"=====")
                    print(f" collection || dataset || old_resource_hash || endpoint")
                    print(f" {collection_name} || {dataset} || {old_resource_hash} || {endpoint}")
                    old_resource_df = get_old_resource_df_from_hash(old_resource_hash, collection_name, dataset)
                
                # get current transformed resource
                current_resource_df = pd.read_csv(cache_dir / "assign_entities" / "transformed" / f"{resource}.csv",dtype=str)

                if not skip_checks and len(current_resource_df) == 0:
                    add_output_log([
                        {
                            "dataset": dataset,
                            "resource": resource,
                            "organisation": organisation_name,
                            "reference": "",
                            "status": "error",
                            "error_code": "current_resource_empty",
                            "message": "Current resource has no entities for assignment.",
                        }
                    ])
                    output_df = pd.concat(
                        [output_df, pd.DataFrame(output_rows)],
                        ignore_index=True,
                    )
                    continue

                if skip_checks:
                    old_entities = set()
                    new_entities = set(current_resource_df['entity'])
                    validation_rows = []
                else:
                    validation_rows, old_entities, new_entities = _collect_validation_rows(
                        current_resource_df,
                        old_resource_df,
                        dataset,
                        resource,
                        new_entity_threshold,
                        old_resource_hash,
                        organisation_name=organisation_name
                    )
                    add_output_log(validation_rows)
                    
                    if len(output_rows) == 0:
                        iui = invalid_uri_issues[
                                    (invalid_uri_issues["resource"] == resource)
                                    & (invalid_uri_issues["dataset"] == dataset)
                                ]
                        if len(iui) > 0:
                            add_output_log([
                                {
                                    "dataset": dataset,
                                    "resource": resource,
                                    "organisation": organisation_name,
                                    "reference": "",
                                    "status": "error",
                                    "error_code": "invalid_uri_issue",
                                    "message": f"Resource has known issues with invalid URIs that require manual review.",
                                }
                            ])

                if output_rows:
                    output_df = pd.concat(
                        [output_df, pd.DataFrame(output_rows)],
                        ignore_index=True,
                    )
                    continue

                add_output_log(
                    [
                        {
                            "dataset": dataset,
                            "resource": resource,
                            "organisation": organisation_name,
                            "reference": "",
                            "status": "success",
                            "entities_created": len(new_entities),
                            "error_code": "",
                            "message": f"Entities assigned successfully. [{sorted(new_entities)[-5:]}]",
                        }
                    ]
                )
                output_df = pd.concat(
                    [output_df, pd.DataFrame(output_rows)],
                    ignore_index=True,
                )
                shutil.copy(cache_dir / "assign_entities" / collection_name / "pipeline" / "lookup.csv", Path("pipeline") / collection_name / "lookup.csv")
                print(f"\nEntities assigned successfully for resource: {resource}. ")
                successful_resources.append(resource_path)

                # After successful entity assignment and duplicate checks append entity range to entity-organisation.csv
                post_lookup_df = pd.read_csv(lookup_path, dtype=str)
                post_org_entities = set(
                    post_lookup_df[
                        (post_lookup_df["prefix"] == dataset) &
                        (post_lookup_df["organisation"] == organisation_name)
                    ]["entity"].dropna().astype(int)
                )
                org_new_entities = post_org_entities - pre_org_entities
                if org_new_entities:
                    min_entity = min(org_new_entities)
                    max_entity = max(org_new_entities)
                    entity_org_file = Path("pipeline") / collection_name / "entity-organisation.csv"
                    # Hard code single exception for conservation-area dataset org HE
                    if not (dataset == "conservation-area" and organisation_name == "government-organisation:PB1164"):
                        with open(entity_org_file, "a", newline="") as f:
                            writer = csv.writer(f)
                            writer.writerow([dataset, min_entity, max_entity, organisation_name])
                            print(f"\033[95mAppended entity range {min_entity}-{max_entity} for {organisation_name} to {entity_org_file}\033[0m")

            except Exception as e:
                print(f"Failed to assign entities for resource: {resource}")
                logging.error(f"Error: {str(e)}", exc_info=True)
                output_df = pd.concat([output_df, pd.DataFrame([{
                    "dataset": dataset,
                    "resource": resource,
                    "status": "error",
                    "error_code": type(e).__name__,
                    "message": str(e)
                }])], ignore_index=True)
            finally:
                print(f"\nCompleted processing for resource: {resource} in {perf_counter() - start_time:.2f} seconds.")
    finally:
        summary_filename = (
            f"batch_assign_summary_{scope}_batch_{start_batch}.csv"
            if batch_size > 0
            else f"batch_assign_summary_{scope}.csv"
        )
        output_df.to_csv(summary_filename, index=False)
        # Remove successfully processed resources
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
    new_entity_threshold: int = 10,
    resources: Optional[str] = None,
    skip_checks: bool = False,
    triggered_by: Optional[str] = None,
    commit: bool = True,
    batch_size: int = 0,
    start_batch: int = 1,
):
    endpoint_issue_summary_path = "https://datasette.planning.data.gov.uk/performance/endpoint_dataset_issue_type_summary.csv?_sort=rowid&issue_type__exact=unknown+entity&_size=max"

    response = requests.get(endpoint_issue_summary_path)
    issue_summary_df = pd.read_csv(StringIO(response.text),dtype=str)
    
    invalid_uri_issues_path = "https://datasette.planning.data.gov.uk/performance/endpoint_dataset_issue_type_summary.csv?_sort=rowid&issue_type__exact=invalid+URI&_size=max"
    invalid_uri_response = requests.get(invalid_uri_issues_path)
    invalid_uri_issues = pd.read_csv(StringIO(invalid_uri_response.text),dtype=str)
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
        (issue_summary_df["dataset"].str.lower() != "title-boundary")
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
            new_entity_threshold,
            skip_checks,
            invalid_uri_issues,
            batch_size=batch_size,
            start_batch=start_batch
        )
        error_count = len(output_df[output_df['status'] == 'error'])
        success_count = len(output_df[output_df['status'] == 'success'])

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
    "--new-entity-threshold",
    default=10,
    show_default=True,
    type=click.IntRange(min=0, max=100),
    help="The threshold for the number of new entities to be assigned in percentage compared to the previous version of the resource.",
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
    new_entity_threshold: int = 10,
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
    print(f"  new_entity_threshold={new_entity_threshold}")
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
        new_entity_threshold=new_entity_threshold,
        resources=resources,
        skip_checks=skip_checks,
        triggered_by=triggered_by,
        commit=commit,
        batch_size=batch_size,
        start_batch=start_batch,
    )

if __name__ == "__main__":
    main()
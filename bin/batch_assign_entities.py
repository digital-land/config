import csv
import click
import requests
import sys
import pandas as pd
import os
import shutil
import logging
import traceback
import subprocess

from pathlib import Path
from typing import Optional
from io import StringIO
from digital_land.commands import check_and_assign_entities
from digital_land.specification import Specification

from tqdm import tqdm
from urllib.request import urlretrieve
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("__name__")


def run_command(cmd, capture_output=False, check=True):
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


def checkout_branch_for_create_mode(branch_name):
    current_branch = run_command(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
    )
    if current_branch == branch_name:
        return

    local_exists = subprocess.run(
        ["git", "show-ref", "--verify", f"refs/heads/{branch_name}"],
        text=True,
        capture_output=True,
        check=False,
    ).returncode == 0

    if local_exists:
        run_command(["git", "checkout", branch_name])
    else:
        run_command(["git", "checkout", "-b", branch_name])


def create_or_update_pr_for_success(branch, triggered_by, success_count):
    if not branch:
        print("No --branch supplied; skipping PR creation")
        return

    commit_label = f"Batch assign entities update ({success_count} successful resource(s))"

    pr_body = (
        f"{commit_label}\n\n"
        f"Triggered by: {triggered_by or 'unknown'}\n"
        f"Branch: {branch}"
    )

    run_command(["git", "config", "user.name", "github-actions-add-data-bot"])
    run_command(["git", "config", "user.email", "matthew.poole@communities.gov.uk"])
    checkout_branch_for_create_mode(branch)

    run_command(["git", "add", "pipeline/"])
    run_command(["git", "add", "collection/"], check=False)

    staged_changes = subprocess.run(
        ["git", "diff", "--staged", "--quiet"],
        check=False,
    ).returncode != 0

    if not staged_changes:
        print("No staged changes after batch assignment; skipping PR creation")
        return

    run_command(["git", "commit", "-m", commit_label])
    run_command(["git", "push", "origin", branch])

    pr_number = run_command(
        [
            "gh",
            "pr",
            "list",
            "--head",
            branch,
            "--state",
            "open",
            "--json",
            "number",
            "--jq",
            ".[0].number // empty",
        ],
        capture_output=True,
    )

    if pr_number:
        current_body = run_command(
            ["gh", "pr", "view", pr_number, "--json", "body", "--jq", ".body"],
            capture_output=True,
        )
        new_body = f"{current_body}\n\n{pr_body}" if current_body else pr_body
        run_command(["gh", "pr", "edit", pr_number, "--title", "Batch Assign Entities Update", "--body", new_body])
        print(f"Updated existing PR #{pr_number} on branch {branch}")
        return

    run_command(
        [
            "gh",
            "pr",
            "create",
            "--title",
            "Batch Assign Entities Update",
            "--body",
            pr_body,
            "--base",
            "main",
            "--head",
            branch,
        ]
    )
    print(f"Created PR on branch {branch}")

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
                logger.errors(f"Error during download: {e}")
        return results

def get_old_resource_df(endpoint, collection_name, dataset):
    """
    returns transformed file for second latest resource using endpoint hash from CDN
    """
    url = (
        f"https://datasette.planning.data.gov.uk/performance/reporting_historic_endpoints.csv"
        f"?_sort=rowid&resource_end_date__notblank=1&endpoint__exact={endpoint}&_size=1"
    )
    response = requests.get(url)
    response.raise_for_status()
    previous_resource_df = pd.read_csv(StringIO(response.text))
    if len(previous_resource_df) == 0:
        return None

    old_resource_hash = previous_resource_df['resource'][0]
    print(f"=====")
    print(f" collection || dataset || old_resource_hash")
    print(f" {collection_name} || {dataset} || {old_resource_hash}")
    transformed_url = (
        f"https://files.planning.data.gov.uk/{collection_name}-collection/transformed/{dataset}/{old_resource_hash}.csv"
    )
    transformed_response = requests.get(transformed_url)
    transformed_response.raise_for_status()
    return pd.read_csv(StringIO(transformed_response.text))


                            
def _make_fingerprints(df,except_fields=["reference", "entry-date"],only_fields=None):
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
                        


def process_csv(scope, resource_dir, issue_summary_df, cache_dir, new_entity_threshold=10, skip_checks=False):
    """
    Uses provided file path to automatically process and assign unknown entities
    """
    failed_downloads = []
    successful_resources = []
    output_df = pd.DataFrame(columns=["dataset", "resource", "organisation", "reference","status","entities_created", "error_code", "message"])
    try:
        pbar = tqdm(issue_summary_df.iterrows(), total=issue_summary_df.shape[0], desc="Processing resources")
        for row_number, row in pbar:
            collection_name = row["collection"]
            resource = row["resource"]
            endpoint = row["endpoint"]
            dataset = row["pipeline"]
            organisation_name = row["organisation"]
            download_link = row["download_link"]
            resource_path = Path(row["resource_path"])
            cache_dir=Path(cache_dir)
            
            
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
                continue

            print(f"Processing resource: {resource}")
            collection_path = Path(f"collection/{collection_name}")

            input_path = cache_dir / "assign_entities" / "transformed" / f"{resource}.csv"
            lookup_path = Path("pipeline") / collection_name / "lookup.csv"
            try:
                # Snapshot existing entities for this org before assignment
                pre_lookup_df = pd.read_csv(lookup_path)
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
                    prompt_user=False
                )
                issue_df = pd.read_csv(cache_dir / "assign_entities" /"issue" / f"{resource}.csv")
                
                output_rows = []

                def add_output_log(rows):
                    if rows:
                        output_rows.extend(rows)

                # get old transformed resource
                old_resource_df = get_old_resource_df(endpoint,collection_name,dataset)
                # get current transformed resource
                current_resource_df = pd.read_csv(cache_dir / "assign_entities" / "transformed" / f"{resource}.csv")
                new_entities = set(current_resource_df['entity'])
                current_entities = set(current_resource_df['entity'])
                old_entities = set()
                
                if not skip_checks and len(current_resource_df) == 0:
                    add_output_log([
                            {
                                "dataset": dataset,
                                "resource": resource,
                                "organisation": organisation_name,
                                "reference": "",
                                "status": "error",
                                "error_code": "current_resource_empty",
                                "message": f"Current resource has no entities for assignment."
                            }
                        ])
                    output_df = pd.concat(
                        [output_df, pd.DataFrame(output_rows)],
                        ignore_index=True,
                    )
                        
                    successful_resources.append(resource_path)
                    continue
                
                
                if(old_resource_df is not None):
                    # we get the old entities from the old transformed resource to compare against new entities in current transformed resource for validation checks. 
                    old_entities = set(old_resource_df['entity'])
                    # store new entities in current_resource_df
                    new_entities = list(current_entities - old_entities)
                    
                
                if not skip_checks:
                    # There is a possibility that the previous resource had no entities. 
                    if len(old_entities) < 1 and old_resource_df is not None:
                        add_output_log([
                                {
                                    "dataset": dataset,
                                    "resource": resource,
                                    "organisation": organisation_name,
                                    "reference": "",
                                "status": "error",
                                    "error_code": "previous_resource_empty",
                                    "message": f"Previous resource is has no entities."
                                }
                            ])
                    

                    # perform checks that require the previous resource or existing entities
                    if len(old_entities) > 0 and len(new_entities) > 0:
                            
                        new_resource_only_df = current_resource_df[current_resource_df['entity'].isin(new_entities)]
                        
                        if len(new_entities) > (new_entity_threshold / 100) * len(current_entities):
                            add_output_log(
                                [
                                    {
                                        "dataset": dataset, 
                                        "resource": resource,
                                        "organisation": organisation_name,
                                        "status": "error",
                                        "error_code": "large_number_of_new_entities",
                                        "message": f"Resource contains a large number of new entities ({len(new_entities)}) compared to the previous version ({len(old_entities)})."
                                    }
                                ]
                            )
                        

                        old_fp = _make_fingerprints(old_resource_df)
                        cur_fp = _make_fingerprints(new_resource_only_df)
                        
                        # join on fingerprint to find exact content matches (many-to-many possible)
                        matches = cur_fp.merge(old_fp, on='fingerprint', how='inner', suffixes=('_new', '_old'))
                        if not matches.empty:
                            matches["entity_list"] = (
                                matches.groupby("fingerprint")["entity_old"]
                                .transform(lambda x: ", ".join(x.astype(str)))
                            )
                            matches = matches.drop_duplicates("fingerprint")
                            add_output_log([
                                {
                                    "dataset": dataset,
                                    "resource": resource,
                                    "organisation": match_row["organisation_new"],
                                    "reference": match_row["reference_new"],
                                    "status": "error",
                                    "error_code": "duplicate_entity_all_fields",
                                    "message": (
                                        f"Matches existing entity(s) "
                                        f"{match_row['entity_list']} "
                                        f"{match_row['fingerprint']}."
                                    )
                                }
                                for fingerprint, match_row in matches.iterrows()
                            ])
                            
                                
                        dup_old_df=_make_fingerprints(old_resource_df, except_fields=[], only_fields=["prefix", "organisation", "reference"])
                        dup_new_df=_make_fingerprints(new_resource_only_df, except_fields=[], only_fields=["prefix", "organisation", "reference"])
                        
                        matches = dup_new_df.merge(dup_old_df, on='fingerprint', how='inner', suffixes=('_new', '_old'))
                        if not matches.empty:
                            matches["entity_list"] = (
                                matches.groupby("fingerprint")["entity_old"]
                                .transform(lambda x: ", ".join(x.astype(str)))
                            )
                            matches = matches.drop_duplicates("fingerprint")
                            add_output_log([
                                {
                                    "dataset": dataset,
                                    "resource": resource,
                                    "organisation": match_row["organisation_new"],
                                    "reference": match_row["reference_new"],
                                    "status": "error",
                                    "error_code": "duplicate_prefix_reference_organisation",
                                    "message":( f"Entity exists with the same prefix, reference and organisation"
                                                f"{match_row['entity_list']} "
                                                f"{match_row['fingerprint']}.")
                                }
                                for _, match_row in matches.iterrows()
                            ])

                        # Duplicate reference for the same organisation in existing entities
                        dup_ref_org_old_df = _make_fingerprints(old_resource_df, except_fields=[], only_fields=["organisation", "reference"])
                        dup_ref_org_new_df = _make_fingerprints(new_resource_only_df, except_fields=[], only_fields=["organisation", "reference"])
                        
                        matches = dup_ref_org_new_df.merge(dup_ref_org_old_df, on='fingerprint', how='inner', suffixes=('_new', '_old'))
                        
                        if not matches.empty:
                            matches["entity_list"] = (
                                matches.groupby("fingerprint")["entity_old"]
                                .transform(lambda x: ", ".join(x.astype(str)))
                            )
                            matches = matches.drop_duplicates("fingerprint")
                            add_output_log([
                                {
                                    "dataset": dataset,
                                    "resource": resource,
                                    "organisation": match_row["organisation_new"],
                                    "reference": match_row["reference_new"],
                                "status": "error",
                                    "error_code": "duplicate_reference_organisation",
                                    "message": (f"Entity exists with the same reference and organisation"
                                                f"{match_row['entity_list']} "
                                                f"{match_row['fingerprint']}.")
                                }
                                for _, match_row in matches.iterrows()
                            ])
                        
                    # Duplicate reference for the same organisation in new entities only (not in old resource)
                    dup_ref_org_new_only_df = _make_fingerprints(current_resource_df, except_fields=[], only_fields=["organisation", "reference"])
                    dup_ref_org_new_only_df = dup_ref_org_new_only_df[
                        dup_ref_org_new_only_df.duplicated("fingerprint", keep=False)
                    ].drop_duplicates("fingerprint")

                    if not dup_ref_org_new_only_df.empty:
                        
                        add_output_log([
                            {
                                "dataset": dataset,
                                "resource": resource,
                                "organisation": dup_row["organisation"],
                                "reference": dup_row["reference"],
                                "status": "error",
                                "error_code": "duplicate_reference_organisation_in_new_resource",
                                "message": f"Duplicate reference and organisation found in resource {dup_row['fingerprint']}."
                            }
                            for _, dup_row in dup_ref_org_new_only_df.iterrows()
                        ])
                        
                    field_values = current_resource_df[
                        current_resource_df['field'].isin(['organisation', 'reference', 'prefix'])
                    ][['entity', 'field', 'value']].drop_duplicates()
                    fp = field_values.pivot_table(index='entity', columns='field', values='value', aggfunc='first').reset_index()

                    # Missing organisation in current transformed rows
                    missing_organisation_df = fp[(fp['organisation'].isna()) | (fp['organisation'] == '')]
                    
                    if not missing_organisation_df.empty:
                        add_output_log([
                            {
                                'dataset': dataset,
                                'resource': resource,
                                'organisation': missing_org_row.get('organisation', ''),
                                'reference': missing_org_row.get('reference', ''),
                                'status': 'error',
                                'error_code': 'missing_organisation',
                                'message': f"Missing organisation for entity {missing_org_row.get('entity')} with reference {missing_org_row.get('reference')} in current transformed rows."
                            }
                            for _, missing_org_row in missing_organisation_df.iterrows()
                        ])

                    # Missing reference in current transformed rows
                    missing_reference_df = fp[(fp['reference'].isna()) | (fp['reference'] == '')]
                    if not missing_reference_df.empty:
                        add_output_log([
                            {
                                'dataset': dataset,
                                'resource': resource,
                                'organisation': missing_ref_row.get('organisation', ''),
                                'reference': missing_ref_row.get('reference', ''),
                                'status': 'error',
                                'error_code': 'missing_reference',
                                'message': f"Missing reference for entity {missing_ref_row.get('entity')} with organisation {missing_ref_row.get('organisation')} in current transformed rows."
                            }
                            for _, missing_ref_row in missing_reference_df.iterrows()
                        ])

                if output_rows:
                    output_df = pd.concat(
                        [output_df, pd.DataFrame(output_rows)],
                        ignore_index=True,
                    )
                        
                    successful_resources.append(resource_path)
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
                            "message": f"Entities assigned successfully."
                        }
                    ]
                )
                output_df = pd.concat(
                    [output_df, pd.DataFrame(output_rows)],
                    ignore_index=True,
                )
                shutil.copy(cache_dir / "assign_entities" / collection_name / "pipeline" / "lookup.csv", Path("pipeline") / collection_name / "lookup.csv")
                print(f"\nEntities assigned successfully for")
                successful_resources.append(resource_path)

                # After successful entity assignment and duplicate checks append entity range to entity-organisation.csv
                post_lookup_df = pd.read_csv(lookup_path,dtype=str)
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
                logging.error(f"Error: {str(e)}",exc_info=True)
                output_df = pd.concat([output_df, pd.DataFrame([{
                    "dataset": dataset,
                    "resource": resource,  
                    "error_code": type(e).__name__,
                    "message": str(e)
                }])], ignore_index=True)
    
    finally:
        output_df.to_csv(f"batch_assign_summary_{scope}.csv", index=False)
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
    cache_dir: str = "var/cache/",
    new_entity_threshold: int = 10,
    resources: Optional[str] = None,
    skip_checks: bool = False,
    triggered_by: Optional[str] = None,
    branch: str = "auto/batch-assign-entities",
):
    endpoint_issue_summary_path = "https://datasette.planning.data.gov.uk/performance/endpoint_dataset_issue_type_summary.csv?_sort=rowid&issue_type__exact=unknown+entity&_size=max"

    response = requests.get(endpoint_issue_summary_path)
    issue_summary_df = pd.read_csv(StringIO(response.text))
    specification_dir = ensure_specification_dir()
    provision_rule_df = pd.read_csv(specification_dir / "provision-rule.csv")
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

    if triggered_by:
        print(f"Triggered by: {triggered_by}")
    if branch:
        print(f"Branch parameter: {branch}")
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
        )
        error_count = len(output_df[output_df['status'] == 'error'])
        success_count = len(output_df[output_df['status'] == 'success'])

        print(f"\nTotal failed downloads: {len(failed_downloads)}")
        print(f"Total failed assign-entities operations: {error_count}")
        print(f"Total successful assign-entities operations: {success_count}")

        if success_count > 0:
            create_or_update_pr_for_success(
                branch=branch,
                triggered_by=triggered_by,
                success_count=success_count,
            )
        else:
            print("No successful assignments; skipping PR creation")
    except Exception as e:
        print(f"An error occurred while processing the CSV file: {e}")
        traceback.print_exc()
    
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
    "--branch",
    required=False,
    type=str,
    help="Git branch to use (optional metadata).",
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

def main(
    scope: str = 'odp',
    cache_dir: str = "var/cache/",
    new_entity_threshold: int = 10,
    resources: Optional[str] = None,
    skip_checks: bool = False,
    triggered_by: Optional[str] = None,
    branch: str = "auto/batch-assign-entities",
) -> None:
    run_batch_assign_entities(
        scope=scope,
        cache_dir=cache_dir,
        new_entity_threshold=new_entity_threshold,
        resources=resources,
        skip_checks=skip_checks,
        triggered_by=triggered_by,
        branch=branch,
    )

if __name__ == "__main__":
    main()
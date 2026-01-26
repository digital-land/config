#!/usr/bin/env python3
import csv
import requests
import sys
import pandas as pd
import os
import shutil
import logging
import builtins

from pathlib import Path
from io import StringIO
from digital_land.commands import check_and_assign_entities

# ----------------------------
# CONFIGURATION FOR GITHUB ACTIONS
# ----------------------------
SCOPE = "single-source"   # hard-coded
AUTO_CONTINUE = True      # auto-answer yes to all prompts (CI)

# ----------------------------
# FORCE NON-INTERACTIVE "YES" IN CI
#   - digital_land sometimes uses click.confirm()
#   - and sometimes uses plain input() via digital_land.utils.add_data_utils.get_user_response
# This patch covers BOTH, preventing EOFError in GitHub Actions.
# ----------------------------
if AUTO_CONTINUE and (os.environ.get("CI") or os.environ.get("GITHUB_ACTIONS")):
    # Patch Python input() to always return "yes"
    def _auto_yes_input(prompt=""):
        try:
            print(f"{prompt}yes")
        except Exception:
            pass
        return "yes"

    builtins.input = _auto_yes_input

    # Patch click.confirm() to always confirm
    try:
        import click

        def _auto_confirm(text, *args, **kwargs):
            try:
                click.echo(f"{text} yes")
            except Exception:
                pass
            return True

        click.confirm = _auto_confirm
    except Exception:
        pass

    print("CI mode: auto-answer YES enabled for prompts (input() + click.confirm).")

# ----------------------------
# UTILITY FUNCTIONS
# ----------------------------
def download_file(url, local_path):
    """Download file from URL if it doesn't exist locally."""
    local_path = Path(local_path)
    if not local_path.exists():
        local_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Downloading {url} -> {local_path}")
        response = requests.get(url)
        response.raise_for_status()
        local_path.write_bytes(response.content)
        print(f"Downloaded {local_path}")
    else:
        print(f"{local_path} already exists, skipping download")

def get_old_resource_df(endpoint, collection_name, dataset):
    """
    Return transformed file for a previous ended resource using endpoint hash from CDN.

    NOTE: The original code said "second latest" but used _size=1.
    This uses _size=2 and picks the most recent row returned.
    """
    url = (
        "https://datasette.planning.data.gov.uk/performance/reporting_historic_endpoints.csv"
        f"?_sort=rowid&resource_end_date__notblank=1&endpoint__exact={endpoint}&_size=2"
    )
    response = requests.get(url)
    response.raise_for_status()
    previous_resource_df = pd.read_csv(StringIO(response.text))
    if len(previous_resource_df) == 0:
        return None

    old_resource_hash = previous_resource_df["resource"].iloc[-1]
    transformed_url = (
        f"https://files.planning.data.gov.uk/{collection_name}-collection/transformed/{dataset}/{old_resource_hash}.csv"
    )
    transformed_response = requests.get(transformed_url)
    transformed_response.raise_for_status()
    return pd.read_csv(StringIO(transformed_response.text))

def get_field_value_map(df, entity_number):
    """Return a dict of field-value pairs for a given entity from transformed file."""
    sub_df = df[
        (df["entity"] == entity_number)
        & (df["field"] != "reference")
        & (df["field"] != "entry-date")
    ]
    return dict(zip(sub_df["field"], sub_df["value"]))

# ----------------------------
# MAIN PROCESSING FUNCTION
# ----------------------------
def process_csv(scope):
    """Automatically process and assign unknown entities for given scope."""
    failed_downloads = []
    failed_assignments = []
    successful_resources = []

    resources_dir = Path("./resource")
    resources_dir.mkdir(exist_ok=True)

    with open("issue_summary.csv", "r") as file:
        csv_reader = csv.DictReader(file)

        for row_number, row in enumerate(csv_reader, start=1):
            if (
                row["issue_type"].lower() != "unknown entity"
                or row["scope"].lower() != scope
                or row["dataset"].lower() == "title-boundary"
            ):
                continue

            collection_name = row["collection"]
            resource = row["resource"]
            endpoint = row["endpoint"]
            dataset = row["pipeline"]
            organisation_name = row["organisation"]

            download_link = (
                f"https://files.planning.data.gov.uk/{collection_name}-collection/collection/resource/{resource}"
            )
            resource_path = resources_dir / resource
            cache_dir = Path("var/cache/")

            # Download resource
            try:
                response = requests.get(download_link)
                response.raise_for_status()
                resource_path.write_bytes(response.content)
                print(f"Downloaded: {resource}")
            except requests.RequestException as e:
                print(f"Failed to download: {resource} - {e}")
                failed_downloads.append((row_number, resource, str(e)))
                continue

            collection_path = Path(f"collection/{collection_name}")
            input_path = Path(cache_dir / "assign_entities" / "transformed" / f"{resource}.csv")

            try:
                success = check_and_assign_entities(
                    [resource_path],
                    [endpoint],
                    collection_name,
                    dataset,
                    [organisation_name],
                    collection_path,
                    cache_dir / "organisation.csv",
                    Path("specification"),
                    Path(f"pipeline/{collection_name}"),
                    input_path,
                )

                if not success:
                    # In CI, success=False often means "cancelled" (prompt path).
                    print(f"Entity assignment for resource '{resource}' returned False.")
                    failed_assignments.append(
                        (row_number, resource, "Cancelled", "check_and_assign_entities returned False")
                    )
                    continue

                old_resource_df = get_old_resource_df(endpoint, collection_name, dataset)

                if old_resource_df is not None:
                    current_resource_df = pd.read_csv(
                        cache_dir / "assign_entities" / "transformed" / f"{resource}.csv"
                    )
                    current_entities = set(current_resource_df["entity"])
                    old_entities = set(old_resource_df["entity"])
                    new_entities = list(current_entities - old_entities)

                    current_new_df = current_resource_df[current_resource_df["entity"].isin(new_entities)]

                    duplicate_entity = {}
                    field_map_to_old_entity = {}

                    for old_entity in old_resource_df["entity"].unique():
                        field_map = tuple(sorted(get_field_value_map(old_resource_df, old_entity).items()))
                        field_map_to_old_entity[field_map] = old_entity

                    for entity in new_entities:
                        current_fields = tuple(sorted(get_field_value_map(current_new_df, entity).items()))
                        if current_fields in field_map_to_old_entity:
                            duplicate_entity[entity] = field_map_to_old_entity[current_fields]

                    if duplicate_entity:
                        print("Matching entities found:", duplicate_entity)
                        print("AUTO-CONTINUE: yes (GitHub Action)")

                shutil.copy(
                    cache_dir / "assign_entities" / collection_name / "pipeline" / "lookup.csv",
                    Path("pipeline") / collection_name / "lookup.csv",
                )
                print(f"Entities assigned successfully for resource: {resource}")
                successful_resources.append(resource_path)

            except Exception as e:
                print(f"Failed to assign entities for resource: {resource}")
                logging.error(f"Error: {str(e)}", exc_info=True)
                failed_assignments.append((row_number, resource, "AssignmentError", str(e)))

    # Cleanup successful downloads
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
        if resources_dir.exists() and not any(resources_dir.iterdir()):
            resources_dir.rmdir()
    except OSError as e:
        print(f"Failed to remove resources directory: {e}")

    # Summary
    print("\n--- Summary Report ---")
    if failed_downloads:
        print("Failed Downloads:", failed_downloads)
    if failed_assignments:
        print("Failed Assignments:", failed_assignments)
    if not failed_downloads and not failed_assignments:
        print("All operations completed successfully.")

    return failed_downloads, failed_assignments

# ----------------------------
# MAIN EXECUTION
# ----------------------------
if __name__ == "__main__":
    # Ensure provision-rule.csv exists
    download_file(
        "https://raw.githubusercontent.com/digital-land/specification/refs/heads/main/content/provision-rule.csv",
        "specification/provision-rule.csv",
    )

    # Download issue summary
    endpoint_issue_summary_path = (
        "https://datasette.planning.data.gov.uk/performance/"
        "endpoint_dataset_issue_type_summary.csv?_sort=rowid&issue_type__exact=unknown+entity&_size=max"
    )
    response = requests.get(endpoint_issue_summary_path)
    df = pd.read_csv(StringIO(response.text))

    provision_rule_df = pd.read_csv("specification/provision-rule.csv")

    # Assign scope per dataset
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

    def determine_scope(dataset):
        if dataset in scope_dict["odp"]:
            return "odp"
        elif dataset in scope_dict["mandated"]:
            return "mandated"
        else:
            return "single-source"

    df["scope"] = df["dataset"].apply(determine_scope)
    df.to_csv("issue_summary.csv", index=False)
    print("issue_summary.csv downloaded successfully")

    # Run only single-source
    failed_downloads, failed_assignments = process_csv(SCOPE)
    print(f"Failed downloads: {len(failed_downloads)}")
    print(f"Failed assign-entities operations: {len(failed_assignments)}")

    # Fail CI if anything failed
    if failed_downloads or failed_assignments:
        sys.exit(1)

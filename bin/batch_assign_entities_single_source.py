#!/usr/bin/env python3

import csv
import requests
import sys
import pandas as pd
import os
import shutil
import logging

from pathlib import Path
from io import StringIO

# ------------------------------------------------------------------
# FORCE NON-INTERACTIVE MODE (CRITICAL)
# ------------------------------------------------------------------
# digital-land-python internally calls get_user_response(), which
# calls input(). We override it globally so it ALWAYS returns "yes".

import digital_land.utils.add_data_utils as add_data_utils

def _always_yes(*args, **kwargs):
    return "yes"

add_data_utils.get_user_response = _always_yes

# Also enforce via env vars (belt-and-braces)
os.environ["DIGITAL_LAND_ASSUME_YES"] = "1"
os.environ["DIGITAL_LAND_NON_INTERACTIVE"] = "1"
# ------------------------------------------------------------------

from digital_land.commands import check_and_assign_entities
from digital_land.collection import Collection

# ----------------------------
# CONFIGURATION
# ----------------------------
SCOPE = "single-source"  # hard-coded

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
    """Return transformed file for second latest resource using endpoint hash from CDN."""
    url = (
        "https://datasette.planning.data.gov.uk/performance/"
        "reporting_historic_endpoints.csv"
        "?_sort=rowid&resource_end_date__notblank=1"
        f"&endpoint__exact={endpoint}&_size=1"
    )
    response = requests.get(url)
    response.raise_for_status()
    previous_resource_df = pd.read_csv(StringIO(response.text))
    if len(previous_resource_df) == 0:
        return None

    old_resource_hash = previous_resource_df["resource"][0]
    transformed_url = (
        f"https://files.planning.data.gov.uk/"
        f"{collection_name}-collection/transformed/{dataset}/{old_resource_hash}.csv"
    )
    transformed_response = requests.get(transformed_url)
    transformed_response.raise_for_status()
    return pd.read_csv(StringIO(transformed_response.text))

def get_field_value_map(df, entity_number):
    """Return a dict of field-value pairs for a given entity."""
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
    """Automatically process and assign unknown entities."""
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
                f"https://files.planning.data.gov.uk/"
                f"{collection_name}-collection/collection/resource/{resource}"
            )
            resource_path = resources_dir / resource
            cache_dir = Path("var/cache/")

            try:
                response = requests.get(download_link)
                response.raise_for_status()
                resource_path.write_bytes(response.content)
                print(f"Downloaded resource: {resource}")
            except requests.RequestException as e:
                print(f"Failed to download {resource}: {e}")
                failed_downloads.append((row_number, resource, str(e)))
                continue

            collection_path = Path(f"collection/{collection_name}")
            input_path = cache_dir / "assign_entities" / "transformed" / f"{resource}.csv"

            try:
                # 🚀 NON-INTERACTIVE assignment (always proceeds)
                check_and_assign_entities(
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

                old_resource_df = get_old_resource_df(endpoint, collection_name, dataset)

                if old_resource_df is not None:
                    current_resource_df = pd.read_csv(
                        cache_dir / "assign_entities" / "transformed" / f"{resource}.csv"
                    )
                    current_entities = set(current_resource_df["entity"])
                    old_entities = set(old_resource_df["entity"])
                    new_entities = list(current_entities - old_entities)

                    current_resource_df = current_resource_df[
                        current_resource_df["entity"].isin(new_entities)
                    ]

                    field_map_to_old_entity = {}
                    for old_entity in old_resource_df["entity"].unique():
                        field_map = tuple(
                            sorted(get_field_value_map(old_resource_df, old_entity).items())
                        )
                        field_map_to_old_entity[field_map] = old_entity

                    duplicate_entity = {}
                    for entity in new_entities:
                        current_fields = tuple(
                            sorted(get_field_value_map(current_resource_df, entity).items())
                        )
                        if current_fields in field_map_to_old_entity:
                            duplicate_entity[entity] = field_map_to_old_entity[current_fields]

                    if duplicate_entity:
                        print("Matching entities found:", duplicate_entity)
                        print("AUTO-CONTINUE: yes")

                shutil.copy(
                    cache_dir / "assign_entities" / collection_name / "pipeline" / "lookup.csv",
                    Path("pipeline") / collection_name / "lookup.csv",
                )

                print(f"Entities assigned successfully for resource: {resource}")
                successful_resources.append(resource_path)

            except Exception as e:
                print(f"Failed to assign entities for resource: {resource}")
                logging.error(str(e), exc_info=True)
                failed_assignments.append((row_number, resource, str(e)))

    # Cleanup
    for resource_path in successful_resources:
        try:
            if resource_path.exists():
                resource_path.unlink()
            gfs_path = resource_path.with_suffix(".gfs")
            if gfs_path.exists():
                gfs_path.unlink()
        except OSError as e:
            print(f"Cleanup failed for {resource_path}: {e}")

    try:
        if resources_dir.exists() and not any(resources_dir.iterdir()):
            resources_dir.rmdir()
    except OSError:
        pass

    print("\n--- Summary Report ---")
    if failed_downloads:
        print("Failed downloads:", failed_downloads)
    if failed_assignments:
        print("Failed assignments:", failed_assignments)
    if not failed_downloads and not failed_assignments:
        print("All operations completed successfully.")

    return failed_downloads, failed_assignments

# ----------------------------
# MAIN EXECUTION
# ----------------------------
if __name__ == "__main__":

    download_file(
        "https://raw.githubusercontent.com/digital-land/specification/main/content/provision-rule.csv",
        "specification/provision-rule.csv",
    )

    endpoint_issue_summary_path = (
        "https://datasette.planning.data.gov.uk/performance/"
        "endpoint_dataset_issue_type_summary.csv"
        "?_sort=rowid&issue_type__exact=unknown+entity&_size=max"
    )

    response = requests.get(endpoint_issue_summary_path)
    df = pd.read_csv(StringIO(response.text))

    provision_rule_df = pd.read_csv("specification/provision-rule.csv")

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
        if dataset in scope_dict["mandated"]:
            return "mandated"
        return "single-source"

    df["scope"] = df["dataset"].apply(determine_scope)
    df.to_csv("issue_summary.csv", index=False)
    print("issue_summary.csv generated")

    failed_downloads, failed_assignments = process_csv(SCOPE)
    print(f"Failed downloads: {len(failed_downloads)}")
    print(f"Failed assign-entities operations: {len(failed_assignments)}")

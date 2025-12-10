import csv
import requests
import sys
import pandas as pd
import os
import shutil
import logging

from pathlib import Path
from io import StringIO
from digital_land.commands import check_and_assign_entities
from digital_land.collection import Collection

# ----------------------------
# CONFIGURATION FOR GITHUB ACTIONS
# ----------------------------
SCOPE = "single-source"
AUTO_CONTINUE = True

SPEC_REPO_RAW_BASE = "https://raw.githubusercontent.com/digital-land/specification/main"

# ----------------------------
# UTILITY FUNCTIONS
# ----------------------------
def download_file(url, local_path):
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


def ask_yes_no(prompt="Continue? (y/n): "):
    print(f"{prompt} AUTO-ANSWERED: yes")
    return True


def get_user_response(prompt):
    print(f"{prompt} AUTO-ANSWERED: yes")
    return True


def ensure_specification_files():
    """
    Ensure that all core spec CSVs required by digital_land.Specification exist.
    This prevents:
    FileNotFoundError: specification/dataset.csv
    """
    spec_files = {
        "specification/dataset.csv": f"{SPEC_REPO_RAW_BASE}/specification/dataset.csv",
        "specification/field.csv": f"{SPEC_REPO_RAW_BASE}/specification/field.csv",
        "specification/dataset-field.csv": f"{SPEC_REPO_RAW_BASE}/specification/dataset-field.csv",
        "specification/typology.csv": f"{SPEC_REPO_RAW_BASE}/specification/typology.csv",
        "specification/datatype.csv": f"{SPEC_REPO_RAW_BASE}/specification/datatype.csv",
        "specification/provision-rule.csv": f"{SPEC_REPO_RAW_BASE}/content/provision-rule.csv",
    }

    for local_path, url in spec_files.items():
        try:
            download_file(url, local_path)
        except Exception as e:
            print(f"WARNING: Failed to download {url}: {e}")


def get_old_resource_df(endpoint, collection_name, dataset):
    url = (
        "https://datasette.planning.data.gov.uk/performance/"
        "reporting_historic_endpoints.csv"
        f"?_sort=rowid&resource_end_date__notblank=1&endpoint__exact={endpoint}&_size=1"
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
    failed_downloads = []
    failed_assignments = []
    successful_resources = []
    resources_dir = Path("./resource")
    resources_dir.mkdir(exist_ok=True)

    with open("issue_summary.csv", "r") as file:
        csv_reader = csv.DictReader(file)
        for row_number, row in enumerate(csv_reader, start=1):

            issue_type = str(row.get("issue_type", "")).lower().strip()
            row_scope = str(row.get("scope", "")).lower().strip()
            dataset_name = str(row.get("dataset", "")).lower().strip()

            if (
                issue_type != "unknown entity"
                or row_scope != scope
                or dataset_name == "title-boundary"
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

            # ---- DOWNLOAD RESOURCE ----
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
            input_path = (
                cache_dir / "assign_entities" / "transformed" / f"{resource}.csv"
            )

            try:
                # ---- RUN ENTITY ASSIGNMENT ----
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
                    raise RuntimeError(f"Entity assignment returned False for {resource}")

                # ---- DUPLICATE DETECTION ----
                old_resource_df = get_old_resource_df(
                    endpoint, collection_name, dataset
                )

                if old_resource_df is not None:
                    current_resource_df = pd.read_csv(
                        cache_dir
                        / "assign_entities"
                        / "transformed"
                        / f"{resource}.csv"
                    )
                    current_entities = set(current_resource_df["entity"])
                    old_entities = set(old_resource_df["entity"])
                    new_entities = list(current_entities - old_entities)

                    duplicate_entity = {}

                    field_map_to_old_entity = {}
                    for old_entity in old_resource_df["entity"].unique():
                        field_map = tuple(
                            sorted(get_field_value_map(old_resource_df, old_entity).items())
                        )
                        field_map_to_old_entity[field_map] = old_entity

                    for entity in new_entities:
                        current_fields = tuple(
                            sorted(get_field_value_map(current_resource_df, entity).items())
                        )
                        if current_fields in field_map_to_old_entity:
                            duplicate_entity[entity] = field_map_to_old_entity[current_fields]

                    if duplicate_entity:
                        print("Matching entities found:", duplicate_entity)

                # ---- COPY LOOKUP FILE ----
                src_lookup = (
                    cache_dir
                    / "assign_entities"
                    / collection_name
                    / "pipeline"
                    / "lookup.csv"
                )

                if not src_lookup.exists():
                    raise FileNotFoundError(f"lookup.csv NOT found at {src_lookup}")

                dest_lookup = Path("pipeline") / collection_name / "lookup.csv"
                dest_lookup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(src_lookup, dest_lookup)

                print(f"✅ lookup.csv updated for collection {collection_name}")
                successful_resources.append(resource_path)

            except Exception as e:
                print(f"❌ Failed to assign entities for resource: {resource}")
                logging.error(str(e), exc_info=True)
                failed_assignments.append((row_number, resource, str(e)))

    # ---- CLEANUP ----
    for resource_path in successful_resources:
        try:
            if resource_path.exists():
                resource_path.unlink()
            gfs_path = resource_path.with_suffix(".gfs")
            if gfs_path.exists():
                gfs_path.unlink()
        except OSError as e:
            print(f"Cleanup failed for {resource_path}: {e}")

    if resources_dir.exists() and not any(resources_dir.iterdir()):
        resources_dir.rmdir()

    # ---- SUMMARY ----
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

    ensure_specification_files()

    endpoint_issue_summary_path = (
        "https://datasette.planning.data.gov.uk/performance/"
        "endpoint_dataset_issue_type_summary.csv"
        "?_sort=rowid&issue_type__exact=unknown+entity&_size=max"
    )
    response = requests.get(endpoint_issue_summary_path)
    response.raise_for_status()
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
        elif dataset in scope_dict["mandated"]:
            return "mandated"
        else:
            return "single-source"

    df["scope"] = df["dataset"].apply(determine_scope)
    df.to_csv("issue_summary.csv", index=False)
    print("✅ issue_summary.csv generated successfully")

    failed_downloads, failed_assignments = process_csv(SCOPE)

    print(f"Failed downloads: {len(failed_downloads)}")
    print(f"Failed assign-entities operations: {len(failed_assignments)}")

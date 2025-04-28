import csv
import requests
import sys
import pandas as pd
import os
import shutil

from pathlib import Path
from io import StringIO
from digital_land.commands import check_and_assign_entities
from digital_land.collection import Collection
from digital_land.utils.add_data_utils import get_user_response


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
    old_resource_hash = pd.read_csv(StringIO(response.text))['resource'][0]

    transformed_url = (
        f"https://files.planning.data.gov.uk/{collection_name}-collection/transformed/{dataset}/{old_resource_hash}.csv"
    )
    transformed_response = requests.get(transformed_url)
    transformed_response.raise_for_status()
    return pd.read_csv(StringIO(transformed_response.text))

def get_field_value_map(df, entity_number):
    """
    returns a dict of of field-value pairs for a given entity from transformed file
    """
    sub_df = df[(df['entity'] == entity_number) & (df['field'] != 'reference') & (df['field'] != 'entry-date')]
    return dict(zip(sub_df['field'], sub_df['value']))


def process_csv(scope):
    """
    Uses provided file path to automatically process and assign unknown entities
    """
    failed_downloads = []
    failed_assignments = []
    successful_resources = []
    resources_dir = Path("./resource")
    resources_dir.mkdir(exist_ok=True)

    try:
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
                download_link = f"https://files.planning.data.gov.uk/{collection_name}-collection/collection/resource/{resource}"
                resource_path = resources_dir / resource
                cache_dir=Path("var/cache/")
                try:
                    response = requests.get(download_link)
                    response.raise_for_status()
                    resource_path.write_bytes(response.content)
                    print(f"Downloaded: {resource}")
                except requests.RequestException as e:
                    print(f"Failed to download: {resource}")
                    print(f"Error: {e}")
                    failed_downloads.append((row_number, resource, str(e)))
                    continue
                collection_path = Path(f"collection/{collection_name}")
                try:
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
                    )

                    #get old transformed resource
                    old_resource_df = get_old_resource_df(endpoint,collection_name,dataset)

                    # get current transformed resource
                    current_resource_df = pd.read_csv(cache_dir / "assign_entities" / "transformed" / f"{resource}.csv")
                    #old_resource_df = pd.read_csv(os.path.join("var/cache/assign_entities/transformed", "second_resource.csv"))
                    
                    current_entities = set(current_resource_df['entity'])
                    old_entities = set(old_resource_df['entity'])

                    # store new entities in current_resource_df
                    new_entities = list(current_entities - old_entities)
                    current_resource_df = current_resource_df[current_resource_df['entity'].isin(new_entities)]
                
                    duplicate_entity={}
                    for entity in new_entities:
                        current_fields = get_field_value_map(current_resource_df, entity)
      
                        for old_resource_entity in old_resource_df['entity'].unique():
                            old_resource_fields = get_field_value_map(old_resource_df, old_resource_entity)

                            if current_fields == old_resource_fields:
                                duplicate_entity[entity]=old_resource_entity
                                break

                    if duplicate_entity:
                        print("Matching entities found (new_entity:matched_current_entity):",duplicate_entity)
                        if not get_user_response(
                            "Do you want to still assign entities for this resource? (yes/no): "
                        ):
                            successful_resources.append(resource_path)
                            continue

                    shutil.copy(cache_dir / "assign_entities" / collection_name / "pipeline" / "lookup.csv", Path("pipeline") / collection_name / "lookup.csv")
                    print(f"\nEntities assigned successfully for resource: {resource}")
                    successful_resources.append(resource_path)
                except Exception as e:
                    print(f"Failed to assign entities for resource: {resource}")
                    print(f"Error: {str(e)}")
                    failed_assignments.append(
                        (row_number, resource, "AssignmentError", str(e))
                    )
    finally:
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
            if not any(resources_dir.iterdir()):
                resources_dir.rmdir()
        except OSError as e:
            print(f"Failed to remove the resources directory: {e}")
    # Summary of results
    print("\n--- Summary Report ---")
    if failed_downloads:
        print("\nFailed Downloads:")
        for resource, error in failed_downloads:
            print(f"Resource: {resource} - Error: {error}")
    if failed_assignments:
        print("\nFailed Assign-Entities Operations:")
        for row_number, resource, error_code, error_message in failed_assignments:
            print(
                f"Resoure : {resource} - Error Code: {error_code}, Message: {error_message}"
            )
    if not failed_downloads and not failed_assignments:
        print("All operations completed successfully.")
    return failed_downloads, failed_assignments


def get_scope(value, scope_dict):
    for scope, datasets in scope_dict.items():
        if value in datasets:
            return scope
    return "single-source"


if __name__ == "__main__":

    endpoint_issue_summary_path = "https://datasette.planning.data.gov.uk/performance/endpoint_dataset_issue_type_summary.csv?_sort=rowid&issue_type__exact=unknown+entity&_size=max"

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

    df["scope"] = df["dataset"].apply(lambda x: get_scope(x, scope_dict))

    df.to_csv("issue_summary.csv", index=False)
    print("issue_summary.csv downloaded successfully")

    user_response = input("Do you want to continue? (yes/no): ").strip().lower()
    if user_response != "yes":
        print("Operation cancelled by user.")
        sys.exit(0)

    scope = input("Enter scope (odp/mandated/single-source): ").strip().lower()
    if scope not in ["odp", "mandated", "single-source"]:
        raise ValueError(f"'{scope}' isn't a valid scope. Please enter a valid scope.")

    try:
        failed_downloads, failed_assignments = process_csv(scope)
        print(f"\nTotal failed downloads: {len(failed_downloads)}")
        print(f"Total failed assign-entities operations: {len(failed_assignments)}")
    except Exception as e:
        print(f"An error occurred while processing the CSV file: {e}")
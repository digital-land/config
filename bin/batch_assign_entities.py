import csv
import requests
import sys
import pandas as pd
import os
import shutil
import logging

from pathlib import Path
from io import StringIO

"""Pre-requistes for the check_and_assign_entities function"""
from digital_land.commands import assign_entities
from digital_land.pipeline import run_pipeline, Lookups, Pipeline
from digital_land.commands import pipeline_run
from digital_land.specification import Specification
from digital_land.utils.add_data_utils import (
    clear_log,
    download_dataset,
    get_column_field_summary,
    get_transformed_entities,
    get_entity_summary,
    get_existing_endpoints_summary,
    get_issue_summary,
    get_updated_entities_summary,
    is_date_valid,
    is_url_valid,
    get_user_response,
)
#from digital_land.commands import check_and_assign_entities


from digital_land.collection import Collection
from digital_land.utils.add_data_utils import get_user_response

from tqdm import tqdm
from urllib.request import urlretrieve
from concurrent.futures import ThreadPoolExecutor

from distutils.dir_util import copy_tree

logger = logging.getLogger("__name__")

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


def ask_yes_no(prompt="Continue? (y/n): "):
    """Ask the user a yes/no question and return True for yes, False for no."""
    while True:
        answer = input(prompt).strip().lower()
        if answer in ("y", "yes"):
            return True
        elif answer in ("n", "no"):
            return False
        else:
            print("Please answer with 'y' or 'n'.")

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

"""
 The following function ( check_and_assign_entities) is adapted from check_and_assign_entities in digital_land.commands 
 to be used for batch processing of resources with unknown entity issues.
 It assigns entities for the given resource and runs the pipeline to get the transformed resource, while also checking
 for any new entities that have been added and prompting the user to continue if there are new entities. 
 """
def check_and_assign_entities(
    resource_file_paths,
    endpoints,
    collection_name,
    dataset,
    organisation,
    collection_dir,
    organisation_path,
    specification_dir,
    pipeline_dir,
    input_path=None,
):
    # Assigns entities for the given resources in the given collection and run pipeline to get the transformed resource.

    collection = Collection(name=collection_name, directory=collection_dir)
    collection.load()

    cache_dir = Path("var/cache/")
    assign_entities_cache_dir = cache_dir / "assign_entities"

    resource_path = resource_file_paths[0]
    resource = Path(resource_path).name
    if input_path:
        output_path = input_path
    else:
        output_path = assign_entities_cache_dir / "transformed/" / f"{resource}.csv"

    issue_dir = assign_entities_cache_dir / "issue/"
    column_field_dir = assign_entities_cache_dir / "column_field/"
    dataset_resource_dir = assign_entities_cache_dir / "dataset_resource/"
    converted_resource_dir = assign_entities_cache_dir / "converted_resource/"
    converted_dir = assign_entities_cache_dir / "converted/"
    output_log_dir = assign_entities_cache_dir / "log/"
    operational_issue_dir = (
        assign_entities_cache_dir / "performance " / "operational_issue/"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    issue_dir.mkdir(parents=True, exist_ok=True)
    column_field_dir.mkdir(parents=True, exist_ok=True)
    dataset_resource_dir.mkdir(parents=True, exist_ok=True)
    converted_resource_dir.mkdir(parents=True, exist_ok=True)
    converted_dir.mkdir(parents=True, exist_ok=True)
    output_log_dir.mkdir(parents=True, exist_ok=True)
    operational_issue_dir.mkdir(parents=True, exist_ok=True)

    cache_pipeline_dir = assign_entities_cache_dir / collection.name / "pipeline"
    copy_tree(str(pipeline_dir), str(cache_pipeline_dir))

    new_lookups = assign_entities(
        resource_file_paths,
        collection,
        dataset,
        organisation,
        cache_pipeline_dir,
        specification_dir,
        organisation_path,
        endpoints,
        cache_dir,
    )
    pipeline = Pipeline(cache_pipeline_dir, dataset)
    try:
        pipeline_run(
            dataset,
            pipeline,
            Specification(specification_dir),
            resource_path,
            output_path=output_path,
            collection_dir=collection_dir,
            issue_dir=issue_dir,
            operational_issue_dir=operational_issue_dir,
            column_field_dir=column_field_dir,
            dataset_resource_dir=dataset_resource_dir,
            converted_resource_dir=converted_resource_dir,
            organisation_path=organisation_path,
            endpoints=endpoints,
            organisations=organisation,
            resource=resource,
            output_log_dir=output_log_dir,
            converted_path=os.path.join(converted_dir, resource + ".csv"),
        )
    except Exception as e:
        raise RuntimeError(
            f"Pipeline failed to process resource with the following error: {e}"
        )

    endpoint_resource_info = {
        "resource": resource,
        "organisation": organisation[0],
    }
    new_entities = [entry["entity"] for entry in new_lookups]
    issue_summary = get_issue_summary(endpoint_resource_info, issue_dir, new_entities)
    print(issue_summary)

    if "No issues found" not in issue_summary:
        if not get_user_response(
            "Do you want to continue processing this resource? (yes/no): "
        ):
            return False
    return True


def process_csv(scope, resource_dir):
    """
    Uses provided file path to automatically process and assign unknown entities
    """
    failed_downloads = []
    failed_assignments = []
    successful_resources = []

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
                resource_path = resource_dir / resource
                cache_dir=Path("var/cache/")
                
                
                print("********************************************************************************************************************************")
                print("********************************************************************************************************************************")
                print(f"Collection_name > {collection_name}")
                print(f"Resource hash > {resource}")
                print(f"Endpoint hash > {endpoint}")
                print(f"Download_link > {download_link }")
                print(f"Resource path > {resource_path}")

                    
                if resource_path.is_file():
                    print(f"Resource  exists in the Path : {resource_path}")
                else:
                    try:
                        response = requests.get(download_link)
                        response.raise_for_status()
                        resource_path.write_bytes(response.content)
                        print(f"Downloaded: {resource}")
                    except requests.RequestException as e:
                        print(f"Failed to download: {resource}")
                        print(f"Error: {e}")
                        failed_downloads.append((row_number, resource, str(e)))
                    #continue

                    print(f"Successfully downloaded resource: {resource}")
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
                    # if operation cancelled by user because of issues with new entities
                    if not success:
                        print(f"Entity assignment for resource '{resource}' was cancelled.")
                        successful_resources.append(resource_path)
                        continue 

                    #get old transformed resource
                    old_resource_df = get_old_resource_df(endpoint,collection_name,dataset)

                    if old_resource_df is not None:
                        # get current transformed resource
                        current_resource_df = pd.read_csv(cache_dir / "assign_entities" / "transformed" / f"{resource}.csv")
                        
                        current_entities = set(current_resource_df['entity'])
                        old_entities = set(old_resource_df['entity'])

                        # store new entities in current_resource_df
                        new_entities = list(current_entities - old_entities)
                        current_resource_df = current_resource_df[current_resource_df['entity'].isin(new_entities)]
                
                        duplicate_entity = {}

                        # store old entity field maps
                        field_map_to_old_entity = {}
                        for old_entity in old_resource_df["entity"].unique():
                            field_map = tuple(sorted(get_field_value_map(old_resource_df, old_entity).items()))
                            field_map_to_old_entity[field_map] = old_entity

                        # compare new entity field maps using dict lookup
                        for entity in new_entities:
                            current_fields = tuple(sorted(get_field_value_map(current_resource_df, entity).items()))
                            if current_fields in field_map_to_old_entity:
                                duplicate_entity[entity] = field_map_to_old_entity[current_fields]

                        if duplicate_entity:
                            print("Matching entities found (new_entity:matched_current_entity):",duplicate_entity)
                            if not get_user_response(
                                "You should not add this resource until doing analysis on why there's duplicate entities. Do you want to still assign entities for this resource? (yes/no): "
                            ):
                                successful_resources.append(resource_path)
                                continue
                    else:
                        print(f"No previous transformed resource found for endpoint: {endpoint}")

                    shutil.copy(cache_dir / "assign_entities" / collection_name / "pipeline" / "lookup.csv", Path("pipeline") / collection_name / "lookup.csv")
                    print(f"\nEntities assigned successfully for resource: {resource}")
                    successful_resources.append(resource_path)

                    # After successful entity assignment and duplicate checks append entity range to entity-organisation.csv
                    if new_entities:  
                        min_entity = min(new_entities)
                        max_entity = max(new_entities)
                        
                        entity_org_file = Path("pipeline") / collection_name / "entity-organisation.csv"
                        
                        # Append the new entity range
                        with open(entity_org_file, "a", newline="") as f:
                            writer = csv.writer(f)
                            writer.writerow([dataset, min_entity, max_entity, organisation_name])
                        
                        print(f"Appended entity range {min_entity}-{max_entity} for {organisation_name} to {entity_org_file}")
                except Exception as e:
                    print(f"Failed to assign entities for resource: {resource}")
                    logging.error(f"Error: {str(e)}",exc_info=True)
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

    if not ask_yes_no(prompt="Do you wish to continue? (y/n): "):
        print("Operation cancelled by user.")
        sys.exit(0)

    scope = input("Enter scope (odp/mandated/single-source): ").strip().lower()
    if scope not in ["odp", "mandated", "single-source"]:
        raise ValueError(f"'{scope}' isn't a valid scope. Please enter a valid scope.")

    print("READY to PROCESS")
    # Build url_map from the CSV data
    url_map = {}
    resource_dir = Path("./resource")
    resource_dir.mkdir(exist_ok=True)
    
    with open("issue_summary.csv", "r") as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            if (
                row["issue_type"].lower() != "unknown entity"
                or row["scope"].lower() != scope
                or row["dataset"].lower() == "title-boundary"
            ):
                continue
            collection_name = row["collection"]
            resource = row["resource"]
            download_link = f"https://files.planning.data.gov.uk/{collection_name}-collection/collection/resource/{resource}"
            resource_path = resource_dir / resource
            url_map[download_link] = str(resource_path)
    
    if ask_yes_no(prompt="Do you wish to batch download the resources? (y/n): "):
        print("Downloading resources")
        download_urls(url_map, max_threads=4)
    else: 
        print("Downloading individual resource files at a time")

    try:
        failed_downloads, failed_assignments = process_csv(scope, resource_dir)
        print(f"\nTotal failed downloads: {len(failed_downloads)}")
        print(f"Total failed assign-entities operations: {len(failed_assignments)}")
    except Exception as e:
        print(f"An error occurred while processing the CSV file: {e}")
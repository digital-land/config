import csv
import requests
import os
import subprocess
import sys

from digital_land.cli import assign_entities_cmd
from pathlib import Path
from digital_land.commands import assign_entities
from digital_land.organisation import Organisation
from digital_land.collection import Collection

import click

def process_csv(csv_file):
    failed_downloads = []
    failed_assignments = []
    successful_resources = []
    
    resources_dir = './resources'
    os.makedirs(resources_dir, exist_ok=True)
    try:
        with open(csv_file, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row_number, row in enumerate(csv_reader, start=1):

                # only process unknown entities
                if row['issue_type'].lower() != 'unknown entity':
                    continue

                collection_name = row['collection']
                resource = row['resource']
                endpoint = row['endpoint']
                dataset = row['pipeline']
                organisation_name = row['organisation']


                download_link = f"https://files.planning.data.gov.uk/{collection_name}-collection/collection/resource/{resource}"
                resource_path = os.path.join(resources_dir, resource)

                # try to download the resource
                try:
                    response = requests.get(download_link)
                    response.raise_for_status()
                    with open(resource_path, 'wb') as f:
                        f.write(response.content)
                    print(f"Downloaded: {resource}")
                except requests.RequestException as e:
                    print(f"Failed to download: {resource}")
                    print(f"Error: {e}")
                    failed_downloads.append((row_number, resource, str(e)))
                    continue


                # command = [
                #     "digital-land",
                #     "assign-entities",
                #     f"resources/{resource}",
                #     endpoint,
                #     collection_name,
                #     dataset,
                #     organisation,
                #     "-c", f"./collection/{collection_name}",
                #     "-p", f"./pipeline/{collection_name}"
                # ]

                # try:
                #     result = subprocess.run(command, check=True, capture_output=True, text=True)

                # execute the command but return error if assign-entities fails
                # collection = Collection(collection_name,f"collection/{collection_name}")
                collection_path = f"collection/{collection_name}"
                # specification_dir = "specification",
                # pipeline_dir = f"pipeline/{collection_name}",
                # organisation_path = "var/cache/organisation.csv"
                
# resource_path,
#     endpoints,
#     collection_name,
#     dataset,
#     organisation,
#     collection_dir,
#     specification_dir,
#     pipeline_dir,
#     organisation_path,

                collection = Collection(name=collection_name, directory=collection_path)
                collection.load()


                resource_path = Path(f'resources/{resource}')
                print(resource_path)
                endpoints = endpoint
                collection_name = collection_name
                dataset = dataset
                organisation = organisation_name
                collection_dir = Path(collection_path)
                specification_dir = Path("specification")
                organisation_path = Path("var/cache/organisation.csv")
                pipeline_dir = Path("pipeline/")

                try:
                    # assign_entities(
                    #     resource_file_paths = [resource],
                    #     collection =collection,
                    #     dataset = dataset,
                    #     organisation = [organisation_name],
                    #     pipeline_dir = f"pipeline/{collection_name}",
                    #     specification_dir = "specification",
                    #     organisation_path =  "var/cache/organisation.csv",
                    #     endpoints = [endpoint],
                    # )
                    
                    # assign_entities_cmd(
                    #     resource,
                    #     endpoints,
                    #     collection_name,
                    #     dataset,
                    #     organisation,
                    #     collection_dir,
                    #     specification_dir,
                    #     organisation_path,
                    # )

                    # ctx = click.Context(assign_entities_cmd)


                    assign_entities_cmd.callback(
                    # ctx,
                    resource,
                    endpoints,
                    collection_name,
                    dataset,
                    organisation,
                    collection_dir,
                    specification_dir,
                    pipeline_dir,
                    organisation_path
                    )
                    print(f"Entities assigned successfully for resource: {resource}")
                    successful_resources.append(resource_path)
                except Exception as e:
                    print(f"Failed to assign entities for resource: {resource}")
                    print(f"Error: {str(e)}")
                    failed_assignments.append((row_number, resource, "AssignmentError", str(e)))

    finally:
        # remove successfully processed resources
        for resource_path in successful_resources:
            try:
                
                os.remove(resource_path)
                print(f"Removed: {resource_path}")
            except OSError as e:
                print(f"Failed to remove {resource_path}: {e}")

        # Removes resources directory if empty
        try:
            os.rmdir(resources_dir)
        except OSError as e:
            print(f"Failed to remove the resources directory: {e}")

    # Summary of results
    print("\n--- Summary Report ---")
    if failed_downloads:
        print("\nFailed Downloads:")
        for row, resource, error in failed_downloads:
            print(f"Row {row}: {resource} - Error: {error}")
    
    if failed_assignments:
        print("\nFailed Assign-Entities Operations:")
        for row, resource, error_code, error_message in failed_assignments:
            print(f"Row {row}: {resource} - Error Code: {error_code}, Message: {error_message}")
    
    if not failed_downloads and not failed_assignments:
        print("All operations completed successfully.")

    return failed_downloads, failed_assignments

if __name__ == "__main__":
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = input("Enter csv file path: ")
    
    try:
        failed_downloads, failed_assignments = process_csv(csv_file)
        print(f"\nTotal failed downloads: {len(failed_downloads)}")
        print(f"Total failed assign-entities operations: {len(failed_assignments)}") 
    except Exception as e:
        print(f"An error occurred while processing the CSV file: {e}")
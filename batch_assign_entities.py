import csv
import requests
import sys

from pathlib import Path
from digital_land.cli import assign_entities_cmd
from digital_land.collection import Collection

def process_csv(csv_file):
    """
    Uses provided file path to automatically process and assign unknown entities
    """
    failed_downloads = []
    failed_assignments = []
    successful_resources = []
    
    resources_dir = Path('./resources')
    resources_dir.mkdir(exist_ok=True)
    
    try:
        with open(csv_file, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row_number, row in enumerate(csv_reader, start=1):
                if row['issue_type'].lower() != 'unknown entity':
                    continue

                collection_name = row['collection']
                resource = row['resource']
                endpoint = row['endpoint']
                dataset = row['pipeline']
                organisation_name = row['organisation']

                download_link = f"https://files.planning.data.gov.uk/{collection_name}-collection/collection/resource/{resource}"
                resource_path = resources_dir / resource

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
                collection = Collection(name=collection_name, directory=collection_path)
                collection.load()

                try:
                    assign_entities_cmd.callback(
                        resource_path,
                        endpoint,
                        collection_name,
                        dataset,
                        organisation_name,
                        collection_path,
                        Path("specification"),
                        Path(f"pipeline/{collection_name}"),
                        Path("var/cache/organisation.csv")
                    )
                    print(f"\nEntities assigned successfully for resource: {resource}")
                    successful_resources.append(resource_path)
                except Exception as e:
                    print(f"Failed to assign entities for resource: {resource}")
                    print(f"Error: {str(e)}")
                    failed_assignments.append((row_number, resource, "AssignmentError", str(e)))

    finally:
        # Remove successfully processed resources 
        for resource_path in successful_resources:
            try:
                if resource_path.exists():
                    resource_path.unlink()
                
                gfs_path = resource_path.with_suffix('.gfs')
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
        for resource, error_code, error_message in failed_assignments:
            print(f"Resoure : {resource} - Error Code: {error_code}, Message: {error_message}")
    
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
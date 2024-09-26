import csv
import requests
import os
import subprocess
import sys

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
                organisation = row['organisation']


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

                command = [
                    "digital-land",
                    "assign-entities",
                    f"resources/{resource}",
                    endpoint,
                    collection_name,
                    dataset,
                    organisation,
                    "-c", f"./collection/{collection_name}",
                    "-p", f"./pipeline/{collection_name}"
                ]

                # execute the command but return error if assign-entities fails
                try:
                    result = subprocess.run(command, check=True, capture_output=True, text=True)
                    print(f"Command executed successfully: {' '.join(command)}")
                    print(result.stdout)
                    successful_resources.append(resource_path)  

                except subprocess.CalledProcessError as e:
                    print(f"Command failed: {' '.join(command)}")
                    print(f"Error code: {e.returncode}")
                    print(f"Error output: {e.stderr}")
                    failed_assignments.append((row_number, resource, e.returncode, e.stderr))
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")
                    failed_assignments.append((row_number, resource, "Unexpected error", str(e)))

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
import requests
import csv
from datetime import datetime, timedelta, timezone
from io import StringIO
import urllib.parse
import time

NUMBER_OF_DAYS_BACK_TO_CHECK = 7


def csv_to_json(csv_text):
    reader = csv.DictReader(StringIO(csv_text))
    return list(reader)


def get_dataset_names():
    url = "https://api.github.com/repos/digital-land/config/contents/collection"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return [item['name'] for item in data if item['type'] == 'dir']


def get_filtered_endpoints(dataset_name, days_ago=NUMBER_OF_DAYS_BACK_TO_CHECK):
    csv_url = f"https://raw.githubusercontent.com/digital-land/config/refs/heads/main/collection/{dataset_name}/endpoint.csv"
    response = requests.get(csv_url)
    if response.status_code != 200:
        return []
    csv_text = response.text
    rows = csv_to_json(csv_text)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_ago)
    filtered = [
        row for row in rows
        if row.get("endpoint") and datetime.fromisoformat(row["entry-date"]).replace(tzinfo=timezone.utc) > cutoff_date
    ]
    return filtered


def check_count_match(dataset_name):
    base_api = "https://datasette.planning.data.gov.uk/digital-land/endpoint.json"
    endpoints = get_filtered_endpoints(dataset_name)
    if not endpoints:
        return True
    endpoint_hashes = [row["endpoint"] for row in endpoints]
    query_params = {"endpoint__in": ",".join(endpoint_hashes)}
    url = f"{base_api}?{urllib.parse.urlencode(query_params)}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        expected = len(endpoints)
        actual = data.get("filtered_table_rows_count", 0)
        if expected != actual:
            print(f"Check URL: {url}")
        return expected == actual
    except Exception as e:
        print(f"{dataset_name} - Error - {e}")
        print(f"Check URL: {url}")
        return False


def check_endpoints(dataset_name):
    base_api = "https://datasette.planning.data.gov.uk/digital-land/endpoint.json?_sort=endpoint&endpoint__exact="
    endpoints = get_filtered_endpoints(dataset_name)
    failed = []

    for endpoint in endpoints:
        encoded_endpoint = urllib.parse.quote(endpoint["endpoint"])
        url = base_api + encoded_endpoint
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            count = len(data.get("rows", []))

            if count == 0:
                print(f"{dataset_name} - {endpoint['endpoint']}: ❌ No rows")
                failed.append(endpoint["endpoint"])
        except Exception as e:
            print(f"{dataset_name} - {endpoint['endpoint']}: ⚠️ Error - {e}")
            failed.append(endpoint["endpoint"])

    return failed


def main():
    print(
        f"Checking endpoints for {NUMBER_OF_DAYS_BACK_TO_CHECK} days back...")
    failedEndpoints = []
    dataset_names = get_dataset_names()

    for dataset_name in dataset_names:
        count_match = check_count_match(dataset_name)
        print(f"{dataset_name} - {'✅' if count_match else '❌'}")

        if not count_match:
            failed = check_endpoints(dataset_name)
            if failed:
                failedEndpoints.append(failed)
                print(f"{dataset_name} - Failed endpoints: {', '.join(failed)}")

        # Delay between datasets to avoid rate limiting
        time.sleep(1)

    if len(failedEndpoints) > 0:
        raise Exception(f"Failed endpoints: {len(failedEndpoints)}")

    print("Done")


if __name__ == "__main__":
    main()

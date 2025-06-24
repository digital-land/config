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
    csv_url = (
        f"https://raw.githubusercontent.com/digital-land/config/refs/heads/main/"
        f"collection/{dataset_name}/endpoint.csv"
    )
    response = requests.get(csv_url)
    if response.status_code != 200:
        print(f"{dataset_name} - ⚠️ No endpoint.csv found")
        return []

    rows = csv_to_json(response.text)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_ago)
    filtered = []
    for row in rows:
        endpoint = row.get("endpoint")
        entry_date = row.get('entry-date')
        if not endpoint or not entry_date:
            continue
        try:
            dt = datetime.fromisoformat(entry_date)
        except ValueError:
            print(f"{dataset_name} - ⚠️ Invalid date format for endpoint {endpoint}: {entry_date}")
            continue
        # Normalize to UTC-aware datetime
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        if dt > cutoff_date:
            filtered.append(row)
    print(f"{dataset_name} - ▶️ Found {len(filtered)} new endpoint(s) in last {days_ago} day(s)")
    return filtered


def get_sources(dataset_name):
    csv_url = (
        f"https://raw.githubusercontent.com/digital-land/config/refs/heads/main/"
        f"collection/{dataset_name}/source.csv"
    )
    response = requests.get(csv_url)
    if response.status_code != 200:
        print(f"{dataset_name} - ⚠️ No source.csv found")
        return {}

    rows = csv_to_json(response.text)
    sources = {}
    for row in rows:
        endpoint = row.get('endpoint')
        organisation = row.get('organisation', '').strip()
        pipeline = row.get('pipeline', '').strip()
        if not endpoint:
            continue
        if endpoint not in sources:
            sources[endpoint] = {'organisations': set(), 'pipelines': set()}
        if organisation:
            sources[endpoint]['organisations'].add(organisation)
        if pipeline:
            sources[endpoint]['pipelines'].add(pipeline)
    return sources


def check_endpoints(dataset_name):
    base_api = (
        "https://datasette.planning.data.gov.uk/digital-land/endpoint.json"
        "?_sort=endpoint&endpoint__exact="
    )
    endpoints = get_filtered_endpoints(dataset_name)
    failed = []
    sources = get_sources(dataset_name)

    for row in endpoints:
        endpoint = row['endpoint']
        encoded = urllib.parse.quote(endpoint)
        url = base_api + encoded
        entry = sources.get(endpoint, {})
        org_label = ', '.join(sorted(entry.get('organisations', set()))) or 'Unknown org'
        pipe_label = ', '.join(sorted(entry.get('pipelines', set()))) or ''
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json().get('rows', [])
            if data:
                print(f"{dataset_name} - {org_label} [{pipe_label}] - {endpoint}: ✅ found")
            else:
                print(f"{dataset_name} - {org_label} [{pipe_label}] - {endpoint}: ⚠️ not found")
                failed.append(endpoint)
        except Exception as e:
            print(f"{dataset_name} - {org_label} [{pipe_label}] - {endpoint}: ❗ Error {e}")
            failed.append(endpoint)
    return failed


def main():
    print(f"Checking endpoints for {NUMBER_OF_DAYS_BACK_TO_CHECK} days back...")
    failedEndpoints = []
    dataset_names = get_dataset_names()

    for dataset_name in dataset_names:
        failures = check_endpoints(dataset_name)
        if failures:
            failedEndpoints.extend(failures)
            print(f"{dataset_name} - Failed endpoints: {', '.join(failures)}")
        time.sleep(1)

    if failedEndpoints:
        raise Exception(f"Failed endpoints: {len(failedEndpoints)}")

    print("Done")


if __name__ == "__main__":
    main()

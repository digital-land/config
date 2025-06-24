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

def check_endpoint(dataset_name, endpoint_hash, organisation_name, pipeline_label):
    url = (
        "https://datasette.planning.data.gov.uk/digital-land/endpoint.json"
        f"?_sort=endpoint&endpoint__exact={requests.utils.quote(endpoint_hash)}"
    )
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json().get('rows', [])
        label = organisation_name or 'Unknown org'
        pipeline_info = f"[{pipeline_label}]" if pipeline_label else ''
        if data:
            print(f"    ✅ {label} {pipeline_info} - Endpoint {endpoint_hash}: found")
            return True
        else:
            print(f"    ⚠️ {label} {pipeline_info} - Endpoint {endpoint_hash}: not found")
            return False
    except Exception as e:
        label = organisation_name or 'Unknown org'
        pipeline_info = f"[{pipeline_label}]" if pipeline_label else ''
        print(f"    ❗ {label} {pipeline_info} - Endpoint {endpoint_hash}: Error {e}")
        return False


def main():
    print(f"Checking endpoints added in the last {NUMBER_OF_DAYS_BACK_TO_CHECK} day(s):")
    datasets = get_dataset_names()
    failures = {}

    for ds in datasets:
        print(f"\nDataset: {ds}")
        filtered = get_filtered_endpoints(ds, NUMBER_OF_DAYS_BACK_TO_CHECK)
        if not filtered:
            continue
        source_map = get_source_map(ds)
        for row in filtered:
            h = row['endpoint']
            entry = source_map.get(h, {})
            orgs = entry.get('organisations', set())
            pipes = entry.get('pipelines', set())
            org_label = ', '.join(sorted(orgs)) if orgs else 'Unknown org'
            pipe_label = ', '.join(sorted(pipes)) if pipes else ''
            ok = check_endpoint(ds, h, org_label, pipe_label)
            if not ok:
                failures.setdefault(ds, []).append(h)
        time.sleep(1)

    if failures:
        print("\nSummary of failures:")
        for ds, fails in failures.items():
            print(f"  - {ds}: {len(fails)} failure(s)")
        sys.exit(1)
    else:
        print("\nAll endpoints checked and OK!")


if __name__ == '__main__':
    main()

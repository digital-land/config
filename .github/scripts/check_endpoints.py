import sys
import time
import requests
import csv
import io
from datetime import datetime, timedelta

# Number of days back to check
NUMBER_OF_DAYS_BACK_TO_CHECK = 7


def csv_to_json(csv_text):
    reader = csv.DictReader(io.StringIO(csv_text))
    return list(reader)


def get_dataset_names():
    url = "https://api.github.com/repos/digital-land/config/contents/collection"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    return [d['name'] for d in data if d['type'] == 'dir']


def get_filtered_endpoints(dataset_name, days_ago):
    url = f"https://raw.githubusercontent.com/digital-land/config/main/collection/{dataset_name}/endpoint.csv"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"  ⚠️ No endpoint.csv for dataset '{dataset_name}'")
        return []
    rows = csv_to_json(resp.text)
    cutoff = datetime.utcnow() - timedelta(days=days_ago)
    filtered = [r for r in rows if r.get("endpoint")
                and datetime.fromisoformat(r['entry-date']) >= cutoff]
    print(f"  ▶️ Found {len(filtered)} new endpoint(s) in last {days_ago} day(s)")
    return filtered


def get_source_map(dataset_name):
    url = f"https://raw.githubusercontent.com/digital-land/config/main/collection/{dataset_name}/source.csv"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"  ⚠️ No source.csv for dataset '{dataset_name}'")
        return {}
    rows = csv_to_json(resp.text)
    mapping = {}
    for r in rows:
        endpoint = r.get('endpoint')
        organisation = r.get('organisation', '').strip()
        pipeline = r.get('pipeline', '').strip()
        if not endpoint:
            continue
        if endpoint not in mapping:
            mapping[endpoint] = {'organisations': set(), 'pipelines': set()}
        if organisation:
            mapping[endpoint]['organisations'].add(organisation)
        if pipeline:
            mapping[endpoint]['pipelines'].add(pipeline)
    return mapping


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

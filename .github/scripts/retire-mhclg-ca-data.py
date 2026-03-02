#!/usr/bin/env python3
"""
Retire MHCLG conservation-area data.

This script retires conservation-area and conservation-area-document datasets
for Local Planning Authorities (LPAs) that are in the Open Digital Planning (ODP)
programme by:
- Marking endpoints as retired (adding end-dates)
- Recording retired resources in old-resource.csv
"""

import csv
import json
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path

DATASETTE_URL = 'https://datasette.planning.data.gov.uk'

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CA_COLLECTION = REPO_ROOT / 'collection' / 'conservation-area'
ENDPOINT_PATH = CA_COLLECTION / 'endpoint.csv'
SOURCE_PATH = CA_COLLECTION / 'source.csv'
OLD_RESOURCE_PATH = CA_COLLECTION / 'old-resource.csv'


def execute_datasette_query(database, sql):
    """Execute a SQL query against Datasette and return all results with pagination."""
    all_rows = []
    offset = 0

    while True:
        # Add LIMIT and OFFSET to query
        paginated_sql = f"{sql} LIMIT 1000 OFFSET {offset}"
        url = f"{DATASETTE_URL}/{database}.json"
        params = urllib.parse.urlencode({"sql": paginated_sql, "_shape": "array"})
        full_url = f"{url}?{params}"

        try:
            with urllib.request.urlopen(full_url) as response:
                data = json.loads(response.read().decode('utf-8'))

            if not data:
                break

            all_rows.extend(data)

            # If we got fewer than 1000 rows, we've reached the end
            if len(data) < 1000:
                break

            offset += 1000
        except Exception as e:
            print(f"Error executing query: {e}")
            raise

    return all_rows


def get_odp_organisations():
    """Load and return set of organisations in the Open Digital Planning programme."""
    print("Loading ODP organisations from provision table...")
    try:
        sql = """
            SELECT DISTINCT organisation
            FROM provision
            WHERE project = 'open-digital-planning'
        """
        rows = execute_datasette_query('digital-land', sql)
        odp_orgs = set(row['organisation'] for row in rows)
        print(f"Found {len(odp_orgs)} organisations in ODP")
        return odp_orgs
    except Exception as e:
        print(f"Error loading ODP organisations: {e}")
        raise


def get_ca_endpoints_in_odp(odp_orgs):
    """Fetch historic endpoints and filter for CA datasets in ODP organisations."""
    print("Loading historic endpoints...")
    try:
        sql = """
            SELECT *
            FROM reporting_historic_endpoints
            WHERE dataset IN ('conservation-area', 'conservation-area-document')
        """
        all_rows = execute_datasette_query('performance', sql)
        print(f"Loaded {len(all_rows)} endpoints")

        # Extract organisation code and check if in ODP
        endpoints_in_odp = []
        for row in all_rows:
            endpoint_url = row.get('endpoint_url', '')
            # Extract org code from URL (e.g., https://.../camden-... -> camden)
            org_code = 'local-authority:' + endpoint_url.split('/')[-1].split('-')[0]

            if org_code in odp_orgs:
                endpoints_in_odp.append(row)

        print(f"Found {len(endpoints_in_odp)} endpoints in ODP organisations")
        return endpoints_in_odp
    except Exception as e:
        print(f"Error loading endpoints: {e}")
        raise


def update_csv_with_end_dates(file_path, endpoints_to_retire):
    """Update end-dates for records matching given endpoints."""
    try:
        # Read existing file
        rows = []
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = list(reader)

        # Update end-dates for matching endpoints
        today = datetime.now().strftime('%Y-%m-%d')
        updated_count = 0

        for row in rows:
            if row.get('endpoint') in endpoints_to_retire:
                row['end-date'] = today
                updated_count += 1

        # Write back to file
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return updated_count
    except Exception as e:
        print(f"Error updating {file_path}: {e}")
        raise


def update_endpoint_dates(endpoints_in_odp):
    """Update end-dates for endpoints and sources."""
    print(f"Updating endpoint and source records at {CA_COLLECTION}...")
    try:
        endpoints_to_retire = set(row.get('endpoint') for row in endpoints_in_odp)

        # Update source.csv
        source_count = update_csv_with_end_dates(SOURCE_PATH, endpoints_to_retire)
        print(f"Updated {source_count} source records")

        # Update endpoint.csv
        endpoint_count = update_csv_with_end_dates(ENDPOINT_PATH, endpoints_to_retire)
        print(f"Updated {endpoint_count} endpoint records")
    except Exception as e:
        print(f"Error updating endpoint dates: {e}")
        raise


def get_resources_for_retirement(endpoints_in_odp):
    """Fetch resource data and extract resources for endpoints being retired."""
    print("Loading resource data...")
    try:
        sql = """
            SELECT *
            FROM reporting_historic_endpoints
        """
        all_rows = execute_datasette_query('performance', sql)
        print(f"Loaded {len(all_rows)} resource records")

        # Get resources for endpoints we're retiring
        endpoints_set = set(row.get('endpoint') for row in endpoints_in_odp)
        resources_to_retire = [
            row.get('resource') for row in all_rows
            if row.get('endpoint') in endpoints_set and row.get('resource')
        ]

        print(f"Found {len(resources_to_retire)} resources to retire")
        return resources_to_retire
    except Exception as e:
        print(f"Error loading resources: {e}")
        raise


def update_old_resource_csv(resources_to_retire):
    """Create retirement records and update old-resource.csv."""
    print(f"Updating old-resource.csv at {OLD_RESOURCE_PATH}...")
    try:
        # Read existing records
        existing_rows = []
        with open(OLD_RESOURCE_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            existing_rows = list(reader)

        # Create retirement records
        new_rows = [
            {
                'old-resource': resource,
                'status': '410',
                'resource': '',
                'notes': 'Remove scraped data as we have data from authoritative source'
            }
            for resource in resources_to_retire
        ]

        # Combine and save
        all_rows = existing_rows + new_rows

        fieldnames = ['old-resource', 'status', 'resource', 'notes']
        with open(OLD_RESOURCE_PATH, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)

        print(f"Added {len(new_rows)} new records to old-resource.csv")
        print(f"Total records now: {len(all_rows)}")
    except Exception as e:
        print(f"Error updating old-resource.csv: {e}")
        raise


def main():
    """Main execution function."""
    try:
        # Get ODP organisations
        odp_orgs = get_odp_organisations()

        # Get CA endpoints in ODP
        endpoints_in_odp = get_ca_endpoints_in_odp(odp_orgs)

        if len(endpoints_in_odp) == 0:
            print("No endpoints to retire. Exiting.")
            return

        # Update endpoint and source records
        update_endpoint_dates(endpoints_in_odp)

        # Get resources and update old-resource.csv
        resources_to_retire = get_resources_for_retirement(endpoints_in_odp)
        if len(resources_to_retire) > 0:
            update_old_resource_csv(resources_to_retire)

        print("\nRetirement complete!")
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == '__main__':
    main()

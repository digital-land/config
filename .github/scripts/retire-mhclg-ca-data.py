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


def get_odp_organisations_for_dataset(dataset_name):
    """Load and return set of ODP organisations that have provided a specific dataset."""
    try:
        sql = f"""
            SELECT DISTINCT organisation
            FROM provision
            WHERE project = 'open-digital-planning'
            AND dataset = '{dataset_name}'
        """
        rows = execute_datasette_query('digital-land', sql)
        odp_orgs = set(row['organisation'] for row in rows)
        print(f"Found {len(odp_orgs)} ODP organisations with {dataset_name} dataset")
        return odp_orgs
    except Exception as e:
        print(f"Error loading ODP organisations for {dataset_name}: {e}")
        raise


def get_endpoints_by_dataset(dataset_name):
    """Fetch MHCLG endpoints where ACTIVE authoritative LPA data also exists.

    For each LPA, check if there is BOTH:
    - MHCLG data (organisation='government-organisation:D1342')
    - ACTIVE authoritative data from LPA (organisation != 'government-organisation:D1342' AND endpoint_end_date is empty)

    Only retire MHCLG endpoints for LPAs that have both sources, and where the authoritative data is still active.
    """
    print(f"\nProcessing {dataset_name} dataset...")
    try:
        # Get ALL endpoints for this dataset (not filtered by organisation)
        sql = f"""
            SELECT endpoint, endpoint_url, organisation, endpoint_end_date
            FROM reporting_historic_endpoints
            WHERE pipeline = '{dataset_name}'
        """
        all_rows = execute_datasette_query('performance', sql)
        print(f"Loaded {len(all_rows)} total {dataset_name} endpoints")

        # Group by LPA/organisation code
        endpoints_by_lpa = {}
        for row in all_rows:
            org = row.get('organisation', '')

            # Skip Historic England (never retire their data)
            if org == 'government-organisation:PB1164':
                continue

            # Extract organisation/LPA code
            # MHCLG: extract from URL (e.g., ADU-conservation-area.csv -> ADU)
            # Others: use organisation code (local-authority:ADU, national-park:*, development-corporation:*, etc.)
            if org == 'government-organisation:D1342':
                endpoint_url = row.get('endpoint_url', '')
                lpa_code = endpoint_url.split('/')[-1].split('-')[0]
            else:
                # For local-authority, national-park, development-corporation, etc.
                # Extract the code after the colon (e.g., local-authority:ADU -> ADU)
                if ':' in org:
                    lpa_code = org.split(':')[1]
                else:
                    # Skip if can't parse
                    continue

            if lpa_code not in endpoints_by_lpa:
                endpoints_by_lpa[lpa_code] = {'mhclg': [], 'authoritative_active': [], 'authoritative_retired': []}

            if org == 'government-organisation:D1342':
                endpoints_by_lpa[lpa_code]['mhclg'].append(row)
            else:
                # Separate active and retired authoritative endpoints
                if row.get('endpoint_end_date'):
                    endpoints_by_lpa[lpa_code]['authoritative_retired'].append(row)
                else:
                    endpoints_by_lpa[lpa_code]['authoritative_active'].append(row)

        # Find LPAs with both MHCLG and ACTIVE authoritative data
        endpoints_to_retire = []
        lpas_with_both = set()
        mhclg_only = 0
        auth_only_active = 0
        auth_only_retired = 0
        for lpa, data in endpoints_by_lpa.items():
            has_mhclg = len(data['mhclg']) > 0
            has_auth_active = len(data['authoritative_active']) > 0
            has_auth_retired = len(data['authoritative_retired']) > 0

            if has_mhclg and has_auth_active:
                # Only retire MHCLG if active authoritative alternative exists
                endpoints_to_retire.extend(data['mhclg'])
                lpas_with_both.add(lpa)
            elif has_mhclg and not has_auth_active:
                # MHCLG-only: keep it (no active alternative)
                mhclg_only += 1
            elif not has_mhclg and has_auth_active:
                # Active authoritative-only
                auth_only_active += 1
            elif not has_mhclg and has_auth_retired:
                # Retired authoritative-only
                auth_only_retired += 1

        print(f"Found {len(lpas_with_both)} LPAs with both MHCLG and ACTIVE authoritative data")
        print(f"  MHCLG-only LPAs: {mhclg_only}")
        print(f"  Active authoritative-only LPAs: {auth_only_active}")
        print(f"  Retired authoritative-only LPAs: {auth_only_retired}")
        print(f"Retiring {len(endpoints_to_retire)} MHCLG {dataset_name} endpoints")

        # Debug: show sample data if nothing to retire
        if len(endpoints_to_retire) == 0 and len(all_rows) > 0:
            print(f"\nDEBUG: Sample endpoints from {dataset_name}:")
            for i, row in enumerate(all_rows[:3]):
                print(f"  [{i}] endpoint_url: {row.get('endpoint_url', 'N/A')[:80]}")
                print(f"      organisation: {row.get('organisation', 'N/A')}")

        return endpoints_to_retire, lpas_with_both
    except Exception as e:
        print(f"Error loading {dataset_name} endpoints: {e}")
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

        # Update end-dates for matching endpoints that haven't been retired yet
        today = datetime.now().strftime('%Y-%m-%d')
        updated_count = 0

        for row in rows:
            # Only update if endpoint matches AND it doesn't already have an end-date
            if row.get('endpoint') in endpoints_to_retire and not row.get('end-date'):
                row['end-date'] = today
                updated_count += 1

        # Write back to file
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            # Remove None keys that can appear from malformed CSV rows
            cleaned_rows = [{k: v for k, v in row.items() if k is not None} for row in rows]
            writer.writerows(cleaned_rows)

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
    """Fetch resource data for endpoints being retired.

    Excludes resources that are already in old-resource.csv to avoid
    adding duplicate retirement records.
    """
    print("Loading resource data...")
    try:
        # Read old-resource.csv to get list of already-retired resources
        already_retired_resources = set()
        with open(OLD_RESOURCE_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                already_retired_resources.add(row.get('old-resource'))

        sql = """
            SELECT *
            FROM reporting_historic_endpoints
        """
        all_rows = execute_datasette_query('performance', sql)
        print(f"Loaded {len(all_rows)} resource records")

        # Get resources for endpoints we're retiring (excluding those already in old-resource.csv)
        endpoints_set = set(row.get('endpoint') for row in endpoints_in_odp)
        resources_to_retire = [
            row.get('resource') for row in all_rows
            if row.get('endpoint') in endpoints_set
            and row.get('resource')
            and row.get('resource') not in already_retired_resources
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
    """Main execution function.

    Processes conservation-area and conservation-area-document datasets separately.
    For each dataset, retires MHCLG endpoints where authoritative LPA data also exists.
    """
    try:
        all_resources_to_retire = []
        all_retired_lpas = set()

        # Process conservation-area dataset
        print("\n" + "="*60)
        print("RETIRING CONSERVATION-AREA ENDPOINTS")
        print("="*60)
        ca_endpoints, ca_retired_lpas = get_endpoints_by_dataset('conservation-area')
        all_retired_lpas.update(ca_retired_lpas)
        if len(ca_endpoints) > 0:
            update_endpoint_dates(ca_endpoints)
            ca_resources = get_resources_for_retirement(ca_endpoints)
            all_resources_to_retire.extend(ca_resources)
            print(f"Conservation-area: {len(ca_endpoints)} endpoints, {len(ca_resources)} resources")
        else:
            print("Conservation-area: No endpoints to retire")

        # Process conservation-area-document dataset
        print("\n" + "="*60)
        print("RETIRING CONSERVATION-AREA-DOCUMENT ENDPOINTS")
        print("="*60)
        cad_endpoints, cad_retired_lpas = get_endpoints_by_dataset('conservation-area-document')
        all_retired_lpas.update(cad_retired_lpas)
        if len(cad_endpoints) > 0:
            update_endpoint_dates(cad_endpoints)
            cad_resources = get_resources_for_retirement(cad_endpoints)
            all_resources_to_retire.extend(cad_resources)
            print(f"Conservation-area-document: {len(cad_endpoints)} endpoints, {len(cad_resources)} resources")
        else:
            print("Conservation-area-document: No endpoints to retire")

        # Update old-resource.csv with all collected resources
        if len(all_resources_to_retire) > 0:
            print("\n" + "="*60)
            print("UPDATING OLD-RESOURCE CSV")
            print("="*60)
            update_old_resource_csv(all_resources_to_retire)

        # Final check: report how many retired LPAs are in ODP
        if len(all_retired_lpas) > 0:
            print("\n" + "="*60)
            print("ODP COVERAGE CHECK")
            print("="*60)
            ca_odp_orgs = get_odp_organisations_for_dataset('conservation-area')
            cad_odp_orgs = get_odp_organisations_for_dataset('conservation-area-document')
            all_odp_orgs = ca_odp_orgs | cad_odp_orgs

            retired_in_odp = 0
            for lpa in all_retired_lpas:
                if f'local-authority:{lpa}' in all_odp_orgs:
                    retired_in_odp += 1

            print(f"LPAs where MHCLG data was retired: {len(all_retired_lpas)}")
            print(f"  Of these, in ODP: {retired_in_odp}")
            print(f"  Of these, NOT in ODP: {len(all_retired_lpas) - retired_in_odp}")

        print("\n" + "="*60)
        print("RETIREMENT COMPLETE!")
        print("="*60)
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == '__main__':
    main()

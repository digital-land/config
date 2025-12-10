import csv
import requests
import sys
import pandas as pd
import os
import shutil
import logging

from pathlib import Path
from io import StringIO
from digital_land.commands import check_and_assign_entities

# ----------------------------
# CONFIG
# ----------------------------

SCOPE = "single-source"

BASE_DIR = Path.cwd()
CACHE_ROOT = BASE_DIR / "var" / "cache"
PIPELINE_TMP = BASE_DIR / "var" / "tmp" / "pipeline"
RESOURCE_DIR = BASE_DIR / "resource"
SPEC_DIR = BASE_DIR / "specification"

CACHE_ROOT.mkdir(parents=True, exist_ok=True)
PIPELINE_TMP.mkdir(parents=True, exist_ok=True)
RESOURCE_DIR.mkdir(parents=True, exist_ok=True)
SPEC_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# DOWNLOAD UTILITY
# ----------------------------

def download_file(url, local_path):
    local_path = Path(local_path)
    if not local_path.exists():
        local_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Downloading {url}")
        r = requests.get(url)
        r.raise_for_status()
        local_path.write_bytes(r.content)

# ----------------------------
# LOAD PREVIOUS RESOURCE FOR DEDUPE
# ----------------------------

def get_old_resource_df(endpoint, collection_name, dataset):
    url = (
        f"https://datasette.planning.data.gov.uk/performance/reporting_historic_endpoints.csv"
        f"?_sort=rowid&resource_end_date__notblank=1&endpoint__exact={endpoint}&_size=1"
    )
    r = requests.get(url)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))

    if df.empty:
        return None

    old_hash = df["resource"][0]

    transformed_url = (
        f"https://files.planning.data.gov.uk/"
        f"{collection_name}-collection/transformed/{dataset}/{old_hash}.csv"
    )

    r2 = requests.get(transformed_url)
    r2.raise_for_status()
    return pd.read_csv(StringIO(r2.text))

# ----------------------------
# MAIN PROCESSOR
# ----------------------------

def process_csv(scope):
    failures = []

    with open("issue_summary.csv", "r") as f:
        reader = csv.DictReader(f)

        for row_number, row in enumerate(reader, start=1):

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

            # ✅ SAFE TEMP PIPELINE DIR
            pipeline_work_dir = PIPELINE_TMP / collection_name
            pipeline_work_dir.mkdir(parents=True, exist_ok=True)

            resource_url = (
                f"https://files.planning.data.gov.uk/"
                f"{collection_name}-collection/collection/resource/{resource}"
            )

            resource_path = RESOURCE_DIR / resource

            try:
                r = requests.get(resource_url)
                r.raise_for_status()
                resource_path.write_bytes(r.content)

            except Exception as e:
                failures.append((row_number, resource, str(e)))
                continue

            transformed_output = (
                CACHE_ROOT
                / "assign_entities"
                / "transformed"
                / f"{resource}.csv"
            )

            try:
                success = check_and_assign_entities(
                    [resource_path],
                    [endpoint],
                    collection_name,
                    dataset,
                    [organisation_name],
                    Path(f"collection/{collection_name}"),
                    CACHE_ROOT / "organisation.csv",
                    SPEC_DIR,
                    pipeline_work_dir,
                    transformed_output,
                )

                if not success:
                    continue

                # ✅ COPY LOOKUP ONLY (NOT ENTIRE PIPELINE)
                generated_lookup = (
                    pipeline_work_dir / "pipeline" / "lookup.csv"
                )

                final_lookup = (
                    BASE_DIR / "pipeline" / collection_name / "lookup.csv"
                )

                if generated_lookup.exists():
                    final_lookup.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(generated_lookup, final_lookup)
                    print(f"Updated lookup.csv → {final_lookup}")

                else:
                    print(f"⚠️ lookup.csv not produced for {collection_name}")

            except Exception as e:
                logging.exception("Assignment failed")
                failures.append((row_number, resource, str(e)))

            finally:
                # ✅ CLEAN TEMP FILES
                if resource_path.exists():
                    resource_path.unlink()

    print("\n--- Summary ---")
    if failures:
        print("Failures:", failures)
    else:
        print("✅ All entity assignment operations completed successfully")

# ----------------------------
# MAIN ENTRY
# ----------------------------

if __name__ == "__main__":

    # ✅ REQUIRED SPEC FILE
    download_file(
        "https://raw.githubusercontent.com/digital-land/specification/main/content/provision-rule.csv",
        SPEC_DIR / "provision-rule.csv",
    )

    # ✅ ISSUE SUMMARY
    issue_url = (
        "https://datasette.planning.data.gov.uk/performance/"
        "endpoint_dataset_issue_type_summary.csv"
        "?_sort=rowid&issue_type__exact=unknown+entity&_size=max"
    )

    df = pd.read_csv(issue_url)

    provision_df = pd.read_csv(SPEC_DIR / "provision-rule.csv")

    scope_dict = {
        "odp": provision_df.loc[provision_df["project"] == "open-digital-planning", "dataset"].tolist(),
        "mandated": provision_df.loc[
            (provision_df["provision-reason"] == "statutory") |
            ((provision_df["provision-reason"] == "encouraged") &
             (provision_df["role"] == "local-planning-authority")),
            "dataset"
        ].tolist(),
    }

    def determine_scope(dataset):
        if dataset in scope_dict["odp"]:
            return "odp"
        elif dataset in scope_dict["mandated"]:
            return "mandated"
        return "single-source"

    df["scope"] = df["dataset"].apply(determine_scope)
    df.to_csv("issue_summary.csv", index=False)
    print("✅ issue_summary.csv generated")

    process_csv(SCOPE)

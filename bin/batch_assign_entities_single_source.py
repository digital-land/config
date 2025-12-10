import csv
import requests
import pandas as pd
import shutil
import logging
from pathlib import Path
from io import StringIO

from digital_land.commands import check_and_assign_entities

# ----------------------------
# CONFIG
# ----------------------------
SCOPE = "single-source"
SPEC_REPO_RAW_BASE = "https://raw.githubusercontent.com/digital-land/specification/main"

TMP_ROOT = Path("var/tmp")
CACHE_ROOT = Path("var/cache")
RESOURCE_TMP = TMP_ROOT / "resource"
PIPELINE_TMP = TMP_ROOT / "pipeline"
COLLECTION_TMP = TMP_ROOT / "collection"

for p in [RESOURCE_TMP, PIPELINE_TMP, COLLECTION_TMP]:
    p.mkdir(parents=True, exist_ok=True)

# ----------------------------
# UTILS
# ----------------------------
def download_file(url, local_path):
    local_path = Path(local_path)
    if not local_path.exists():
        local_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"Downloading {url} -> {local_path}")
        r = requests.get(url)
        r.raise_for_status()
        local_path.write_bytes(r.content)
        print(f"Downloaded {local_path}")


def ensure_specification_files():
    spec_files = {
        "specification/dataset.csv": f"{SPEC_REPO_RAW_BASE}/specification/dataset.csv",
        "specification/field.csv": f"{SPEC_REPO_RAW_BASE}/specification/field.csv",
        "specification/dataset-field.csv": f"{SPEC_REPO_RAW_BASE}/specification/dataset-field.csv",
        "specification/typology.csv": f"{SPEC_REPO_RAW_BASE}/specification/typology.csv",
        "specification/datatype.csv": f"{SPEC_REPO_RAW_BASE}/specification/datatype.csv",
        "specification/provision-rule.csv": f"{SPEC_REPO_RAW_BASE}/content/provision-rule.csv",
    }
    for local_path, url in spec_files.items():
        try:
            download_file(url, local_path)
        except Exception as e:
            raise RuntimeError(f"FAILED to download spec file {url}: {e}")


def get_old_resource_df(endpoint, collection_name, dataset):
    url = (
        "https://datasette.planning.data.gov.uk/performance/"
        "reporting_historic_endpoints.csv"
        f"?_sort=rowid&resource_end_date__notblank=1&endpoint__exact={endpoint}&_size=1"
    )
    r = requests.get(url)
    r.raise_for_status()
    prev_df = pd.read_csv(StringIO(r.text))

    if len(prev_df) == 0:
        return None

    old_hash = prev_df["resource"][0]
    transformed_url = (
        f"https://files.planning.data.gov.uk/"
        f"{collection_name}-collection/transformed/{dataset}/{old_hash}.csv"
    )
    tr = requests.get(transformed_url)
    tr.raise_for_status()
    return pd.read_csv(StringIO(tr.text))


def get_field_value_map(df, entity_number):
    sub_df = df[
        (df["entity"] == entity_number)
        & (df["field"] != "reference")
        & (df["field"] != "entry-date")
    ]
    return dict(zip(sub_df["field"], sub_df["value"]))


def find_latest_lookup(collection_name):
    root = CACHE_ROOT / "assign_entities"
    matches = []

    if root.exists():
        for p in root.rglob("lookup.csv"):
            if collection_name in p.parts:
                matches.append(p)

    if not matches:
        raise FileNotFoundError(
            f"No lookup.csv found for collection {collection_name} under {root}"
        )

    return sorted(matches, key=lambda p: len(p.parts))[-1]


# ----------------------------
# MAIN ASSIGNMENT PROCESS
# ----------------------------
def process_csv(scope):
    failed_downloads = []
    failed_assignments = []

    with open("issue_summary.csv", "r") as f:
        reader = csv.DictReader(f)

        for row_number, row in enumerate(reader, start=1):

            issue_type = str(row.get("issue_type", "")).lower().strip()
            row_scope = str(row.get("scope", "")).lower().strip()
            dataset_name = str(row.get("dataset", "")).lower().strip()

            if (
                issue_type != "unknown entity"
                or row_scope != scope
                or dataset_name == "title-boundary"
            ):
                continue

            collection_name = row["collection"]
            resource = row["resource"]
            endpoint = row["endpoint"]
            dataset = row["pipeline"]
            organisation_name = row["organisation"]

            download_link = (
                f"https://files.planning.data.gov.uk/"
                f"{collection_name}-collection/collection/resource/{resource}"
            )

            resource_path = RESOURCE_TMP / resource

            # ---- DOWNLOAD RESOURCE (TEMP ONLY) ----
            try:
                r = requests.get(download_link)
                r.raise_for_status()
                resource_path.write_bytes(r.content)
                print(f"Downloaded: {resource}")
            except Exception as e:
                failed_downloads.append((row_number, resource, str(e)))
                continue

            try:
                # ---- RUN ENTITY ASSIGNMENT (ALL OUTPUTS TO var/tmp & var/cache) ----
                success = check_and_assign_entities(
                    [resource_path],
                    [endpoint],
                    collection_name,
                    dataset,
                    [organisation_name],
                    COLLECTION_TMP / collection_name,
                    CACHE_ROOT / "organisation.csv",
                    Path("specification"),
                    PIPELINE_TMP / collection_name,
                    CACHE_ROOT / "assign_entities" / "transformed" / f"{resource}.csv",
                )

                if not success:
                    raise RuntimeError("check_and_assign_entities returned False")

                # ---- DUPLICATE DETECTION ----
                old_df = get_old_resource_df(endpoint, collection_name, dataset)

                if old_df is not None:
                    current_df = pd.read_csv(
                        CACHE_ROOT
                        / "assign_entities"
                        / "transformed"
                        / f"{resource}.csv"
                    )

                    new_entities = set(current_df["entity"]) - set(old_df["entity"])

                    field_map_to_old = {}
                    for old_entity in old_df["entity"].unique():
                        field_map = tuple(
                            sorted(get_field_value_map(old_df, old_entity).items())
                        )
                        field_map_to_old[field_map] = old_entity

                    for entity in new_entities:
                        current_fields = tuple(
                            sorted(get_field_value_map(current_df, entity).items())
                        )
                        if current_fields in field_map_to_old:
                            print(
                                f"Duplicate detected: {entity} -> "
                                f"{field_map_to_old[current_fields]}"
                            )

                # ---- COPY LOOKUP (ONLY FILE THAT TOUCHES GIT) ----
                src_lookup = find_latest_lookup(collection_name)

                dest_lookup = Path("pipeline") / collection_name / "lookup.csv"
                dest_lookup.parent.mkdir(parents=True, exist_ok=True)

                shutil.copy(src_lookup, dest_lookup)
                print(f"✅ lookup.csv updated for {collection_name}")

            except Exception as e:
                logging.error(str(e), exc_info=True)
                failed_assignments.append((row_number, resource, str(e)))

    print("\n--- Summary Report ---")
    if failed_downloads:
        print("Failed Downloads:", failed_downloads)
    if failed_assignments:
        print("Failed Assignments:", failed_assignments)

    return failed_downloads, failed_assignments


# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":

    ensure_specification_files()

    # ---- DOWNLOAD UNKNOWN ENTITY REPORT ----
    issue_url = (
        "https://datasette.planning.data.gov.uk/performance/"
        "endpoint_dataset_issue_type_summary.csv"
        "?_sort=rowid&issue_type__exact=unknown+entity&_size=max"
    )

    r = requests.get(issue_url)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))

    provision_rule_df = pd.read_csv("specification/provision-rule.csv")

    scope_dict = {
        "odp": provision_rule_df.loc[
            provision_rule_df["project"] == "open-digital-planning", "dataset"
        ].tolist(),
        "mandated": provision_rule_df.loc[
            (provision_rule_df["provision-reason"] == "statutory")
            | (
                (provision_rule_df["provision-reason"] == "encouraged")
                & (provision_rule_df["role"] == "local-planning-authority")
            ),
            "dataset",
        ].tolist(),
    }

    def determine_scope(dataset):
        if dataset in scope_dict["odp"]:
            return "odp"
        elif dataset in scope_dict["mandated"]:
            return "mandated"
        else:
            return "single-source"

    df["scope"] = df["dataset"].apply(determine_scope)
    df.to_csv("issue_summary.csv", index=False)
    print("✅ issue_summary.csv generated")

    failed_downloads, failed_assignments = process_csv(SCOPE)

    print(f"Failed downloads: {len(failed_downloads)}")
    print(f"Failed assign-entities operations: {len(failed_assignments)}")

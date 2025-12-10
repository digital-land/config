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

RESOURCE_TMP.mkdir(parents=True, exist_ok=True)
PIPELINE_TMP.mkdir(parents=True, exist_ok=True)

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
        download_file(url, local_path)


def find_latest_lookup(collection_name):
    root = CACHE_ROOT / "assign_entities"
    candidates = []

    if root.exists():
        for p in root.rglob("lookup.csv"):
            if collection_name in p.parts:
                candidates.append(p)

    if not candidates:
        raise FileNotFoundError(f"No lookup.csv found for {collection_name}")

    return sorted(candidates, key=lambda p: len(p.parts))[-1]


# ----------------------------
# MAIN PROCESS
# ----------------------------
def process_csv(scope):

    failed = []

    with open("issue_summary.csv", "r") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start=1):

            if str(row["issue_type"]).lower().strip() != "unknown entity":
                continue
            if str(row["scope"]).lower().strip() != scope:
                continue
            if str(row["dataset"]).lower().strip() == "title-boundary":
                continue

            collection_name = row["collection"]
            resource = row["resource"]
            endpoint = row["endpoint"]
            dataset = row["pipeline"]
            organisation_name = row["organisation"]

            download_url = (
                f"https://files.planning.data.gov.uk/"
                f"{collection_name}-collection/collection/resource/{resource}"
            )

            resource_path = RESOURCE_TMP / resource

            try:
                r = requests.get(download_url)
                r.raise_for_status()
                resource_path.write_bytes(r.content)

                success = check_and_assign_entities(
                    [resource_path],
                    [endpoint],
                    collection_name,
                    dataset,
                    [organisation_name],

                    # ✅ REAL collection directory (REQUIRED)
                    Path(f"collection/{collection_name}"),

                    CACHE_ROOT / "organisation.csv",
                    Path("specification"),

                    # ✅ TEMP pipeline output
                    PIPELINE_TMP / collection_name,

                    CACHE_ROOT / "assign_entities" / "transformed" / f"{resource}.csv",
                )

                if not success:
                    raise RuntimeError("check_and_assign_entities returned False")

                # ✅ ONLY COPY lookup.csv INTO GIT
                src_lookup = find_latest_lookup(collection_name)
                dest_lookup = Path("pipeline") / collection_name / "lookup.csv"
                dest_lookup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(src_lookup, dest_lookup)

                print(f"✅ lookup.csv updated for {collection_name}")

            except Exception as e:
                failed.append((i, resource, str(e)))
                logging.error(str(e), exc_info=True)

    print("\n--- Summary ---")
    if failed:
        print("Failures:", failed)
    else:
        print("✅ All assignments successful")

    return failed


# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":

    ensure_specification_files()

    issue_url = (
        "https://datasette.planning.data.gov.uk/performance/"
        "endpoint_dataset_issue_type_summary.csv"
        "?_sort=rowid&issue_type__exact=unknown+entity&_size=max"
    )

    df = pd.read_csv(StringIO(requests.get(issue_url).text))

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

    process_csv(SCOPE)

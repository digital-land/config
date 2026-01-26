import csv
import json
import logging
import shutil
import sys
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from digital_land.commands import check_and_assign_entities

# ----------------------------
# CONFIGURATION FOR GITHUB ACTIONS
# ----------------------------
SCOPE = "single-source"   # hard-coded
TIMEOUT = (10, 60)        # (connect, read) seconds
RESOURCES_DIR = Path("./resource")
CACHE_DIR = Path("var/cache/")
REPORTS_DIR = Path("reports")

# ----------------------------
# LOGGING
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ----------------------------
# HTTP SESSION WITH RETRIES
# ----------------------------
def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

SESSION = make_session()

# ----------------------------
# UTILITY FUNCTIONS
# ----------------------------
def http_get(url: str) -> requests.Response:
    resp = SESSION.get(url, timeout=TIMEOUT)
    # If we got a retriable error status after retries, raise for clarity
    resp.raise_for_status()
    return resp

def download_file(url: str, local_path: str | Path) -> None:
    """Download file from URL if it doesn't exist locally."""
    local_path = Path(local_path)
    if local_path.exists():
        logging.info("Exists, skipping download: %s", local_path)
        return

    local_path.parent.mkdir(parents=True, exist_ok=True)
    logging.info("Downloading %s -> %s", url, local_path)
    resp = http_get(url)
    local_path.write_bytes(resp.content)
    logging.info("Downloaded %s (%d bytes)", local_path, len(resp.content))

def read_csv_from_url(url: str) -> pd.DataFrame:
    resp = http_get(url)
    return pd.read_csv(StringIO(resp.text))

def read_csv_from_path(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Expected CSV does not exist: {path}")
    return pd.read_csv(path)

def get_old_resource_df(endpoint: str, collection_name: str, dataset: str) -> pd.DataFrame | None:
    """
    Return transformed file for a previous ended resource using endpoint hash from CDN.

    NOTE: The earlier code said "second latest" but fetched only 1 row.
    Here we fetch up to 2 ended resources and use the most recent one available.
    """
    url = (
        "https://datasette.planning.data.gov.uk/performance/reporting_historic_endpoints.csv"
        f"?_sort=rowid&resource_end_date__notblank=1&endpoint__exact={endpoint}&_size=2"
    )
    df = read_csv_from_url(url)
    if len(df) == 0:
        return None

    # df is sorted by rowid; take the last row as the "most recent ended" we got back
    old_resource_hash = df["resource"].iloc[-1]

    transformed_url = (
        f"https://files.planning.data.gov.uk/{collection_name}-collection/transformed/{dataset}/{old_resource_hash}.csv"
    )
    transformed_df = read_csv_from_url(transformed_url)
    return transformed_df

def get_field_value_map(df: pd.DataFrame, entity_number) -> dict:
    """Return a dict of field-value pairs for a given entity from transformed file."""
    sub_df = df[
        (df["entity"] == entity_number)
        & (~df["field"].isin(["reference", "entry-date"]))
    ]
    return dict(zip(sub_df["field"], sub_df["value"]))

def safe_copy(src: Path, dst: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(f"Expected file to copy does not exist: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(src, dst)

# ----------------------------
# MAIN PROCESSING FUNCTION
# ----------------------------
def process_csv(scope: str) -> tuple[list, list, dict]:
    """
    Automatically process and assign unknown entities for given scope.

    Returns:
      failed_downloads: list of tuples
      failed_assignments: list of tuples
      report: dict suitable for JSON output
    """
    failed_downloads = []
    failed_assignments = []
    successful_resources = []
    processed_rows = 0

    RESOURCES_DIR.mkdir(exist_ok=True)
    REPORTS_DIR.mkdir(exist_ok=True)

    run_report = {
        "run_started_utc": datetime.now(timezone.utc).isoformat(),
        "scope": scope,
        "rows_processed": 0,
        "rows_skipped": 0,
        "success_count": 0,
        "download_failures": [],
        "assignment_failures": [],
        "notes": [],
    }

    with open("issue_summary.csv", "r", newline="") as file:
        csv_reader = csv.DictReader(file)

        for row_number, row in enumerate(csv_reader, start=1):
            issue_type = (row.get("issue_type") or "").lower()
            row_scope = (row.get("scope") or "").lower()
            dataset_name = (row.get("dataset") or "").lower()

            if issue_type != "unknown entity" or row_scope != scope or dataset_name == "title-boundary":
                run_report["rows_skipped"] += 1
                continue

            processed_rows += 1

            collection_name = row["collection"]
            resource = row["resource"]
            endpoint = row["endpoint"]
            dataset = row["pipeline"]          # this is the pipeline/dataset used by assign tool
            organisation_name = row["organisation"]

            logging.info(
                "ROW %d | collection=%s | dataset=%s | resource=%s",
                row_number, collection_name, dataset, resource
            )

            download_link = (
                f"https://files.planning.data.gov.uk/{collection_name}-collection/collection/resource/{resource}"
            )
            resource_path = RESOURCES_DIR / resource

            # A) Download resource
            try:
                resp = http_get(download_link)
                resource_path.write_bytes(resp.content)
                logging.info("Downloaded resource: %s (%d bytes)", resource, len(resp.content))
            except Exception as e:
                msg = str(e)
                logging.error("Failed to download resource %s: %s", resource, msg)
                failed_downloads.append((row_number, resource, msg))
                run_report["download_failures"].append(
                    {"row_number": row_number, "resource": resource, "error": msg}
                )
                continue

            # B) Assign entities
            collection_path = Path(f"collection/{collection_name}")
            input_transformed_path = CACHE_DIR / "assign_entities" / "transformed" / f"{resource}.csv"
            lookup_src = CACHE_DIR / "assign_entities" / collection_name / "pipeline" / "lookup.csv"
            lookup_dst = Path("pipeline") / collection_name / "lookup.csv"

            try:
                success = check_and_assign_entities(
                    [resource_path],
                    [endpoint],
                    collection_name,
                    dataset,
                    [organisation_name],
                    collection_path,
                    CACHE_DIR / "organisation.csv",
                    Path("specification"),
                    Path(f"pipeline/{collection_name}"),
                    input_transformed_path,
                )

                if not success:
                    # Treat as failure in CI: it means the operation didn't complete.
                    raise RuntimeError("check_and_assign_entities returned False (cancelled or incomplete)")

                # C) Optional duplicate detection vs previous ended resource
                try:
                    old_resource_df = get_old_resource_df(endpoint, collection_name, dataset)
                except Exception as e:
                    old_resource_df = None
                    run_report["notes"].append(
                        f"Row {row_number} resource {resource}: could not load old resource for duplicate check: {e}"
                    )

                if old_resource_df is not None:
                    current_resource_df = read_csv_from_path(input_transformed_path)
                    current_entities = set(current_resource_df["entity"])
                    old_entities = set(old_resource_df["entity"])
                    new_entities = list(current_entities - old_entities)

                    if new_entities:
                        # Build signature maps
                        field_map_to_old_entity = {}
                        for old_entity in old_resource_df["entity"].unique():
                            field_map = tuple(sorted(get_field_value_map(old_resource_df, old_entity).items()))
                            field_map_to_old_entity[field_map] = old_entity

                        duplicate_entity = {}
                        # Only compare new entities
                        current_new_df = current_resource_df[current_resource_df["entity"].isin(new_entities)]
                        for entity in new_entities:
                            current_fields = tuple(sorted(get_field_value_map(current_new_df, entity).items()))
                            if current_fields in field_map_to_old_entity:
                                duplicate_entity[entity] = field_map_to_old_entity[current_fields]

                        if duplicate_entity:
                            logging.warning("Potential duplicate entities found: %s", duplicate_entity)
                            run_report["notes"].append(
                                f"Row {row_number} resource {resource}: duplicates={duplicate_entity}"
                            )

                # D) Copy lookup.csv (validate existence)
                safe_copy(lookup_src, lookup_dst)
                logging.info("Entities assigned; copied lookup.csv to %s", lookup_dst)

                successful_resources.append(resource_path)
                run_report["success_count"] += 1

            except Exception as e:
                msg = str(e)
                logging.exception("Failed to assign entities for resource %s", resource)
                failed_assignments.append((row_number, resource, "AssignmentError", msg))
                run_report["assignment_failures"].append(
                    {"row_number": row_number, "resource": resource, "error": msg}
                )

            # Keep resource for cleanup even on failure? For CI debugging, keep on failure.
            # We only delete on success.

    run_report["rows_processed"] = processed_rows

    # Cleanup successful resource files
    for resource_path in successful_resources:
        try:
            if resource_path.exists():
                resource_path.unlink()
            gfs_path = resource_path.with_suffix(".gfs")
            if gfs_path.exists():
                gfs_path.unlink()
        except OSError as e:
            logging.warning("Failed to remove %s or .gfs: %s", resource_path, e)

    try:
        if RESOURCES_DIR.exists() and not any(RESOURCES_DIR.iterdir()):
            RESOURCES_DIR.rmdir()
    except OSError as e:
        logging.warning("Failed to remove resources directory: %s", e)

    # Write JSON report
    run_report["run_finished_utc"] = datetime.now(timezone.utc).isoformat()
    report_path = REPORTS_DIR / "unknown-entity-run.json"
    report_path.write_text(json.dumps(run_report, indent=2), encoding="utf-8")
    logging.info("Wrote report: %s", report_path)

    # Summary
    logging.info("--- Summary ---")
    logging.info("Rows processed: %d | skipped: %d | success: %d",
                 run_report["rows_processed"], run_report["rows_skipped"], run_report["success_count"])
    logging.info("Failed downloads: %d | Failed assignments: %d",
                 len(failed_downloads), len(failed_assignments))

    return failed_downloads, failed_assignments, run_report

# ----------------------------
# MAIN EXECUTION
# ----------------------------
if __name__ == "__main__":
    # Ensure provision-rule.csv exists
    download_file(
        "https://raw.githubusercontent.com/digital-land/specification/refs/heads/main/content/provision-rule.csv",
        "specification/provision-rule.csv",
    )

    # Download issue summary
    endpoint_issue_summary_path = (
        "https://datasette.planning.data.gov.uk/performance/"
        "endpoint_dataset_issue_type_summary.csv?_sort=rowid&issue_type__exact=unknown+entity&_size=max"
    )
    df = read_csv_from_url(endpoint_issue_summary_path)

    provision_rule_df = pd.read_csv("specification/provision-rule.csv")

    # Assign scope per dataset
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

    def determine_scope(dataset: str) -> str:
        if dataset in scope_dict["odp"]:
            return "odp"
        if dataset in scope_dict["mandated"]:
            return "mandated"
        return "single-source"

    df["scope"] = df["dataset"].apply(determine_scope)
    df.to_csv("issue_summary.csv", index=False)
    logging.info("issue_summary.csv written")

    failed_downloads, failed_assignments, report = process_csv(SCOPE)

    # Fail the GitHub Action if anything failed
    if failed_downloads or failed_assignments:
        logging.error(
            "Run completed with failures: downloads=%d, assignments=%d",
            len(failed_downloads), len(failed_assignments),
        )
        sys.exit(1)

    logging.info("All operations completed successfully.")
    sys.exit(0)

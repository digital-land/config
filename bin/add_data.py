#!/usr/bin/env python3
import csv
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import click
import pandas as pd


API_BASE_URL_BY_ENVIRONMENT = {
    "development": "http://development-pub-async-api-lb-69142969.eu-west-2.elb.amazonaws.com",
    "staging": "http://staging-pub-async-api-lb-12493311.eu-west-2.elb.amazonaws.com",
    "production": "http://production-pub-async-api-lb-636110663.eu-west-2.elb.amazonaws.com",
}
DEFAULT_ENVIRONMENT = "staging"


def resolve_api_base_url(environment: str) -> str:
    env = str(environment).strip().lower() or DEFAULT_ENVIRONMENT
    if env in API_BASE_URL_BY_ENVIRONMENT:
        return API_BASE_URL_BY_ENVIRONMENT[env]
    fail(f"Unsupported environment: {environment}")


def fail(message: str) -> None:
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def run_command(cmd: list[str], capture_output: bool = False, check: bool = True) -> str:
    try:
        result = subprocess.run(
            cmd,
            text=True,
            capture_output=capture_output,
            check=False,
        )
    except FileNotFoundError:
        fail(
            f"Required command not found on PATH: {cmd[0]}. "
            "Install it before running this script."
        )
    if check and result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        detail = stderr or stdout or "command failed"
        fail(f"{' '.join(cmd)} -> {detail}")
    return (result.stdout or "").strip() if capture_output else ""


def fetch_request(api_base_url: str, request_id: str) -> dict:
    request_url = f"{api_base_url.rstrip('/')}/requests/{request_id}"
    print(f"Fetching request data from {request_url}")

    try:
        with urlopen(request_url) as response:  # nosec B310
            payload = response.read().decode("utf-8")
    except HTTPError as exc:
        fail(f"Failed to fetch request data for {request_id}: HTTP {exc.code}")
    except URLError as exc:
        fail(f"Failed to fetch request data for {request_id}: {exc.reason}")

    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        fail(f"Failed to parse async API response: {exc}")


def ensure_dir_exists(path: Path) -> None:
    if not path.is_dir():
        fail(f"{path} does not exist")


def ensure_file_ends_with_newline(path: Path) -> None:
    if not path.exists() or path.stat().st_size == 0:
        return

    with path.open("rb") as f:
        f.seek(-1, os.SEEK_END)
        last = f.read(1)

    if last != b"\n":
        with path.open("ab") as f:
            f.write(b"\r\n")


def append_csv_rows(path: Path, rows: list[list[object]]) -> int:
    ensure_file_ends_with_newline(path)
    count = 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\r\n")
        for row in rows:
            if row:
                writer.writerow(["" if value is None else str(value) for value in row])
                count += 1
    return count


def normalize_retire_endpoints(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def retire_endpoints_in_csv(collection: str, retire_endpoints: list[str]) -> None:
    if not retire_endpoints:
        return

    today = datetime.now().strftime("%Y-%m-%d")
    endpoint_file = Path("collection") / collection / "endpoint.csv"
    source_file = Path("collection") / collection / "source.csv"

    def update_file(path: Path, file_label: str) -> None:
        if not path.exists():
            print(f"{file_label} not found, skipping retire endpoints")
            return

        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        if "endpoint" not in frame.columns or "end-date" not in frame.columns:
            print(f"{file_label} missing required columns, skipping retire endpoints")
            return

        mask = frame["endpoint"].isin(retire_endpoints)
        updated_count = int(mask.sum())
        if updated_count == 0:
            print(f"No matching endpoints found in {file_label}")
            return

        frame.loc[mask, "end-date"] = today
        frame.to_csv(path, index=False, lineterminator="\r\n")
        print(f"Retired {updated_count} row(s) in {file_label} with end-date {today}")

    update_file(endpoint_file, "endpoint.csv")
    update_file(source_file, "source.csv")


def as_bool(value: object) -> bool:
    return str(value).lower() == "true"


def build_test_branch_name(branch_name: str, collection: str) -> str:
    if branch_name:
        return f"test/{branch_name}"
    return f"test/add-data-async/{collection}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


def resolve_branch(branch_param: Optional[str], collection: str) -> tuple[str, str, str]:
    if branch_param:
        pr_number = run_command(
            [
                "gh",
                "pr",
                "list",
                "--head",
                branch_param,
                "--state",
                "open",
                "--json",
                "number",
                "--jq",
                ".[0].number // empty",
            ],
            capture_output=True,
        )

        if pr_number:
            print(f"Found open PR #{pr_number} for branch {branch_param}, checking out")
            run_command(["git", "fetch", "origin", branch_param])
            run_command(["git", "checkout", branch_param])
            return branch_param, pr_number, "append"

        branch_exists = subprocess.run(
            ["git", "ls-remote", "--exit-code", "--heads", "origin", branch_param],
            text=True,
            capture_output=True,
            check=False,
        ).returncode == 0

        if branch_exists:
            print(f"Branch {branch_param} exists but has no open PR, checking out")
            run_command(["git", "fetch", "origin", branch_param])
            run_command(["git", "checkout", branch_param])
        else:
            print(f"Branch {branch_param} does not exist, will create it")

        return branch_param, "", "create"

    new_branch = f"add-data-async/{collection}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    print(f"No branch specified, will create new branch: {new_branch}")
    return new_branch, "", "new"


def checkout_branch_for_create_mode(branch_name: str) -> None:
    current_branch = run_command(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
    )
    if current_branch == branch_name:
        return

    local_exists = subprocess.run(
        ["git", "show-ref", "--verify", f"refs/heads/{branch_name}"],
        text=True,
        capture_output=True,
        check=False,
    ).returncode == 0

    if local_exists:
        run_command(["git", "checkout", branch_name])
    else:
        run_command(["git", "checkout", "-b", branch_name])


def append_endpoint(response: dict, collection: str) -> None:
    endpoint_file = Path("collection") / collection / "endpoint.csv"
    endpoint_summary = response.get("response", {}).get("data", {}).get("endpoint-summary", {})

    if as_bool(endpoint_summary.get("endpoint_url_in_endpoint_csv")):
        print("Endpoint already exists in endpoint.csv, skipping")
        return

    new_entry = endpoint_summary.get("new_endpoint_entry")
    if not new_entry:
        print("No new endpoint entry found, skipping")
        return

    row = [
        new_entry.get("endpoint"),
        new_entry.get("endpoint-url"),
        new_entry.get("parameters"),
        new_entry.get("plugin"),
        new_entry.get("entry-date"),
        new_entry.get("start-date"),
        new_entry.get("end-date"),
    ]
    count = append_csv_rows(endpoint_file, [row])

    print(f"Added {count} row(s) to endpoint.csv")


def append_source(response: dict, collection: str) -> None:
    source_file = Path("collection") / collection / "source.csv"
    source_summary = response.get("response", {}).get("data", {}).get("source-summary", {})

    if as_bool(source_summary.get("documentation_url_in_source_csv")):
        print("Source already exists in source.csv, skipping")
        return

    new_entry = source_summary.get("new_source_entry")
    if not new_entry:
        print("No new source entry found, skipping")
        return

    row = [
        new_entry.get("source"),
        new_entry.get("attribution"),
        new_entry.get("collection"),
        new_entry.get("documentation-url"),
        new_entry.get("endpoint"),
        new_entry.get("licence"),
        new_entry.get("organisation"),
        new_entry.get("pipelines"),
        new_entry.get("entry-date"),
        new_entry.get("start-date"),
        new_entry.get("end-date"),
    ]
    count = append_csv_rows(source_file, [row])

    print(f"Added {count} row(s) to source.csv")


def append_lookup(response: dict, collection: str) -> None:
    lookup_file = Path("pipeline") / collection / "lookup.csv"
    pipeline_summary = response.get("response", {}).get("data", {}).get("pipeline-summary", {})
    new_entities = pipeline_summary.get("new-entities") or []

    if not new_entities:
        print("No new entities found, skipping lookup.csv")
        return

    rows = []
    for entity in new_entities:
        rows.append(
            [
                entity.get("prefix"),
                entity.get("resource"),
                entity.get("endpoint"),
                entity.get("entry-number"),
                entity.get("organisation"),
                entity.get("reference"),
                entity.get("entity"),
                entity.get("entry-date"),
                entity.get("start-date"),
                entity.get("end-date"),
            ]
        )

    count = append_csv_rows(lookup_file, rows)

    print(f"Added {count} row(s) to lookup.csv")


def append_column(response: dict, collection: str) -> None:
    column_file = Path("pipeline") / collection / "column.csv"
    params = response.get("params", {})
    mapping = params.get("column_mapping")

    if not isinstance(mapping, dict) or not mapping:
        print("No column mapping found, skipping column.csv")
        return

    endpoint_entry = (
        response.get("response", {})
        .get("data", {})
        .get("endpoint-summary", {})
        .get("new_endpoint_entry", {})
    )

    dataset = params.get("dataset", "")
    endpoint = endpoint_entry.get("endpoint", "")
    entry_date = endpoint_entry.get("entry-date", "")
    start_date = endpoint_entry.get("start-date", "")

    rows = []
    for key, value in mapping.items():
        rows.append([dataset, endpoint, "", key, value, start_date, "", entry_date])

    count = append_csv_rows(column_file, rows)

    print(f"Added {count} row(s) to column.csv")


def append_entity_organisation(response: dict, collection: str) -> None:
    entity_org_file = Path("pipeline") / collection / "entity-organisation.csv"
    params = response.get("params", {})
    if not as_bool(params.get("authoritative", False)):
        print("authoritative is not true, skipping entity-organisation.csv")
        return

    entries = (
        response.get("response", {})
        .get("data", {})
        .get("pipeline-summary", {})
        .get("entity-organisation")
        or []
    )

    if not entries:
        print("No entity-organisation entries found, skipping")
        return

    rows = []
    for entry in entries:
        rows.append(
            [
                entry.get("dataset"),
                entry.get("entity-minimum"),
                entry.get("entity-maximum"),
                entry.get("organisation"),
            ]
        )

    count = append_csv_rows(entity_org_file, rows)

    print(f"Added {count} row(s) to entity-organisation.csv")


def get_commit_label(response: dict, triggered_by: str) -> str:
    params = response.get("params", {})
    dataset = str(params.get("dataset", "")).strip()
    organisation = str(params.get("organisation", "")).strip()
    suffix = str(triggered_by).strip()

    parts = ["add"]
    if dataset:
        parts.append(dataset)
    if organisation:
        parts.append(organisation)
    if suffix:
        parts.append(suffix)
    return " ".join(parts)


def write_summary(collection: str, request_id: str) -> None:
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    summary = (
        "### Workflow Summary\n\n"
        f"{collection} updated via async request {request_id}\n"
    )

    with open(summary_path, "a", encoding="utf-8") as f:
        f.write(summary)


def run_add_data_async(
    request_id: str,
    branch: str = "",
    triggered_by: str = "",
    environment: str = DEFAULT_ENVIRONMENT,
    test_mode: bool = False,
    retire_endpoints: Optional[list[str]] = None,
) -> None:
    if not request_id.strip():
        fail("request_id is required")

    api_base_url = resolve_api_base_url(environment)
    response = fetch_request(api_base_url, request_id)

    status = response.get("status")
    if status != "COMPLETE":
        fail(f"Request status is '{status}', expected 'COMPLETE'")

    error_value = response.get("response", {}).get("error")
    if error_value:
        fail(f"Request has error: {error_value}")

    collection = response.get("params", {}).get("collection")
    if not collection:
        fail("collection not found in request params")

    collection_dir = Path("collection") / collection
    pipeline_dir = Path("pipeline") / collection
    ensure_dir_exists(collection_dir)
    ensure_dir_exists(pipeline_dir)

    retire_endpoints = normalize_retire_endpoints(retire_endpoints or [])

    if test_mode:
        print("Test mode enabled; this creates a draft PR that must not be merged.")
        branch_name = build_test_branch_name(branch.strip(), collection)
        mode = "test"
        pr_number = ""
    else:
        branch_name, pr_number, mode = resolve_branch(branch.strip(), collection)

    retire_endpoints_in_csv(collection, retire_endpoints)
    append_endpoint(response, collection)
    append_source(response, collection)
    append_lookup(response, collection)
    append_column(response, collection)
    append_entity_organisation(response, collection)

    commit_label = get_commit_label(response, triggered_by)

    run_command(["git", "config", "user.name", "github-actions-add-data-bot"])
    run_command(["git", "config", "user.email", "matthew.poole@communities.gov.uk"])

    run_command(["git", "add", f"collection/{collection}/"])
    run_command(["git", "add", f"pipeline/{collection}/"])

    staged_changes = subprocess.run(
        ["git", "diff", "--staged", "--quiet"],
        check=False,
    ).returncode != 0

    if not staged_changes:
        print("No changes to commit")
        write_summary(collection, request_id)
        return

    if mode == "append":
        run_command(["git", "commit", "-m", commit_label])
        run_command(["git", "push", "origin", branch_name])

        current_body = run_command(
            ["gh", "pr", "view", pr_number, "--json", "body", "--jq", ".body"],
            capture_output=True,
        )
        new_body = f"{current_body}\n{commit_label}" if current_body else commit_label
        run_command(["gh", "pr", "edit", pr_number, "--title", "Manage Service Update", "--body", new_body])
        print(f"Appended to PR #{pr_number} on branch {branch_name}")
    elif mode == "test":
        test_title = f"TEST ONLY: {commit_label}"
        test_body = (
            "This is a draft test PR generated by add_data.py.\n\n"
            "Do not merge this PR.\n\n"
            f"Request: {request_id}\n"
            f"Branch: {branch_name}\n"
            f"Commit: {commit_label}\n"
        )
        checkout_branch_for_create_mode(branch_name)
        run_command(["git", "commit", "-m", commit_label])
        run_command(["git", "push", "origin", branch_name])
        run_command(
            [
                "gh",
                "pr",
                "create",
                "--draft",
                "--title",
                test_title,
                "--body",
                test_body,
                "--base",
                "main",
                "--head",
                branch_name,
            ]
        )
        print(f"Created draft test PR on branch {branch_name}")
    else:
        checkout_branch_for_create_mode(branch_name)
        run_command(["git", "commit", "-m", commit_label])
        run_command(["git", "push", "origin", branch_name])
        run_command(
            [
                "gh",
                "pr",
                "create",
                "--title",
                commit_label,
                "--body",
                commit_label,
                "--base",
                "main",
                "--head",
                branch_name,
            ],
        )
        print(f"Created PR on branch {branch_name}")

    write_summary(collection, request_id)


@click.command(help="Append async API data to collection/pipeline CSV files and manage PR flow")
@click.option("--request-id", required=True, type=click.STRING, help="Async API request id")
@click.option("--branch", default="", type=click.STRING, help="Optional branch supplied by dispatch payload")
@click.option("--triggered-by", default="", type=click.STRING, help="Identifier for the actor/system that triggered this run")
@click.option(
    "--environment",
    default=lambda: os.getenv("ASYNC_API_ENVIRONMENT", DEFAULT_ENVIRONMENT),
    show_default=f"env ASYNC_API_ENVIRONMENT or {DEFAULT_ENVIRONMENT}",
    type=click.Choice(["development", "staging", "production"], case_sensitive=False),
    help="Async API environment",
)
@click.option(
    "--retire-endpoints",
    multiple=True,
    type=click.STRING,
    help="Endpoint strings to retire; pass multiple times or as comma-separated values",
)
@click.option("--test/--no-test", "test_mode", default=False, help="Create a draft test PR that should not be merged")
def main(
    request_id: str,
    branch: str,
    triggered_by: str,
    environment: str,
    retire_endpoints: tuple[str, ...],
    test_mode: bool,
) -> None:
    retire_endpoint_values: list[str] = []
    for value in retire_endpoints:
        retire_endpoint_values.extend(normalize_retire_endpoints(value))

    run_add_data_async(
        request_id=request_id,
        branch=branch,
        triggered_by=triggered_by,
        environment=environment,
        test_mode=test_mode,
        retire_endpoints=retire_endpoint_values,
    )


if __name__ == "__main__":
    main()

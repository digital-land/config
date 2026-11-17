#!/usr/bin/env python3
"""Run Manage's Check and Add data steps on this machine, then write the result into this checkout.

For use while Manage and async are unavailable. Run it with `make manage-check`, which sets up the
Python 3.13 venv and puts async-request-backend's request-processor code on PYTHONPATH.

What comes from where:
- fetching the endpoint: DLP's Collector, trying plugins in the same order as async's
  tasks._fetch_resource (tasks.py can't be imported here: it needs Celery and a database).
- Check and Add data: async's own workflow.run_workflow and workflow.add_data_workflow, unchanged.
- the must-fix rule: copied from config-manager application/blueprints/datamanager/controllers/check.py.
- writing rows: bin/add_data.py, the same functions the add-data-async-script workflow uses for Manage.

Config (pipeline/ and collection/ CSVs) is read from this checkout rather than GitHub, so rows added
by an earlier run that haven't been pushed yet count when new entity numbers are allocated.
"""
import csv
import hashlib
import json
import shutil
import subprocess
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urljoin, urlparse

import click
import requests
from digital_land.collect import Collector, FetchStatus

from application.configurations.config import CONFIG_URL, Directories
from application.core import workflow

import add_data

RUNS_DIR = Path("var/manage/runs")
CONFIG_BRANCH = "config-manager-update"
BLOCKING_SEVERITY = "critical"
NON_BLOCKING_SEVERITY = "error"


def check_organisation(organisation, dataset, organisation_csv):
    """Manage's organisation rules: the code must exist and, if the specification lists
    who provides this dataset (specification/provision.csv), be one of them."""
    with open(organisation_csv, newline="", encoding="utf-8") as f:
        known = {row["organisation"] for row in csv.DictReader(f)}
    if organisation not in known:
        raise click.ClickException(
            f"{organisation} is not an organisation code in {organisation_csv}."
        )
    with open("specification/provision.csv", newline="", encoding="utf-8") as f:
        providers = {
            row["organisation"]
            for row in csv.DictReader(f)
            if row["dataset"] == dataset
        }
    if providers and organisation not in providers:
        raise click.ClickException(
            f"{organisation} is not a provider of {dataset} in "
            "specification/provision.csv, so Manage would not accept it."
        )


def git(*args):
    try:
        result = subprocess.run(
            ["git", *args], capture_output=True, text=True, check=True
        )
    except subprocess.CalledProcessError as e:
        raise click.ClickException(f"git {' '.join(args)} failed: {e.stderr.strip()}")
    return result.stdout.strip()


def remote_branch_commit():
    git("fetch", "--quiet", "origin", CONFIG_BRANCH)
    return git("rev-parse", f"origin/{CONFIG_BRANCH}")


def check_branch_is_up_to_date():
    """Like Manage, refuse to start from a stale config-manager-update.

    Returns the remote commit this run started from, or None when running on another
    branch (for trying the tool out), where the clash checks don't apply.
    """
    branch = git("branch", "--show-current")
    if branch != CONFIG_BRANCH:
        if not click.confirm(
            f"You are on {branch}, not {CONFIG_BRANCH}. Continue anyway?", default=False
        ):
            raise click.Abort()
        return None
    started_from = remote_branch_commit()
    behind = int(git("rev-list", "--count", f"HEAD..origin/{CONFIG_BRANCH}"))
    if behind:
        raise click.ClickException(
            f"Your {CONFIG_BRANCH} is {behind} commit(s) behind GitHub. "
            "Run git pull, then run this again."
        )
    return started_from


def check_nobody_pushed(started_from):
    """Like Manage, refuse to write if config-manager-update moved during the run:
    someone else's rows could clash with the entity numbers allocated here."""
    if started_from and remote_branch_commit() != started_from:
        raise click.ClickException(
            f"Someone pushed to {CONFIG_BRANCH} while this was running. Nothing written. "
            "Run git pull, then run this again."
        )


def read_config_from_checkout():
    """Make async read pipeline/ and collection/ CSVs from this checkout instead of GitHub."""
    download_file = workflow.download_file

    def from_checkout(url, destination):
        if not url.startswith(CONFIG_URL):
            return download_file(url, destination)
        local_path = Path(url[len(CONFIG_URL) :])
        if not local_path.is_file():
            # async treats a 404 as "this collection doesn't have that file"
            raise HTTPError(
                url, 404, f"{local_path} is not in this checkout", None, None
            )
        shutil.copyfile(local_path, destination)

    workflow.download_file = from_checkout


def directories_for(run_dir, organisation_csv):
    """Point every async working directory into this run's folder."""
    directories = Directories()
    for name in [
        "COLLECTION_DIR",
        "ISSUE_DIR",
        "COLUMN_FIELD_DIR",
        "TRANSFORMED_DIR",
        "CONVERTED_DIR",
        "PIPELINE_DIR",
        "DATASET_RESOURCE_DIR",
        "CACHE_DIR",
    ]:
        folder = name.removesuffix("_DIR").lower().replace("_", "-")
        setattr(directories, name, f"{run_dir}/{folder}/")
    directories.S3_SPEC = ""
    directories.SPECIFICATION_DIR = "specification/"
    Path(directories.CACHE_DIR).mkdir(parents=True, exist_ok=True)
    shutil.copyfile(organisation_csv, Path(directories.CACHE_DIR) / "organisation.csv")
    return directories


def fetch(url, run_dir):
    """Fetch the endpoint: plain first, then the arcgis and wfs plugins, as async does."""
    resource_dir = run_dir / "fetch"
    resource_dir.mkdir(parents=True, exist_ok=True)
    collector = Collector(resource_dir=resource_dir, log_dir=run_dir / "fetch-log")
    for plugin in [None, "arcgis", "wfs"]:
        status, log = collector.fetch(url, plugin=plugin, refill_todays_logs=True)
        if status == FetchStatus.OK:
            resource_path = next(resource_dir.iterdir())
            return resource_path, plugin
        if log.get("exception") or str(log.get("status", "")).startswith("4"):
            break
    raise click.ClickException(
        f"Could not fetch {url}: {log.get('exception') or log.get('status')}"
    )


def stage_resource(resource_path, directories, request_id):
    """Put the fetched file where async expects a request's resource."""
    target = Path(directories.COLLECTION_DIR) / "resource" / request_id
    target.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(resource_path, target / resource_path.name)


def task_details(task):
    try:
        details = json.loads(task.get("details") or "")
    except (json.JSONDecodeError, TypeError):
        return None
    return details if isinstance(details, dict) else None


def must_and_should_fix(task_log):
    """Manage's rule for whether data can be added.

    Copied from config-manager datamanager/controllers/check.py (_missing_column_tasks and
    _issue_tasks); keep the two in step. Must fix: missing mandatory columns and critical
    issues. Should fix: error issues. Internal issues are ignored.
    """
    must_fix = []
    for task in task_log:
        if task.get("task-source") == "column-field":
            field = (task_details(task) or {}).get("field")
            if task.get("summary") or field:
                must_fix.append(task.get("summary") or f"{field} column is missing")

    issues = {}
    for task in task_log:
        if (
            task.get("task-source") != "issue"
            or task.get("responsibility") == "internal"
        ):
            continue
        details = task_details(task)
        if not details or not details.get("issue_type") or not details.get("field"):
            continue
        severity = task.get("severity")
        if severity not in (BLOCKING_SEVERITY, NON_BLOCKING_SEVERITY):
            continue
        issue = issues.setdefault(
            (details["issue_type"], details["field"]),
            {"severity": severity, "count": 0, "summary": task.get("summary")},
        )
        issue["count"] += details.get("count") or 1
        if severity == BLOCKING_SEVERITY:
            issue["severity"] = severity

    should_fix = []
    for (issue_type, field), issue in issues.items():
        plural = "s" if issue["count"] > 1 else ""
        summary = issue["summary"] or (
            f"{issue['count']} issue{plural} of type {issue_type} in {field}"
        )
        if issue["severity"] == BLOCKING_SEVERITY:
            must_fix.append(summary)
        else:
            should_fix.append(summary)
    return must_fix, should_fix


def save(run_dir, name, response):
    (run_dir / name).write_text(json.dumps(response, indent=1, default=str))


def print_add_summary(response, collection):
    pipeline = response["pipeline-summary"]
    endpoint = response["endpoint-summary"]
    source = response["source-summary"]
    new_entities = pipeline.get("new-entities") or []
    duplicates = pipeline.get("duplicate-candidates") or []

    click.echo(f"New entities: {len(new_entities)}")
    for entity in new_entities[:10]:
        click.echo(f"  {entity.get('entity')}  {entity.get('reference')}")
    if len(new_entities) > 10:
        click.echo(f"  ... and {len(new_entities) - 10} more")
    click.echo(f"Existing entities: {len(pipeline.get('existing-entities') or [])}")
    if duplicates:
        match_types = sorted({d.get("match_type") for d in duplicates})
        click.echo(
            f"Possible duplicates of existing entities: {len(duplicates)} "
            f"({', '.join(match_types)}). Manage asks a person to choose which to redirect; "
            "this tool only writes the automatic ones below."
        )
    click.echo(
        f"Redirects to add to old-entity.csv: {len(pipeline.get('old-entity') or [])}"
    )
    if add_data.as_bool(endpoint.get("endpoint_url_in_endpoint_csv")):
        click.echo(f"Endpoint: already in collection/{collection}/endpoint.csv")
    else:
        click.echo("Endpoint: new row for endpoint.csv")
    if add_data.as_bool(source.get("documentation_url_in_source_csv")):
        click.echo(
            f"Source: documentation URL already in collection/{collection}/source.csv"
        )
    else:
        click.echo("Source: new row for source.csv")


class _HrefCollector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.hrefs.append(value.strip())


def _hrefs_match(href, endpoint_url, documentation_url):
    """Return True if href resolves to endpoint_url under any matching strategy."""
    endpoint_parsed = urlparse(endpoint_url)
    endpoint_path = endpoint_parsed.path

    # Exact full URL match
    if href == endpoint_url:
        return True

    # Resolve relative href against documentation URL and compare
    try:
        resolved = urljoin(documentation_url, href)
        if resolved == endpoint_url:
            return True
    except Exception:
        pass

    # Path-only match (e.g. /downloads/file/3149/... == endpoint path)
    if href == endpoint_path:
        return True

    # Compare the path portion of an absolute href
    try:
        href_parsed = urlparse(href)
        if href_parsed.path and href_parsed.path == endpoint_path:
            return True
    except Exception:
        pass

    return False


def check_endpoint_on_documentation_page(documentation_url, endpoint_url):
    """Whether the documentation page links to the endpoint, as Manage's results page
    shows. Information only: Manage doesn't block on it either.

    Copied from config-manager datamanager/services/doc_crawler.py
    (check_endpoint_in_doc and _hrefs_match); keep the two in step.
    """
    try:
        response = requests.get(
            documentation_url,
            timeout=10,
            allow_redirects=True,
            headers={"User-Agent": "digital-land-config-manager/1.0"},
        )
        response.raise_for_status()
        collector = _HrefCollector()
        collector.feed(response.text)
    except requests.exceptions.HTTPError as e:
        click.echo(
            f"Documentation page: could not be read (HTTP {e.response.status_code})."
        )
        return
    except Exception as e:
        click.echo(f"Documentation page: could not be read ({type(e).__name__}).")
        return
    for href in collector.hrefs:
        if _hrefs_match(href, endpoint_url, documentation_url):
            click.echo(f"Documentation page: links to the endpoint ({href}).")
            return
    click.secho(
        "Documentation page: does NOT link to the endpoint URL. The documentation "
        "URL may be wrong; it doesn't stop the data being added.",
        fg="yellow",
    )


def warn_if_replacement_creates_entities(response, retire):
    """Retiring an endpoint means this replaces it, which normally creates no new
    entities. If it does, the organisation may have changed its references."""
    new_entities = response["pipeline-summary"].get("new-entities") or []
    if retire and new_entities:
        click.secho(
            f"Warning: this replaces {len(retire)} endpoint(s) but creates "
            f"{len(new_entities)} new entities. A replacement normally creates none. "
            "Check the organisation hasn't changed its references before writing.",
            fg="yellow",
            bold=True,
        )


def choose_endpoints_to_retire(response, collection, endpoint_url):
    candidates = (
        response["source-summary"].get("existing_endpoint_for_organisation_dataset")
        or []
    )
    # the endpoint being added, whether or not it is already in endpoint.csv
    new_endpoint = hashlib.sha256(endpoint_url.encode("utf-8")).hexdigest()
    urls = {}
    with open(
        f"collection/{collection}/endpoint.csv", newline="", encoding="utf-8"
    ) as f:
        for row in csv.DictReader(f):
            urls[row["endpoint"]] = (
                row["endpoint-url"],
                row["start-date"],
                row["end-date"],
            )
    retire = []
    for endpoint in candidates:
        if endpoint == new_endpoint:
            continue
        url, start_date, end_date = urls.get(
            endpoint, ("(not in endpoint.csv)", "", "")
        )
        if end_date:
            # async lists retired endpoints too; Manage leaves them alone, so do we
            click.echo(f"Already retired on {end_date}: {url}")
            continue
        if click.confirm(
            f"Retire existing endpoint {url} (start {start_date})?", default=False
        ):
            retire.append(endpoint)
    return retire


def ask_if_authoritative(response, organisation):
    """Manage's "is this organisation the authoritative source?" question.

    Only asked when there are new entities. A yes writes an entity-organisation.csv
    range for them, which is what makes the nightly pipeline mark their data
    "authoritative" rather than "some".
    """
    ranges = response["pipeline-summary"].get("entity-organisation") or []
    to_write = [r for r in ranges if r.get("entity-minimum") is not None]
    if not to_write:
        if ranges:
            click.echo("New entities are already in an entity-organisation.csv range.")
        return False
    for r in to_write:
        click.echo(
            f"New entities {r['entity-minimum']}-{r['entity-maximum']} have no "
            "entity-organisation.csv range yet."
        )
    return click.confirm(
        f"Is {organisation} the authoritative source for this data?", default=None
    )


def write_rows(response, collection, dataset, organisation, retire, authoritative):
    """Write rows in the same order as bin/add_data.py run_add_data_async."""
    result = {
        "params": {
            "collection": collection,
            "dataset": dataset,
            "organisation": organisation,
            "authoritative": authoritative,
        },
        "response": {"data": response},
    }
    add_data.retire_endpoints_in_csv(collection, retire)
    add_data.append_old_entity(result, collection)
    add_data.append_endpoint(result, collection)
    add_data.append_source(result, collection)
    add_data.append_lookup(result, collection)
    add_data.append_column(result, collection)
    add_data.append_entity_organisation(result, collection)


@click.command()
@click.option("--collection", required=True)
@click.option("--dataset", required=True)
@click.option("--organisation", required=True, help="e.g. local-authority:WRL")
@click.option("--endpoint-url", required=True)
@click.option("--documentation-url", required=True)
@click.option("--licence", default="ogl3", show_default=True)
@click.option(
    "--start-date", default=None, help="YYYY-MM-DD; defaults to today, as in Manage"
)
@click.option("--organisation-csv", required=True, type=click.Path(exists=True))
def main(
    collection,
    dataset,
    organisation,
    endpoint_url,
    documentation_url,
    licence,
    start_date,
    organisation_csv,
):
    check_organisation(organisation, dataset, organisation_csv)
    started_from = check_branch_is_up_to_date()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = RUNS_DIR / f"{stamp}-{organisation.replace(':', '-')}-{dataset}"
    run_dir.mkdir(parents=True)
    directories = directories_for(run_dir, organisation_csv)
    read_config_from_checkout()

    click.secho("1. Fetch", bold=True)
    resource_path, plugin = fetch(endpoint_url, run_dir)
    click.echo(
        f"Fetched resource {resource_path.name}"
        + (f" with plugin {plugin}" if plugin else "")
    )

    click.secho("\n2. Check", bold=True)
    click.echo(
        "(Check doesn't load lookup.csv, so any entity numbers it prints are provisional.)"
    )
    stage_resource(resource_path, directories, "check")
    check = workflow.run_workflow(
        resource_path.name,
        "check",
        collection,
        dataset,
        organisation,
        None,
        None,
        directories,
    )
    save(run_dir, "check-response.json", check)
    if "task-log" not in check:
        raise click.ClickException(
            f"Check failed, so treat as blocked: {check.get('message')} {check.get('exception', '')}"
        )
    must_fix, should_fix = must_and_should_fix(check["task-log"])
    click.echo("Must fix: " + ("none" if not must_fix else ""))
    for item in must_fix:
        click.echo(f"  - {item}")
    click.echo("Should fix: " + ("none" if not should_fix else ""))
    for item in should_fix:
        click.echo(f"  - {item}")
    if must_fix:
        raise click.ClickException(
            "Manage would not allow this data to be added. Nothing written."
        )

    click.secho("\n3. Add data", bold=True)
    stage_resource(resource_path, directories, "add")
    added = workflow.add_data_workflow(
        resource_path.name,
        "add",
        collection,
        dataset,
        organisation,
        endpoint_url,
        documentation_url,
        directories,
        licence,
        start_date,
        plugin,
    )
    save(run_dir, "add-response.json", added)
    if "pipeline-summary" not in added:
        raise click.ClickException(f"Add data failed: {added.get('message')}")
    print_add_summary(added, collection)
    check_endpoint_on_documentation_page(documentation_url, endpoint_url)

    click.secho("\n4. Write", bold=True)
    retire = choose_endpoints_to_retire(added, collection, endpoint_url)
    warn_if_replacement_creates_entities(added, retire)
    authoritative = ask_if_authoritative(added, organisation)
    if not click.confirm(
        f"Write these changes to collection/{collection}/ and pipeline/{collection}/?",
        default=False,
    ):
        click.echo(f"Nothing written. Responses saved in {run_dir}")
        return
    check_nobody_pushed(started_from)
    write_rows(added, collection, dataset, organisation, retire, authoritative)
    click.echo(
        f"\nDone. Check with `git diff`, then commit. Responses saved in {run_dir}"
    )


if __name__ == "__main__":
    main()

import csv
from pathlib import Path
from typing import Optional

from click.testing import CliRunner
import pytest

import bin.add_data as add_data


class _CompletedProcess:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _write_csv(path: Path, header: list[str], rows: Optional[list[list[str]]] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header)
        if rows:
            writer.writerows(rows)


def test_get_commit_label_omits_empty_parts():
    response = {
        "params": {
            "dataset": "brownfield-land",
            "organisation": "local-authority:ABC",
        }
    }

    label = add_data.get_commit_label(response, "github-user")

    assert label == "add brownfield-land local-authority:ABC github-user"


def test_csv_writer_quotes_urls_with_commas(tmp_path):
    """Verify that URLs with commas are properly quoted when written to CSV."""
    csv_file = tmp_path / "test.csv"
    url_with_comma = "http://example.test/data?x=1,2,3"
    
    # Write a row with a URL containing commas
    _write_csv(csv_file, ["id", "url"], [["test-id", url_with_comma]])
    
    # Read back and verify the URL is intact
    csv_content = csv_file.read_text()
    rows = list(csv.reader(csv_content.splitlines()))
    
    assert rows[1][0] == "test-id"
    assert rows[1][1] == url_with_comma
    # Verify the CSV text contains quotes around the URL (escaped with quotes)
    assert f'"{url_with_comma}"' in csv_content


def test_ensure_file_ends_with_newline_appends_crlf(tmp_path):
    target = tmp_path / "example.csv"
    target.write_bytes(b"a,b,c")

    add_data.ensure_file_ends_with_newline(target)

    assert target.read_bytes() == b"a,b,c\r\n"


def test_append_endpoint_writes_new_row(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    endpoint_file = tmp_path / "collection/test-collection/endpoint.csv"
    _write_csv(
        endpoint_file,
        ["endpoint", "endpoint-url", "parameters", "plugin", "entry-date", "start-date", "end-date"],
    )

    response = {
        "response": {
            "data": {
                "endpoint-summary": {
                    "endpoint_url_in_endpoint_csv": False,
                    "new_endpoint_entry": {
                        "endpoint": "endpoint-1",
                        "endpoint-url": "https://example.test/data.csv",
                        "parameters": {"x": "1", "y": "2"},
                        "plugin": "url",
                        "entry-date": "2026-01-01",
                        "start-date": "",
                        "end-date": "",
                    },
                }
            }
        }
    }

    add_data.append_endpoint(response, "test-collection")

    rows = list(csv.reader(endpoint_file.read_text(encoding="utf-8").splitlines()))
    assert len(rows) == 2
    assert rows[1] == [
        "endpoint-1",
        "https://example.test/data.csv",
        '{"x": "1", "y": "2"}',
        "url",
        "2026-01-01",
        "",
        "",
    ]


def test_append_source_skips_when_documentation_url_exists(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    source_file = tmp_path / "collection/test-collection/source.csv"
    _write_csv(source_file, ["source", "attribution"])
    original = source_file.read_text(encoding="utf-8")

    response = {
        "response": {
            "data": {
                "source-summary": {
                    "documentation_url_in_source_csv": True,
                    "new_source_entry": {
                        "source": "test-source",
                    },
                }
            }
        }
    }

    add_data.append_source(response, "test-collection")

    assert source_file.read_text(encoding="utf-8") == original


def _entity_organisation_response(entries, authoritative=True):
    return {
        "params": {"authoritative": authoritative},
        "response": {
            "data": {
                "pipeline-summary": {
                    "entity-organisation": entries,
                }
            }
        },
    }


def test_append_entity_organisation_writes_new_row(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    entity_org_file = tmp_path / "pipeline/test-collection/entity-organisation.csv"
    _write_csv(entity_org_file, ["dataset", "entity-minimum", "entity-maximum", "organisation"])

    response = _entity_organisation_response(
        [
            {
                "dataset": "nature-improvement-area",
                "entity-minimum": 10100002,
                "entity-maximum": 10100005,
                "organisation": "government-organisation:PB202",
                "overlap": False,
                "error": False,
            }
        ]
    )

    add_data.append_entity_organisation(response, "test-collection")

    rows = list(csv.reader(entity_org_file.read_text(encoding="utf-8").splitlines()))
    assert rows[1] == [
        "nature-improvement-area",
        "10100002",
        "10100005",
        "government-organisation:PB202",
    ]


def test_append_entity_organisation_skips_entry_missing_range(tmp_path, monkeypatch):
    """overlap/error entries have no entity-minimum/maximum - must not be written"""
    monkeypatch.chdir(tmp_path)
    entity_org_file = tmp_path / "pipeline/test-collection/entity-organisation.csv"
    _write_csv(entity_org_file, ["dataset", "entity-minimum", "entity-maximum", "organisation"])
    original = entity_org_file.read_text(encoding="utf-8")

    response = _entity_organisation_response(
        [
            {
                "dataset": "nature-improvement-area",
                "organisation": "government-organisation:PB202",
                "overlap": True,
                "error": False,
            }
        ]
    )

    add_data.append_entity_organisation(response, "test-collection")

    assert entity_org_file.read_text(encoding="utf-8") == original


def test_append_entity_organisation_skips_when_not_authoritative(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    entity_org_file = tmp_path / "pipeline/test-collection/entity-organisation.csv"
    _write_csv(entity_org_file, ["dataset", "entity-minimum", "entity-maximum", "organisation"])
    original = entity_org_file.read_text(encoding="utf-8")

    response = _entity_organisation_response(
        [
            {
                "dataset": "nature-improvement-area",
                "entity-minimum": 10100002,
                "entity-maximum": 10100005,
                "organisation": "government-organisation:PB202",
                "overlap": False,
                "error": False,
            }
        ],
        authoritative=False,
    )

    add_data.append_entity_organisation(response, "test-collection")

    assert entity_org_file.read_text(encoding="utf-8") == original


def test_retire_endpoints_in_csv_updates_source_and_endpoint(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    endpoint_file = tmp_path / "collection/test-collection/endpoint.csv"
    source_file = tmp_path / "collection/test-collection/source.csv"
    _write_csv(
        endpoint_file,
        ["endpoint", "endpoint-url", "parameters", "plugin", "entry-date", "start-date", "end-date"],
        [
            ["endpoint-1", "https://example.test/1", "", "", "2026-01-01", "", ""],
            ["endpoint-2", "https://example.test/2", "", "", "2026-01-01", "", ""],
        ],
    )
    _write_csv(
        source_file,
        [
            "source",
            "attribution",
            "collection",
            "documentation-url",
            "endpoint",
            "licence",
            "organisation",
            "pipelines",
            "entry-date",
            "start-date",
            "end-date",
        ],
        [
            ["source-1", "", "test-collection", "", "endpoint-1", "", "", "", "2026-01-01", "", ""],
            ["source-2", "", "test-collection", "", "endpoint-2", "", "", "", "2026-01-01", "", ""],
        ],
    )

    add_data.retire_endpoints_in_csv("test-collection", ["endpoint-1"])

    endpoint_rows = list(csv.DictReader(endpoint_file.read_text(encoding="utf-8").splitlines()))
    source_rows = list(csv.DictReader(source_file.read_text(encoding="utf-8").splitlines()))

    today = add_data.datetime.now().strftime("%Y-%m-%d")
    assert endpoint_rows[0]["end-date"] == today
    assert endpoint_rows[1]["end-date"] == ""
    assert source_rows[0]["end-date"] == today
    assert source_rows[1]["end-date"] == ""


def test_resolve_branch_uses_append_mode_when_open_pr(monkeypatch):
    calls = []

    def mock_run_command(cmd, capture_output=False, check=True):
        calls.append(cmd)
        if cmd[:3] == ["gh", "pr", "list"]:
            return "42"
        return ""

    monkeypatch.setattr(add_data, "run_command", mock_run_command)

    branch_name, pr_number, mode = add_data.resolve_branch("feature/test", "collection-x")

    assert (branch_name, pr_number, mode) == ("feature/test", "42", "append")
    assert ["git", "fetch", "origin", "feature/test"] in calls
    assert ["git", "checkout", "feature/test"] in calls


def test_resolve_branch_create_mode_when_branch_exists(monkeypatch):
    def mock_run_command(cmd, capture_output=False, check=True):
        if cmd[:3] == ["gh", "pr", "list"]:
            return ""
        return ""

    monkeypatch.setattr(add_data, "run_command", mock_run_command)
    monkeypatch.setattr(add_data.subprocess, "run", lambda *args, **kwargs: _CompletedProcess(returncode=0))

    branch_name, pr_number, mode = add_data.resolve_branch("feature/test", "collection-x")

    assert (branch_name, pr_number, mode) == ("feature/test", "", "create")


def test_write_summary_appends_markdown(tmp_path, monkeypatch):
    summary_file = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary_file))

    add_data.write_summary("brownfield-land", "request-123")

    body = summary_file.read_text(encoding="utf-8")
    assert "### Workflow Summary" in body
    assert "brownfield-land updated via async request request-123" in body


def test_resolve_api_base_url_by_environment():
    assert add_data.resolve_api_base_url("development") == "https://pub-async.development.planning.data.gov.uk"
    assert add_data.resolve_api_base_url("staging") == "https://pub-async.staging.planning.data.gov.uk"
    assert add_data.resolve_api_base_url("production") == "https://pub-async.planning.data.gov.uk"


def test_click_cli_wires_options_to_runner(monkeypatch):
    captured = {}

    def fake_run(
        request_id,
        branch,
        triggered_by,
        environment,
        test_mode,
        retire_endpoints,
        entity_redirects,
    ):
        captured["request_id"] = request_id
        captured["branch"] = branch
        captured["triggered_by"] = triggered_by
        captured["environment"] = environment
        captured["test_mode"] = test_mode
        captured["retire_endpoints"] = retire_endpoints
        captured["entity_redirects"] = entity_redirects

    monkeypatch.setattr(add_data, "run_add_data_async", fake_run)
    runner = CliRunner()

    result = runner.invoke(
        add_data.main,
        [
            "--request-id",
            "req-123",
            "--branch",
            "feature/test",
            "--triggered-by",
            "bot",
            "--environment",
            "development",
            "--retire-endpoints",
            "endpoint-a,endpoint-b",
            "--retire-endpoints",
            "endpoint-c",
            "--entity-redirects",
            '[{"old_entity":"10","entity":"20","notes":"duplicate"}]',
            "--test",
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "request_id": "req-123",
        "branch": "feature/test",
        "triggered_by": "bot",
        "environment": "development",
        "test_mode": True,
        "retire_endpoints": ["endpoint-a", "endpoint-b", "endpoint-c"],
        "entity_redirects": [
            {"old_entity": "10", "entity": "20", "notes": "duplicate"}
        ],
    }


def test_normalize_entity_redirects_parses_json():
    redirects = add_data.normalize_entity_redirects(
        '[{"old_entity":"10","entity":"20","notes":"duplicate"},'
        '{"old_entity":"11","entity":"21"},'
        '{"old_entity":"","entity":"30"},'
        '"not-a-redirect"]'
    )

    assert redirects == [
        {"old_entity": "10", "entity": "20", "notes": "duplicate"},
        {"old_entity": "11", "entity": "21", "notes": ""},
    ]


def test_append_entity_redirects_creates_old_entity_csv(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    pipeline_dir = tmp_path / "pipeline" / "test-collection"
    pipeline_dir.mkdir(parents=True)

    add_data.append_entity_redirects(
        "test-collection",
        [{"old_entity": "10", "entity": "20", "notes": "duplicate"}],
    )

    old_entity_csv = pipeline_dir / "old-entity.csv"
    rows = list(csv.reader(old_entity_csv.read_text(encoding="utf-8").splitlines()))
    assert rows[0] == add_data.OLD_ENTITY_HEADER
    assert rows[1][0:5] == ["10", "301", "20", "duplicate", ""]
    assert rows[1][5]
    assert rows[1][6] == ""


def test_append_entity_redirects_skips_existing_old_entity(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    pipeline_dir = tmp_path / "pipeline" / "test-collection"
    pipeline_dir.mkdir(parents=True)
    old_entity_csv = pipeline_dir / "old-entity.csv"
    old_entity_csv.write_text(
        "old-entity,status,entity,notes,end-date,entry-date,start-date\r\n"
        "10,301,20,existing,,2026-01-01,\r\n",
        encoding="utf-8",
    )

    add_data.append_entity_redirects(
        "test-collection",
        [
            {"old_entity": "10", "entity": "99", "notes": "skip"},
            {"old_entity": "11", "entity": "21", "notes": "append"},
        ],
    )

    body = old_entity_csv.read_text(encoding="utf-8")
    assert body.count("\n") == 3
    assert "10,301,99,skip" not in body
    assert "11,301,21,append" in body


def test_run_add_data_async_test_mode_creates_draft_pr(tmp_path, monkeypatch, capfd):
    monkeypatch.chdir(tmp_path)
    
    # Create required directories
    collection_dir = tmp_path / "collection" / "test-collection"
    pipeline_dir = tmp_path / "pipeline" / "test-collection"
    collection_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    
    response = {
        "status": "COMPLETE",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "organisation": "test-organisation",
            "column_mapping": {"title": "field-title"},
        },
        "response": {
            "data": {
                "endpoint-summary": {
                    "new_endpoint_entry": {
                        "endpoint": "endpoint-1",
                        "endpoint-url": "example.test/path with space?a=b,c",
                        "parameters": "x=1",
                        "plugin": "url",
                        "entry-date": "2026-01-01",
                        "start-date": "",
                        "end-date": "",
                    }
                },
                "source-summary": {
                    "new_source_entry": {
                        "source": "source-1",
                        "attribution": "Example",
                        "collection": "test-collection",
                        "documentation-url": "example.test/docs path?x=y,z",
                        "endpoint": "endpoint-1",
                        "licence": "OGL",
                        "organisation": "test-organisation",
                        "pipelines": "test-dataset",
                        "entry-date": "2026-01-01",
                        "start-date": "",
                        "end-date": "",
                    }
                },
                "pipeline-summary": {
                    "new-entities": [],
                    "entity-organisation": [],
                },
            }
        },
    }

    commands = []

    def fake_run(cmd, text=True, capture_output=False, check=False):
        commands.append(cmd)
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            return _CompletedProcess(returncode=0, stdout="main")
        if cmd[:2] == ["git", "show-ref"]:
            return _CompletedProcess(returncode=1)
        if cmd[:3] == ["git", "diff", "--staged"]:
            return _CompletedProcess(returncode=1)
        if cmd[:3] == ["gh", "pr", "create"]:
            return _CompletedProcess(returncode=0)
        return _CompletedProcess(returncode=0)

    monkeypatch.setattr(add_data.subprocess, "run", fake_run)
    monkeypatch.setattr(add_data, "fetch_request", lambda api_base_url, request_id: response)
    monkeypatch.setattr(add_data, "write_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_endpoint", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_source", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_lookup", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_column", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_entity_organisation", lambda *args, **kwargs: None)

    add_data.run_add_data_async(
        request_id="req-123",
        branch="feature/test",
        triggered_by="bot",
        test_mode=True,
        environment="development",
    )

    out, err = capfd.readouterr()
    assert "Test mode enabled" in out
    assert "Created draft test PR on branch test/feature/test" in out
    assert ["git", "checkout", "-b", "test/feature/test"] in commands
    assert [
        "gh",
        "pr",
        "create",
        "--draft",
        "--title",
        "TEST ONLY: add test-dataset test-organisation bot",
        "--body",
        "This is a draft test PR generated by add_data.py.\n\nDo not merge this PR.\n\nRequest: req-123\nBranch: test/feature/test\nCommit: add test-dataset test-organisation bot\n",
        "--base",
        "main",
        "--head",
        "test/feature/test",
    ] in commands


def test_run_add_data_async_applies_retire_endpoints(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    collection_dir = tmp_path / "collection" / "test-collection"
    pipeline_dir = tmp_path / "pipeline" / "test-collection"
    collection_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    response = {
        "status": "COMPLETE",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "organisation": "test-organisation",
            "column_mapping": {"title": "field-title"},
        },
        "response": {
            "data": {
                "endpoint-summary": {},
                "source-summary": {},
                "pipeline-summary": {
                    "new-entities": [],
                    "entity-organisation": [],
                },
            }
        },
    }

    retired = {"collection": "", "endpoints": []}

    def fake_retire(collection, endpoints):
        retired["collection"] = collection
        retired["endpoints"] = endpoints

    def fake_run(cmd, text=True, capture_output=False, check=False):
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            return _CompletedProcess(returncode=0, stdout="main")
        if cmd[:2] == ["git", "show-ref"]:
            return _CompletedProcess(returncode=1)
        if cmd[:3] == ["git", "diff", "--staged"]:
            return _CompletedProcess(returncode=0)
        return _CompletedProcess(returncode=0)

    monkeypatch.setattr(add_data.subprocess, "run", fake_run)
    monkeypatch.setattr(add_data, "fetch_request", lambda api_base_url, request_id: response)
    monkeypatch.setattr(add_data, "write_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_endpoint", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_source", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "retire_endpoints_in_csv", fake_retire)
    monkeypatch.setattr(add_data, "append_lookup", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_column", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_entity_organisation", lambda *args, **kwargs: None)

    add_data.run_add_data_async(
        request_id="req-123",
        branch="",
        triggered_by="bot",
        test_mode=False,
        environment="development",
        retire_endpoints=["endpoint-old"],
    )

    assert retired == {
        "collection": "test-collection",
        "endpoints": ["endpoint-old"],
    }


def test_run_add_data_async_applies_entity_redirects(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    collection_dir = tmp_path / "collection" / "test-collection"
    pipeline_dir = tmp_path / "pipeline" / "test-collection"
    collection_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    response = {
        "status": "COMPLETE",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "organisation": "test-organisation",
            "column_mapping": {},
        },
        "response": {
            "data": {
                "endpoint-summary": {},
                "source-summary": {},
                "pipeline-summary": {
                    "new-entities": [],
                    "entity-organisation": [],
                },
            }
        },
    }

    captured = {"collection": "", "redirects": []}

    def fake_append_redirects(collection, redirects):
        captured["collection"] = collection
        captured["redirects"] = redirects

    def fake_run(cmd, text=True, capture_output=False, check=False):
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            return _CompletedProcess(returncode=0, stdout="main")
        if cmd[:2] == ["git", "show-ref"]:
            return _CompletedProcess(returncode=1)
        if cmd[:3] == ["git", "diff", "--staged"]:
            return _CompletedProcess(returncode=0)
        return _CompletedProcess(returncode=0)

    monkeypatch.setattr(add_data.subprocess, "run", fake_run)
    monkeypatch.setattr(add_data, "fetch_request", lambda api_base_url, request_id: response)
    monkeypatch.setattr(add_data, "write_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_endpoint", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_source", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "retire_endpoints_in_csv", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_entity_redirects", fake_append_redirects)
    monkeypatch.setattr(add_data, "append_lookup", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_column", lambda *args, **kwargs: None)
    monkeypatch.setattr(add_data, "append_entity_organisation", lambda *args, **kwargs: None)

    add_data.run_add_data_async(
        request_id="req-123",
        branch="",
        triggered_by="bot",
        test_mode=False,
        environment="development",
        entity_redirects='[{"old_entity":"10","entity":"20","notes":"duplicate"}]',
    )

    assert captured == {
        "collection": "test-collection",
        "redirects": [
            {"old_entity": "10", "entity": "20", "notes": "duplicate"}
        ],
    }


def test_run_command_reports_missing_binary(monkeypatch):
    def raise_missing(*args, **kwargs):
        raise FileNotFoundError("missing")

    monkeypatch.setattr(add_data.subprocess, "run", raise_missing)

    with pytest.raises(SystemExit):
        add_data.run_command(["gh", "--version"])

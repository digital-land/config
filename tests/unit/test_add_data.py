import csv
from pathlib import Path

from click.testing import CliRunner
import pytest

import bin.add_data as add_data


class _CompletedProcess:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_get_commit_label_omits_empty_parts():
    response = {
        "params": {
            "dataset": "brownfield-land",
            "organisation": "local-authority:ABC",
        }
    }

    label = add_data.get_commit_label(response, "github-user")

    assert label == "add brownfield-land local-authority:ABC github-user"


def test_encode_url_for_csv_encodes_spaces_and_preserves_existing_encoding():
    url = "example.test/path with space/already%20encoded?a=a b&x=1,2#frag ment"

    encoded = add_data.normalize_url(url)

    assert encoded == "https://example.test/path%20with%20space/already%20encoded?a=a%20b&x=1%2C2#frag%20ment"


def test_ensure_file_ends_with_newline_appends_crlf(tmp_path):
    target = tmp_path / "example.csv"
    target.write_bytes(b"a,b,c")

    add_data.ensure_file_ends_with_newline(target)

    assert target.read_bytes() == b"a,b,c\r\n"


def test_append_endpoint_writes_new_row(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    endpoint_file = tmp_path / "collection/test-collection/endpoint.csv"
    endpoint_file.parent.mkdir(parents=True, exist_ok=True)
    endpoint_file.write_text(
        "endpoint,endpoint-url,parameters,plugin,entry-date,start-date,end-date",
        encoding="utf-8",
    )

    response = {
        "response": {
            "data": {
                "endpoint-summary": {
                    "endpoint_url_in_endpoint_csv": False,
                    "new_endpoint_entry": {
                        "endpoint": "endpoint-1",
                        "endpoint-url": "https://example.test/data.csv",
                        "parameters": "x=1,y=2",
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
        "x=1,y=2",
        "url",
        "2026-01-01",
        "",
        "",
    ]


def test_append_source_skips_when_documentation_url_exists(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    source_file = tmp_path / "collection/test-collection/source.csv"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    original = "source,attribution\n"
    source_file.write_text(original, encoding="utf-8")

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


def test_retire_endpoints_in_csv_updates_source_and_endpoint(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    endpoint_file = tmp_path / "collection/test-collection/endpoint.csv"
    source_file = tmp_path / "collection/test-collection/source.csv"
    endpoint_file.parent.mkdir(parents=True, exist_ok=True)

    endpoint_file.write_text(
        "endpoint,endpoint-url,parameters,plugin,entry-date,start-date,end-date\n"
        "endpoint-1,https://example.test/1,,,2026-01-01,,\n"
        "endpoint-2,https://example.test/2,,,2026-01-01,,\n",
        encoding="utf-8",
    )
    source_file.write_text(
        "source,attribution,collection,documentation-url,endpoint,licence,organisation,pipelines,entry-date,start-date,end-date\n"
        "source-1,,test-collection,,endpoint-1,,,,2026-01-01,,\n"
        "source-2,,test-collection,,endpoint-2,,,,2026-01-01,,\n",
        encoding="utf-8",
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


def test_click_cli_wires_options_to_runner(monkeypatch):
    captured = {}

    def fake_run(request_id, branch, triggered_by, api_base_url, test_mode, retire_endpoints):
        captured["request_id"] = request_id
        captured["branch"] = branch
        captured["triggered_by"] = triggered_by
        captured["api_base_url"] = api_base_url
        captured["test_mode"] = test_mode
        captured["retire_endpoints"] = retire_endpoints

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
            "--api-base-url",
            "https://example.test",
            "--retire-endpoints",
            "endpoint-a,endpoint-b",
            "--retire-endpoints",
            "endpoint-c",
            "--test",
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "request_id": "req-123",
        "branch": "feature/test",
        "triggered_by": "bot",
        "api_base_url": "https://example.test",
        "test_mode": True,
        "retire_endpoints": ["endpoint-a", "endpoint-b", "endpoint-c"],
    }
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
        api_base_url="https://example.test",
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
        api_base_url="https://example.test",
        retire_endpoints=["endpoint-old"],
    )

    assert retired == {
        "collection": "test-collection",
        "endpoints": ["endpoint-old"],
    }


def test_run_command_reports_missing_binary(monkeypatch):
    def raise_missing(*args, **kwargs):
        raise FileNotFoundError("missing")

    monkeypatch.setattr(add_data.subprocess, "run", raise_missing)

    with pytest.raises(SystemExit):
        add_data.run_command(["gh", "--version"])

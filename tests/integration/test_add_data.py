import csv
from pathlib import Path

from click.testing import CliRunner

import bin.add_data as add_data


class _CompletedProcess:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _write_csv(path: Path, header: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header.split(","))


def test_add_data_cli_creates_expected_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary.md"))

    collection_dir = tmp_path / "collection" / "test-collection"
    pipeline_dir = tmp_path / "pipeline" / "test-collection"
    collection_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        collection_dir / "endpoint.csv",
        "endpoint,endpoint-url,parameters,plugin,entry-date,start-date,end-date",
    )
    _write_csv(
        collection_dir / "source.csv",
        "source,attribution,collection,documentation-url,endpoint,licence,organisation,pipelines,entry-date,start-date,end-date",
    )
    _write_csv(
        pipeline_dir / "lookup.csv",
        "prefix,resource,endpoint,entry-number,organisation,reference,entity,entry-date,start-date,end-date",
    )
    _write_csv(
        pipeline_dir / "column.csv",
        "dataset,endpoint,resource,column,field,start-date,end-date,entry-date",
    )
    _write_csv(
        pipeline_dir / "entity-organisation.csv",
        "dataset,entity-minimum,entity-maximum,organisation",
    )

    response = {
        "status": "COMPLETE",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "organisation": "test-organisation",
            "authoritative": True,
            "column_mapping": {
                "title": "field-title",
                "description": "field-description",
            },
        },
        "response": {
            "data": {
                "endpoint-summary": {
                    "endpoint_url_in_endpoint_csv": False,
                    "new_endpoint_entry": {
                        "endpoint": "endpoint-hash",
                        "endpoint-url": "https://example.test/data.csv",
                        "parameters": "format=csv",
                        "plugin": "download",
                        "entry-date": "2026-04-24",
                        "start-date": "2026-04-01",
                        "end-date": "",
                    },
                },
                "source-summary": {
                    "documentation_url_in_source_csv": False,
                    "new_source_entry": {
                        "source": "source-hash",
                        "attribution": "Example Org",
                        "collection": "test-collection",
                        "documentation-url": "https://example.test/docs",
                        "endpoint": "endpoint-hash",
                        "licence": "OGL",
                        "organisation": "test-organisation",
                        "pipelines": "test-dataset",
                        "entry-date": "2026-04-24",
                        "start-date": "2026-04-01",
                        "end-date": "",
                    },
                },
                "pipeline-summary": {
                    "new-entities": [
                        {
                            "prefix": "test-dataset",
                            "resource": "resource-hash",
                            "endpoint": "endpoint-hash",
                            "entry-number": 1,
                            "organisation": "test-organisation",
                            "reference": "ref-1",
                            "entity": 101,
                            "entry-date": "2026-04-24",
                            "start-date": "2026-04-01",
                            "end-date": "",
                        }
                    ],
                    "entity-organisation": [
                        {
                            "dataset": "test-dataset",
                            "entity-minimum": 101,
                            "entity-maximum": 101,
                            "organisation": "test-organisation",
                        }
                    ],
                },
            }
        },
    }

    monkeypatch.setattr(add_data, "fetch_request", lambda api_base_url, request_id: response)

    def fake_run(cmd, text=True, capture_output=False, check=False):
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            return _CompletedProcess(returncode=0, stdout="main")
        if cmd[:2] == ["git", "show-ref"]:
            return _CompletedProcess(returncode=1)
        if cmd[:3] == ["git", "diff", "--staged"]:
            return _CompletedProcess(returncode=1)
        return _CompletedProcess(returncode=0)

    monkeypatch.setattr(add_data.subprocess, "run", fake_run)

    runner = CliRunner()
    result = runner.invoke(
        add_data.main,
        [
            "--request-id",
            "req-123",
            "--triggered-by",
            "integration-test",
            "--environment",
            "development",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "No branch specified" in result.output

    endpoint_rows = list(csv.reader((collection_dir / "endpoint.csv").read_text(encoding="utf-8").splitlines()))
    source_rows = list(csv.reader((collection_dir / "source.csv").read_text(encoding="utf-8").splitlines()))
    lookup_rows = list(csv.reader((pipeline_dir / "lookup.csv").read_text(encoding="utf-8").splitlines()))
    column_rows = list(csv.reader((pipeline_dir / "column.csv").read_text(encoding="utf-8").splitlines()))
    entity_org_rows = list(csv.reader((pipeline_dir / "entity-organisation.csv").read_text(encoding="utf-8").splitlines()))

    assert endpoint_rows[1] == [
        "endpoint-hash",
        "https://example.test/data.csv",
        "format=csv",
        "download",
        "2026-04-24",
        "2026-04-01",
        "",
    ]
    assert source_rows[1] == [
        "source-hash",
        "Example Org",
        "test-collection",
        "https://example.test/docs",
        "endpoint-hash",
        "OGL",
        "test-organisation",
        "test-dataset",
        "2026-04-24",
        "2026-04-01",
        "",
    ]
    assert lookup_rows[1] == [
        "test-dataset",
        "resource-hash",
        "endpoint-hash",
        "1",
        "test-organisation",
        "ref-1",
        "101",
        "2026-04-24",
        "2026-04-01",
        "",
    ]
    assert column_rows[1] == [
        "test-dataset",
        "endpoint-hash",
        "",
        "title",
        "field-title",
        "2026-04-01",
        "",
        "2026-04-24",
    ]
    assert column_rows[2] == [
        "test-dataset",
        "endpoint-hash",
        "",
        "description",
        "field-description",
        "2026-04-01",
        "",
        "2026-04-24",
    ]
    assert entity_org_rows[1] == ["test-dataset", "101", "101", "test-organisation"]

    summary = (tmp_path / "summary.md").read_text(encoding="utf-8")
    assert "test-collection updated via async request req-123" in summary


def test_add_data_cli_test_mode_creates_draft_pr(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary.md"))

    collection_dir = tmp_path / "collection" / "test-collection"
    pipeline_dir = tmp_path / "pipeline" / "test-collection"
    collection_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    endpoint_file = collection_dir / "endpoint.csv"
    source_file = collection_dir / "source.csv"
    lookup_file = pipeline_dir / "lookup.csv"
    column_file = pipeline_dir / "column.csv"
    entity_org_file = pipeline_dir / "entity-organisation.csv"

    _write_csv(
        endpoint_file,
        "endpoint,endpoint-url,parameters,plugin,entry-date,start-date,end-date",
    )
    _write_csv(
        source_file,
        "source,attribution,collection,documentation-url,endpoint,licence,organisation,pipelines,entry-date,start-date,end-date",
    )
    _write_csv(
        lookup_file,
        "prefix,resource,endpoint,entry-number,organisation,reference,entity,entry-date,start-date,end-date",
    )
    _write_csv(
        column_file,
        "dataset,endpoint,resource,column,field,start-date,end-date,entry-date",
    )
    _write_csv(
        entity_org_file,
        "dataset,entity-minimum,entity-maximum,organisation",
    )

    response = {
        "status": "COMPLETE",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "organisation": "test-organisation",
            "authoritative": True,
            "column_mapping": {"title": "field-title"},
        },
        "response": {
            "data": {
                "endpoint-summary": {
                    "endpoint_url_in_endpoint_csv": False,
                    "new_endpoint_entry": {
                        "endpoint": "endpoint-hash",
                        "endpoint-url": "example.test/data path.csv?x=1,2",
                        "parameters": "format=csv",
                        "plugin": "download",
                        "entry-date": "2026-04-24",
                        "start-date": "2026-04-01",
                        "end-date": "",
                    },
                },
                "source-summary": {
                    "documentation_url_in_source_csv": False,
                    "new_source_entry": {
                        "source": "source-hash",
                        "attribution": "Example Org",
                        "collection": "test-collection",
                        "documentation-url": "example.test/docs path?x=y,z",
                        "endpoint": "endpoint-hash",
                        "licence": "OGL",
                        "organisation": "test-organisation",
                        "pipelines": "test-dataset",
                        "entry-date": "2026-04-24",
                        "start-date": "2026-04-01",
                        "end-date": "",
                    },
                },
                "pipeline-summary": {
                    "new-entities": [
                        {
                            "prefix": "test-dataset",
                            "resource": "resource-hash",
                            "endpoint": "endpoint-hash",
                            "entry-number": 1,
                            "organisation": "test-organisation",
                            "reference": "ref-1",
                            "entity": 101,
                            "entry-date": "2026-04-24",
                            "start-date": "2026-04-01",
                            "end-date": "",
                        }
                    ],
                    "entity-organisation": [
                        {
                            "dataset": "test-dataset",
                            "entity-minimum": 101,
                            "entity-maximum": 101,
                            "organisation": "test-organisation",
                        }
                    ],
                },
            }
        },
    }

    monkeypatch.setattr(add_data, "fetch_request", lambda api_base_url, request_id: response)

    commands = []

    def fake_run(cmd, text=True, capture_output=False, check=False):
        commands.append(cmd)
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            return _CompletedProcess(returncode=0, stdout="main")
        if cmd[:2] == ["git", "show-ref"]:
            return _CompletedProcess(returncode=1)
        if cmd[:3] == ["git", "diff", "--staged"]:
            return _CompletedProcess(returncode=1)
        if cmd[:3] == ["gh", "pr", "view"]:
            return _CompletedProcess(returncode=0, stdout="")
        return _CompletedProcess(returncode=0)

    monkeypatch.setattr(add_data.subprocess, "run", fake_run)

    runner = CliRunner()
    result = runner.invoke(
        add_data.main,
        [
            "--request-id",
            "req-123",
            "--triggered-by",
            "integration-test",
            "--environment",
            "development",
            "--branch",
            "feature/test",
            "--test",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Test mode enabled" in result.output
    assert "Created draft test PR on branch test/feature/test" in result.output

    endpoint_rows = list(csv.reader(endpoint_file.read_text(encoding="utf-8").splitlines()))
    source_rows = list(csv.reader(source_file.read_text(encoding="utf-8").splitlines()))
    lookup_rows = list(csv.reader(lookup_file.read_text(encoding="utf-8").splitlines()))
    column_rows = list(csv.reader(column_file.read_text(encoding="utf-8").splitlines()))
    entity_org_rows = list(csv.reader(entity_org_file.read_text(encoding="utf-8").splitlines()))

    assert endpoint_rows[1] == [
        "endpoint-hash",
        "example.test/data path.csv?x=1,2",
        "format=csv",
        "download",
        "2026-04-24",
        "2026-04-01",
        "",
    ]
    assert source_rows[1] == [
        "source-hash",
        "Example Org",
        "test-collection",
        "example.test/docs path?x=y,z",
        "endpoint-hash",
        "OGL",
        "test-organisation",
        "test-dataset",
        "2026-04-24",
        "2026-04-01",
        "",
    ]
    assert lookup_rows[1] == [
        "test-dataset",
        "resource-hash",
        "endpoint-hash",
        "1",
        "test-organisation",
        "ref-1",
        "101",
        "2026-04-24",
        "2026-04-01",
        "",
    ]
    assert column_rows[1] == [
        "test-dataset",
        "endpoint-hash",
        "",
        "title",
        "field-title",
        "2026-04-01",
        "",
        "2026-04-24",
    ]
    assert entity_org_rows[1] == ["test-dataset", "101", "101", "test-organisation"]

    assert ["git", "checkout", "-b", "test/feature/test"] in commands
    assert [
        "gh",
        "pr",
        "create",
        "--draft",
        "--title",
        "TEST ONLY: add test-dataset test-organisation integration-test",
        "--body",
        "This is a draft test PR generated by add_data.py.\n\nDo not merge this PR.\n\nRequest: req-123\nBranch: test/feature/test\nCommit: add test-dataset test-organisation integration-test\n",
        "--base",
        "main",
        "--head",
        "test/feature/test",
    ] in commands


def test_add_data_cli_retire_endpoint_updates_end_date(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary.md"))

    collection_dir = tmp_path / "collection" / "test-collection"
    pipeline_dir = tmp_path / "pipeline" / "test-collection"
    collection_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    endpoint_file = collection_dir / "endpoint.csv"
    source_file = collection_dir / "source.csv"

    with endpoint_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["endpoint", "endpoint-url", "parameters", "plugin", "entry-date", "start-date", "end-date"])
        writer.writerow(["endpoint-old", "https://example.test/old", "", "url", "2026-04-24", "2026-04-01", ""])
        writer.writerow(["endpoint-keep", "https://example.test/keep", "", "url", "2026-04-24", "2026-04-01", ""])

    with source_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow([
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
        ])
        writer.writerow([
            "source-old",
            "Example Org",
            "test-collection",
            "https://example.test/docs-old",
            "endpoint-old",
            "OGL",
            "test-organisation",
            "test-dataset",
            "2026-04-24",
            "2026-04-01",
            "",
        ])
        writer.writerow([
            "source-keep",
            "Example Org",
            "test-collection",
            "https://example.test/docs-keep",
            "endpoint-keep",
            "OGL",
            "test-organisation",
            "test-dataset",
            "2026-04-24",
            "2026-04-01",
            "",
        ])

    _write_csv(
        pipeline_dir / "lookup.csv",
        "prefix,resource,endpoint,entry-number,organisation,reference,entity,entry-date,start-date,end-date",
    )
    _write_csv(
        pipeline_dir / "column.csv",
        "dataset,endpoint,resource,column,field,start-date,end-date,entry-date",
    )
    _write_csv(
        pipeline_dir / "entity-organisation.csv",
        "dataset,entity-minimum,entity-maximum,organisation",
    )

    response = {
        "status": "COMPLETE",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "organisation": "test-organisation",
            "authoritative": False,
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

    monkeypatch.setattr(add_data, "fetch_request", lambda api_base_url, request_id: response)

    def fake_run(cmd, text=True, capture_output=False, check=False):
        if cmd[:3] == ["git", "diff", "--staged"]:
            return _CompletedProcess(returncode=0)
        return _CompletedProcess(returncode=0)

    monkeypatch.setattr(add_data.subprocess, "run", fake_run)

    runner = CliRunner()
    result = runner.invoke(
        add_data.main,
        [
            "--request-id",
            "req-123",
            "--triggered-by",
            "integration-test",
            "--environment",
            "development",
            "--retire-endpoints",
            "endpoint-old",
        ],
    )

    assert result.exit_code == 0, result.output

    endpoint_rows = list(csv.DictReader(endpoint_file.read_text(encoding="utf-8").splitlines()))
    source_rows = list(csv.DictReader(source_file.read_text(encoding="utf-8").splitlines()))
    today = add_data.datetime.now().strftime("%Y-%m-%d")

    assert endpoint_rows[0]["end-date"] == today
    assert endpoint_rows[1]["end-date"] == ""
    assert source_rows[0]["end-date"] == today
    assert source_rows[1]["end-date"] == ""
"""Comprehensive test suite for batch_assign_entities.py."""

import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch
import pathlib
import pandas as pd
import pytest
import requests

sys.path.insert(0, str(Path(__file__).parent.parent.parent / ".github/scripts"))

from batch_assign_entities import (
    ValidationConfig,
    _collect_validation_rows,
    _make_fingerprints,
    _possible_reference_churn_fuzzy_match_rows,
    _promote,
    download_file,
    download_urls,
    ensure_specification_dir,
    get_endpoints_with_any_history,
    get_old_resource_hashes_batch,
    get_scope,
    process_csv,
    run_command,
    run_batch_assign_entities,
)


@pytest.fixture(autouse=True)
def _isolate_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def temp_dirs():
    with tempfile.TemporaryDirectory() as cache_dir:
        with tempfile.TemporaryDirectory() as resource_dir:
            yield Path(cache_dir), Path(resource_dir)


def _issue_summary_df(resource_file: Path) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "collection": ["conservation-area"],
            "resource": ["resource123"],
            "endpoint": ["endpoint456"],
            "pipeline": ["conservation-area"],
            "organisation": ["org1"],
            "download_link": ["http://example.com/resource123"],
            "resource_path": [str(resource_file)],
        }
    )


def test_get_scope_in_odp_datasets():
    scope_dict = {
        "odp": ["conservation-area", "listed-building"],
        "mandated": ["local-plan"],
    }
    assert get_scope("conservation-area", scope_dict) == "odp"


def test_get_scope_in_mandated_datasets():
    scope_dict = {
        "odp": ["conservation-area"],
        "mandated": ["local-plan", "heritage-coast"],
    }
    assert get_scope("heritage-coast", scope_dict) == "mandated"


def test_get_scope_not_found_returns_single_source():
    scope_dict = {
        "odp": ["conservation-area"],
        "mandated": ["local-plan"],
    }
    assert get_scope("unknown-dataset", scope_dict) == "single-source"


def test_get_scope_empty_dict():
    assert get_scope("any-dataset", {}) == "single-source"


def test_get_endpoints_with_any_history_empty_input():
    assert get_endpoints_with_any_history([]) == set()


@patch("batch_assign_entities.requests.get")
def test_get_endpoints_with_any_history_returns_set(mock_get):
    mock_get.return_value = Mock(
        status_code=200,
        text="endpoint\nendpoint-a\nendpoint-b\n",
        raise_for_status=lambda: None,
    )
    result = get_endpoints_with_any_history(["endpoint-a", "endpoint-b", "endpoint-c"])
    assert result == {"endpoint-a", "endpoint-b"}


@patch("batch_assign_entities.requests.get")
def test_get_endpoints_with_any_history_returns_none_on_failure(mock_get):
    mock_get.side_effect = Exception("network error")
    result = get_endpoints_with_any_history(["endpoint-a"])
    assert result is None


@patch("batch_assign_entities.sleep")
@patch("batch_assign_entities.requests.get")
def test_get_endpoints_with_any_history_retries_transient_failure_then_succeeds(mock_get, mock_sleep):
    """
    A 504 (the real failure mode seen from Datasette under load) must be retried via
    fetch_csv_with_retry, not fail this (whole-run) call on the first blip - that would
    make every endpoint look like it has no history, blocking every resource in the
    batch rather than just failing this one call.
    """
    # requests.get itself succeeds (a connection is made) - it's raise_for_status() that
    # raises HTTPError for a real 504, same as fetch_csv_with_retry expects to catch.
    timeout_response = Mock(
        raise_for_status=Mock(side_effect=requests.HTTPError("504 Server Error: Gateway Time-out"))
    )
    success_response = Mock(
        status_code=200,
        text="endpoint\nendpoint-a\n",
        raise_for_status=lambda: None,
    )
    mock_get.side_effect = [timeout_response, timeout_response, success_response]

    result = get_endpoints_with_any_history(["endpoint-a", "endpoint-b"])

    assert result == {"endpoint-a"}
    assert mock_get.call_count == 3
    assert mock_sleep.call_count == 2


@patch("subprocess.run")
def test_run_command_success(mock_run):
    mock_run.return_value = Mock(returncode=0, stdout="output", stderr="")
    assert run_command(["echo", "test"], capture_output=True) == "output"


@patch("subprocess.run")
def test_run_command_failure_with_check(mock_run):
    mock_run.return_value = Mock(returncode=1, stdout="", stderr="error message")
    with pytest.raises(RuntimeError, match="error message"):
        run_command(["false"], check=True)


@patch("subprocess.run")
def test_run_command_failure_without_check(mock_run):
    mock_run.return_value = Mock(returncode=1, stdout="", stderr="error")
    run_command(["false"], check=False)


@patch("subprocess.run")
def test_run_command_missing_command(mock_run):
    mock_run.side_effect = FileNotFoundError()
    with pytest.raises(RuntimeError, match="Required command not found"):
        run_command(["nonexistent"], check=True)


@patch("subprocess.run")
def test_run_command_no_capture(mock_run):
    mock_run.return_value = Mock(returncode=0, stdout="output", stderr="")
    assert run_command(["echo", "test"], capture_output=False) == ""


@patch("batch_assign_entities.urlretrieve")
def test_download_file_success(mock_retrieve):
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.csv"
        download_file("http://example.com/file.csv", str(output_path))
        mock_retrieve.assert_called_once()
        assert mock_retrieve.call_args[0][0] == "http://example.com/file.csv"


@patch("batch_assign_entities.urlretrieve")
def test_download_file_creates_parent_dirs(mock_retrieve):
    with tempfile.TemporaryDirectory() as tmpdir:
        nested_path = Path(tmpdir) / "sub" / "dir" / "test.csv"
        download_file("http://example.com/file.csv", str(nested_path))
        assert nested_path.parent.exists()


@patch("batch_assign_entities.urlretrieve")
def test_download_file_retry_on_failure(mock_retrieve):
    mock_retrieve.side_effect = Exception("Connection error")
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.csv"
        download_file(
            "http://example.com/file.csv",
            str(output_path),
            raise_error=False,
            max_retries=3,
        )
        assert mock_retrieve.call_count == 3


@patch("batch_assign_entities.urlretrieve")
def test_download_file_raise_on_error(mock_retrieve):
    mock_retrieve.side_effect = Exception("Connection error")
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "test.csv"
        with pytest.raises(Exception, match="Connection error"):
            download_file(
                "http://example.com/file.csv",
                str(output_path),
                raise_error=True,
                max_retries=1,
            )


@patch("batch_assign_entities.download_file")
@patch("batch_assign_entities.ThreadPoolExecutor")
def test_download_multiple_urls(mock_executor_class, mock_download):
    url_map = {
        "http://example.com/file1.csv": "/tmp/file1.csv",
        "http://example.com/file2.csv": "/tmp/file2.csv",
    }

    mock_executor = MagicMock()
    mock_executor_class.return_value.__enter__.return_value = mock_executor

    mock_future1 = MagicMock()
    mock_future1.result.return_value = None
    mock_future2 = MagicMock()
    mock_future2.result.return_value = None
    mock_executor.submit.side_effect = [mock_future1, mock_future2]

    download_urls(url_map, max_threads=2)
    assert mock_executor.submit.call_count == 2


@patch("batch_assign_entities.download_file")
@patch("batch_assign_entities.tqdm")
def test_download_urls_empty_map(mock_tqdm, mock_download):
    mock_tqdm.return_value = []
    download_urls({}, max_threads=2)
    mock_download.assert_not_called()


@patch("batch_assign_entities.get_endpoints_with_any_history")
@patch("batch_assign_entities.get_old_resource_hashes_batch")
@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df_from_hash")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_detects_duplicate_all_fields(
    mock_copy,
    mock_read_csv,
    mock_get_old_hash,
    mock_check,
    mock_batch_hashes,
    mock_endpoints_with_history,
    temp_dirs,
):
    cache_dir, resource_dir = temp_dirs
    resource_file = resource_dir / "resource123"
    resource_file.write_text("test data")

    old_resource_df = pd.DataFrame(
        {
            "entity": [1, 1, 1],
            "field": ["organisation", "reference", "prefix"],
            "value": ["org1", "ref1", "ca"],
        }
    )
    new_resource_df = pd.DataFrame(
        {
            "entity": [2, 2, 2],
            "field": ["organisation", "reference", "prefix"],
            "value": ["org1", "ref1", "ca"],
        }
    )
    lookup_df = pd.DataFrame({"prefix": ["ca"], "organisation": ["org1"], "entity": [1]})

    mock_batch_hashes.return_value = {"endpoint456": "hash123"}
    mock_endpoints_with_history.return_value = {"endpoint456"}
    mock_read_csv.side_effect = [lookup_df, new_resource_df, lookup_df]
    mock_get_old_hash.return_value = old_resource_df

    failed_downloads, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        skip_checks=False,
    )

    assert failed_downloads == []
    assert not output_df.empty
    assert "duplicate_entity_all_fields" in output_df["error_code"].values
    dup_rows = output_df[output_df["error_code"] == "duplicate_entity_all_fields"]
    assert "Matches existing entity" in dup_rows.iloc[0]["message"]


@patch("batch_assign_entities.get_endpoints_with_any_history")
@patch("batch_assign_entities.get_old_resource_hashes_batch")
@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df_from_hash")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_detects_duplicate_reference_organisation_despite_other_field_changes(
    mock_copy,
    mock_read_csv,
    mock_get_old_hash,
    mock_check,
    mock_batch_hashes,
    mock_endpoints_with_history,
    temp_dirs,
):
    """duplicate_prefix_reference_organisation was collapsed into
    duplicate_reference_organisation (provably identical in every call this script
    makes, since prefix is constant per dataset) - this still verifies a
    reference+organisation match is caught even when another field (status) differs,
    which used to additionally exercise the now-removed prefix-specific check."""
    cache_dir, resource_dir = temp_dirs
    resource_file = resource_dir / "resource123"
    resource_file.write_text("test data")

    old_resource_df = pd.DataFrame(
        {
            "entity": [1, 1, 1, 1],
            "field": ["prefix", "reference", "organisation", "status"],
            "value": ["ca", "ref1", "org1", "active"],
        }
    )
    new_resource_df = pd.DataFrame(
        {
            "entity": [2, 2, 2, 2],
            "field": ["prefix", "reference", "organisation", "status"],
            "value": ["ca", "ref1", "org1", "inactive"],
        }
    )
    lookup_df = pd.DataFrame({"prefix": ["ca"], "organisation": ["org1"], "entity": [1]})

    mock_batch_hashes.return_value = {"endpoint456": "hash123"}
    mock_endpoints_with_history.return_value = {"endpoint456"}
    mock_read_csv.side_effect = [lookup_df, new_resource_df, lookup_df]
    mock_get_old_hash.return_value = old_resource_df

    _, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        skip_checks=False,
    )

    assert not output_df.empty
    assert "duplicate_reference_organisation" in output_df["error_code"].values
    assert "duplicate_prefix_reference_organisation" not in output_df["error_code"].values


@patch("batch_assign_entities.get_endpoints_with_any_history")
@patch("batch_assign_entities.get_old_resource_hashes_batch")
@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df_from_hash")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_detects_low_entity_retention(
    mock_copy,
    mock_read_csv,
    mock_get_old_hash,
    mock_check,
    mock_batch_hashes,
    mock_endpoints_with_history,
    temp_dirs,
):
    """Replaces the old growth-only large_number_of_new_entities check (see ticket
    section 3): none of the 10 old entities carry over into the new resource, so
    retention is 0% - well below the threshold, regardless of how much growth there
    is alongside it."""
    cache_dir, resource_dir = temp_dirs
    resource_file = resource_dir / "resource123"
    resource_file.write_text("test data")

    old_entities = list(range(1, 11))
    old_resource_df = pd.DataFrame(
        {
            "entity": old_entities * 2,
            "field": ["organisation"] * 10 + ["reference"] * 10,
            "value": [f"org{i}" for i in range(1, 11)] + [f"ref{i}" for i in range(1, 11)],
        }
    )
    new_entities = list(range(11, 101))
    new_resource_df = pd.DataFrame(
        {
            "entity": new_entities * 2,
            "field": ["organisation"] * 90 + ["reference"] * 90,
            "value": [f"org{i}" for i in range(11, 101)] + [f"ref{i}" for i in range(11, 101)],
        }
    )
    lookup_df = pd.DataFrame(
        {"prefix": ["ca"] * 90, "organisation": [f"org{i}" for i in range(11, 101)], "entity": new_entities}
    )

    mock_batch_hashes.return_value = {"endpoint456": "hash123"}
    mock_endpoints_with_history.return_value = {"endpoint456"}
    mock_read_csv.side_effect = [lookup_df, new_resource_df, lookup_df]
    mock_get_old_hash.return_value = old_resource_df

    _, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        skip_checks=False,
    )

    assert "low_entity_retention" in output_df["error_code"].values
    retention_rows = output_df[output_df["error_code"] == "low_entity_retention"]
    assert "retention" in retention_rows.iloc[0]["message"].lower()


@patch("batch_assign_entities.get_endpoints_with_any_history")
@patch("batch_assign_entities.get_old_resource_hashes_batch")
@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df_from_hash")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_detects_duplicate_reference_organisation_in_new_resource(
    mock_copy,
    mock_read_csv,
    mock_get_old_hash,
    mock_check,
    mock_batch_hashes,
    mock_endpoints_with_history,
    temp_dirs,
):
    cache_dir, resource_dir = temp_dirs
    resource_file = resource_dir / "resource123"
    resource_file.write_text("test data")

    new_resource_df = pd.DataFrame(
        {
            "entity": [1, 1, 2, 2],
            "field": ["reference", "organisation", "reference", "organisation"],
            "value": ["ref1", "org1", "ref1", "org1"],
        }
    )
    lookup_df = pd.DataFrame({"prefix": ["ca", "ca"], "organisation": ["org1", "org1"], "entity": [1, 2]})

    mock_batch_hashes.return_value = {"endpoint456": None}
    mock_endpoints_with_history.return_value = {"endpoint456"}
    mock_read_csv.side_effect = [lookup_df, new_resource_df, lookup_df]
    mock_get_old_hash.return_value = None

    _, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        skip_checks=False,
    )

    assert "duplicate_reference_organisation_in_new_resource" in output_df["error_code"].values


@patch("batch_assign_entities.get_endpoints_with_any_history")
@patch("batch_assign_entities.get_old_resource_hashes_batch")
@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df_from_hash")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_detects_missing_organisation(
    mock_copy,
    mock_read_csv,
    mock_get_old_hash,
    mock_check,
    mock_batch_hashes,
    mock_endpoints_with_history,
    temp_dirs,
):
    cache_dir, resource_dir = temp_dirs
    resource_file = resource_dir / "resource123"
    resource_file.write_text("test data")

    new_resource_df = pd.DataFrame(
        {"entity": [1, 1, 1], "field": ["reference", "prefix", "organisation"], "value": ["ref1", "ca", ""]}
    )
    lookup_df = pd.DataFrame({"prefix": ["ca"], "organisation": ["org1"], "entity": [1]})

    mock_batch_hashes.return_value = {"endpoint456": None}
    mock_endpoints_with_history.return_value = {"endpoint456"}
    mock_read_csv.side_effect = [lookup_df, new_resource_df, lookup_df]
    mock_get_old_hash.return_value = None

    _, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        skip_checks=False,
    )

    assert "missing_organisation" in output_df["error_code"].values
    missing_rows = output_df[output_df["error_code"] == "missing_organisation"]
    assert "Missing organisation" in missing_rows.iloc[0]["message"]


@patch("batch_assign_entities.get_endpoints_with_any_history")
@patch("batch_assign_entities.get_old_resource_hashes_batch")
@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df_from_hash")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_detects_missing_reference(
    mock_copy,
    mock_read_csv,
    mock_get_old_hash,
    mock_check,
    mock_batch_hashes,
    mock_endpoints_with_history,
    temp_dirs,
):
    cache_dir, resource_dir = temp_dirs
    resource_file = resource_dir / "resource123"
    resource_file.write_text("test data")

    new_resource_df = pd.DataFrame(
        {"entity": [1, 1, 1], "field": ["organisation", "prefix", "reference"], "value": ["org1", "ca", ""]}
    )
    lookup_df = pd.DataFrame({"prefix": ["ca"], "organisation": ["org1"], "entity": [1]})

    mock_batch_hashes.return_value = {"endpoint456": None}
    mock_endpoints_with_history.return_value = {"endpoint456"}
    mock_read_csv.side_effect = [lookup_df, new_resource_df, lookup_df]
    mock_get_old_hash.return_value = None

    _, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        skip_checks=False,
    )

    assert "missing_reference" in output_df["error_code"].values
    missing_rows = output_df[output_df["error_code"] == "missing_reference"]
    assert "Missing reference" in missing_rows.iloc[0]["message"]


@patch("batch_assign_entities.get_endpoints_with_any_history")
@patch("batch_assign_entities.get_old_resource_hashes_batch")
@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df_from_hash")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_skip_checks_bypasses_validation(
    mock_copy,
    mock_read_csv,
    mock_get_old_hash,
    mock_check,
    mock_batch_hashes,
    mock_endpoints_with_history,
    temp_dirs,
):
    cache_dir, resource_dir = temp_dirs
    resource_file = resource_dir / "resource123"
    resource_file.write_text("test data")

    new_resource_df = pd.DataFrame({"entity": [1], "field": ["prefix"], "value": ["ca"]})
    lookup_df = pd.DataFrame({"prefix": ["ca"], "organisation": ["org1"], "entity": [1]})

    mock_batch_hashes.return_value = {"endpoint456": None}
    mock_endpoints_with_history.return_value = {"endpoint456"}
    mock_read_csv.side_effect = [lookup_df, new_resource_df, lookup_df]
    mock_get_old_hash.return_value = None

    _, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        skip_checks=True,
    )

    assert len(output_df[output_df["error_code"] == "missing_organisation"]) == 0
    assert len(output_df[output_df["error_code"] == "missing_reference"]) == 0


@patch("batch_assign_entities.Specification.download")
def test_ensure_specification_dir_creates_dir(mock_download):
    with tempfile.TemporaryDirectory() as tmpdir:
        spec_dir = Path(tmpdir) / "specification"
        result = ensure_specification_dir(spec_dir)
        assert result.exists()
        assert result.is_dir()
        mock_download.assert_called_once()


@patch("batch_assign_entities.Specification.download")
def test_ensure_specification_dir_existing(mock_download):
    with tempfile.TemporaryDirectory() as tmpdir:
        spec_dir = Path(tmpdir) / "specification"
        spec_dir.mkdir(parents=True)
        result = ensure_specification_dir(spec_dir)
        assert result.exists()
        mock_download.assert_called_once()


@patch("batch_assign_entities.process_csv")
@patch("batch_assign_entities.download_urls")
@patch("batch_assign_entities.ensure_specification_dir")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.requests.get")
def test_run_batch_assign_entities_handles_empty_filtered_summary(
    mock_get,
    mock_read_csv,
    mock_ensure_specification_dir,
    mock_download_urls,
    mock_process_csv,
):
    empty_issue_summary = pd.DataFrame(
        columns=["issue_type", "scope", "dataset", "collection", "resource", "endpoint", "pipeline", "organisation"]
    )
    provision_rule_df = pd.DataFrame(
        {
            "project": ["open-digital-planning"],
            "dataset": ["example-dataset"],
            "provision-reason": ["statutory"],
            "role": ["local-planning-authority"],
        }
    )
    invalid_uri_issues_df = pd.DataFrame(
        columns=["issue_type", "scope", "dataset", "collection", "resource", "endpoint", "pipeline", "organisation"]
    )

    mock_get.return_value = Mock(text="issue_type,scope,dataset,collection,resource,endpoint,pipeline,organisation\n")
    mock_read_csv.side_effect = [empty_issue_summary, invalid_uri_issues_df, provision_rule_df]
    mock_ensure_specification_dir.return_value = Path("specification")
    mock_process_csv.return_value = ([], pd.DataFrame())

    result = run_batch_assign_entities(scope="mandated", resources="resource-1")

    assert result is None
    mock_download_urls.assert_not_called()
    mock_process_csv.assert_not_called()


def test_detect_duplicate_all_fields_matches_correctly():
    old_df = pd.DataFrame(
        {"entity": [1, 1, 1], "field": ["organisation", "reference", "prefix"], "value": ["org1", "ref1", "ca"]}
    )
    new_df = pd.DataFrame(
        {"entity": [2, 2, 2], "field": ["organisation", "reference", "prefix"], "value": ["org1", "ref1", "ca"]}
    )

    old_fp = _make_fingerprints(old_df)
    new_fp = _make_fingerprints(new_df)

    assert old_fp["fingerprint"].iloc[0] == new_fp["fingerprint"].iloc[0]
    matches = new_fp.merge(old_fp, on="fingerprint", how="inner", suffixes=("_new", "_old"))
    assert not matches.empty
    assert len(matches) == 1
    assert matches["entity_new"].iloc[0] == 2
    assert matches["entity_old"].iloc[0] == 1


def test_fingerprint_excludes_reference_field_by_default():
    old_df = pd.DataFrame(
        {"entity": [1, 1, 1], "field": ["organisation", "reference", "prefix"], "value": ["org1", "ref1", "ca"]}
    )
    new_df = pd.DataFrame(
        {"entity": [2, 2, 2], "field": ["organisation", "reference", "prefix"], "value": ["org1", "ref2", "ca"]}
    )

    old_fp = _make_fingerprints(old_df)
    new_fp = _make_fingerprints(new_df)

    assert old_fp["fingerprint"].iloc[0] == new_fp["fingerprint"].iloc[0]
    matches = new_fp.merge(old_fp, on="fingerprint", how="inner", suffixes=("_new", "_old"))
    assert not matches.empty


def test_no_match_different_organisation():
    old_df = pd.DataFrame(
        {"entity": [1, 1, 1], "field": ["organisation", "reference", "prefix"], "value": ["org1", "ref1", "ca"]}
    )
    new_df = pd.DataFrame(
        {"entity": [2, 2, 2], "field": ["organisation", "reference", "prefix"], "value": ["org2", "ref1", "ca"]}
    )

    old_fp = _make_fingerprints(old_df)
    new_fp = _make_fingerprints(new_df)

    assert old_fp["fingerprint"].iloc[0] != new_fp["fingerprint"].iloc[0]
    matches = new_fp.merge(old_fp, on="fingerprint", how="inner", suffixes=("_new", "_old"))
    assert matches.empty


def test_duplicate_ref_org_fingerprints_match():
    old_df = pd.DataFrame({"entity": [1, 1], "field": ["reference", "organisation"], "value": ["ref1", "org1"]})
    new_df = pd.DataFrame({"entity": [2, 2], "field": ["reference", "organisation"], "value": ["ref1", "org1"]})

    old_fp = _make_fingerprints(old_df, except_fields=[], only_fields=["reference", "organisation"])
    new_fp = _make_fingerprints(new_df, except_fields=[], only_fields=["reference", "organisation"])

    assert old_fp["fingerprint"].iloc[0] == new_fp["fingerprint"].iloc[0]
    matches = new_fp.merge(old_fp, on="fingerprint", how="inner", suffixes=("_new", "_old"))
    assert not matches.empty
    assert matches["entity_new"].iloc[0] == 2
    assert matches["entity_old"].iloc[0] == 1


def test_multiple_duplicates_all_detected():
    old_df = pd.DataFrame(
        {
            "entity": [1, 1, 2, 2],
            "field": ["organisation", "reference", "organisation", "reference"],
            "value": ["org1", "ref1", "org2", "ref2"],
        }
    )
    new_df = pd.DataFrame(
        {
            "entity": [2, 2, 3, 3, 4, 4],
            "field": ["organisation", "reference", "organisation", "reference", "organisation", "reference"],
            "value": ["org1", "ref1", "org2", "ref2", "org3", "ref3"],
        }
    )

    old_fp = _make_fingerprints(old_df)
    new_fp = _make_fingerprints(new_df)
    matches = new_fp.merge(old_fp, on="fingerprint", how="inner", suffixes=("_new", "_old"))

    assert len(matches) == 2


def test_collect_validation_rows_reports_previous_resource_empty_and_missing_fields():
    current_resource_df = pd.DataFrame(
        {
            "entity": [1, 1, 2, 2],
            "field": ["organisation", "reference", "organisation", "reference"],
            "value": ["org1", "", "", "ref2"],
        }
    )
    old_resource_df = pd.DataFrame(columns=["entity", "field", "value"])

    rows, old_entities, new_entities = _collect_validation_rows(
        current_resource_df,
        old_resource_df,
        "conservation-area",
        "resource123",
        old_resource_hash="hash",
        config=ValidationConfig(),
    )

    error_codes = [row["error_code"] for row in rows]
    assert "previous_resource_empty" in error_codes
    assert "missing_organisation" in error_codes
    assert "missing_reference" in error_codes
    assert old_entities == set()
    assert new_entities == {1, 2}


def test_collect_validation_rows_blocks_on_fetch_failure_with_real_history():
    current_resource_df = pd.DataFrame(
        {"entity": [1], "field": ["reference"], "value": ["ref1"]}
    )

    rows, _, _ = _collect_validation_rows(
        current_resource_df,
        None,  # old_resource_df - fetch failed to resolve a baseline
        "conservation-area",
        "resource123",
        old_resource_hash=None,
        config=ValidationConfig(),
        endpoint_has_history=True,
    )

    matching = [row for row in rows if row["error_code"] == "previous_resource_not_found"]
    assert len(matching) == 1
    assert matching[0]["status"] == "blocked"


def test_collect_validation_rows_does_not_block_genuinely_new_endpoint():
    current_resource_df = pd.DataFrame(
        {"entity": [1], "field": ["reference"], "value": ["ref1"]}
    )

    rows, _, _ = _collect_validation_rows(
        current_resource_df,
        None,  # old_resource_df - no previous resource exists at all
        "conservation-area",
        "resource123",
        old_resource_hash=None,
        config=ValidationConfig(),
        endpoint_has_history=False,
    )

    matching = [row for row in rows if row["error_code"] == "new_endpoint_no_history"]
    assert len(matching) == 1
    assert matching[0]["status"] == "warn"
    assert "previous_resource_not_found" not in [row["error_code"] for row in rows]


def test_collect_validation_rows_excludes_pre_existing_entities_with_no_history():
    # Reproduces the 2026-10-28 incident: a resource with no resolvable old_resource_df
    # (new_endpoint_no_history) whose entities were all correctly re-matched by the
    # lookup phase to entities that already existed in the dataset before this resource
    # ran. Without pre_dataset_entities, every one of them would be wrongly counted as
    # new_entity_ids - and if the resource also has an invalid_uri_issue, _promote's
    # invalid_uri_entities exclusion then strips those pre-existing, already-valid rows
    # out of the real lookup.csv entirely.
    current_resource_df = pd.DataFrame(
        {"entity": ["1", "2"], "field": ["reference", "reference"], "value": ["ref1", "ref2"]}
    )

    rows, _, new_entities = _collect_validation_rows(
        current_resource_df,
        None,  # old_resource_df - no previous resource exists for this endpoint
        "conservation-area",
        "resource123",
        old_resource_hash=None,
        config=ValidationConfig(),
        endpoint_has_history=False,
        pre_dataset_entities={1, 2},  # both entities already existed in the dataset
    )

    assert new_entities == set()
    matching = [row for row in rows if row["error_code"] == "new_endpoint_no_history"]
    assert len(matching) == 1


def _entity_rows(entities, reference_prefix):
    rows = []
    for entity in entities:
        rows.append({"entity": entity, "field": "reference", "value": f"{reference_prefix}-{entity}"})
        rows.append({"entity": entity, "field": "organisation", "value": "org1"})
    return pd.DataFrame(rows)


def test_collect_validation_rows_retention_threshold_is_configurable():
    """
    Proves ValidationConfig.retention_threshold is actually read by the retention
    check, not just plumbed through and ignored - the same 80% retention (8 of 10 old
    entities still present) blocks under the default 90% threshold but not under a
    looser 70% override.
    """
    old_resource_df = _entity_rows(range(1, 11), "ref")  # entities 1-10
    current_resource_df = pd.concat(
        [
            _entity_rows(range(1, 9), "ref"),  # entities 1-8 retained
            _entity_rows([99], "new-ref"),  # one genuinely new entity
        ],
        ignore_index=True,
    )

    rows_default, _, _ = _collect_validation_rows(
        current_resource_df,
        old_resource_df,
        "conservation-area",
        "resource123",
        old_resource_hash="hash",
        config=ValidationConfig(),
    )
    assert "low_entity_retention" in [r["error_code"] for r in rows_default]

    rows_loosened, _, _ = _collect_validation_rows(
        current_resource_df,
        old_resource_df,
        "conservation-area",
        "resource123",
        old_resource_hash="hash",
        config=ValidationConfig(retention_threshold=70),
    )
    assert "low_entity_retention" not in [r["error_code"] for r in rows_loosened]


@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.pd.read_csv")
def test_process_csv_exception_handling(mock_read_csv, mock_check):
    with tempfile.TemporaryDirectory() as tmpdir:
        resource_dir = Path(tmpdir)
        cache_dir = Path(tmpdir) / "cache"
        cache_dir.mkdir()

        resource_file = resource_dir / "resource123"
        resource_file.write_text("test data")

        issue_df = pd.DataFrame(
            {
                "collection": ["conservation-area"],
                "resource": ["resource123"],
                "endpoint": ["endpoint456"],
                "pipeline": ["conservation-area"],
                "organisation": ["org1"],
                "download_link": ["http://example.com/resource123"],
                "resource_path": [str(resource_file)],
            }
        )

        mock_check.side_effect = Exception("Entity assignment failed")
        mock_read_csv.return_value = pd.DataFrame({"entity": []})

        failed_downloads, output_df = process_csv(
            "odp",
            resource_dir,
            issue_df,
            cache_dir,
            skip_checks=False,
        )

        assert failed_downloads == []
        assert not output_df.empty


def test_missing_organisation_detection():
    df = pd.DataFrame({"entity": [1, 2], "field": ["reference", "reference"], "value": ["ref1", "ref2"]})
    field_values = df[df["field"].isin(["organisation", "reference", "prefix"])] [["entity", "field", "value"]].drop_duplicates()
    fp = field_values.pivot_table(index="entity", columns="field", values="value", aggfunc="first").reset_index()

    for col in ["organisation", "reference", "prefix"]:
        if col not in fp.columns:
            fp[col] = None

    missing_df = fp[(fp["organisation"].isna()) | (fp["organisation"] == "")]
    assert len(missing_df) == 2
    assert list(missing_df["entity"]) == [1, 2]


def test_missing_reference_detection():
    df = pd.DataFrame({"entity": [1, 2], "field": ["organisation", "organisation"], "value": ["org1", "org2"]})
    field_values = df[df["field"].isin(["organisation", "reference", "prefix"])] [["entity", "field", "value"]].drop_duplicates()
    fp = field_values.pivot_table(index="entity", columns="field", values="value", aggfunc="first").reset_index()

    for col in ["organisation", "reference", "prefix"]:
        if col not in fp.columns:
            fp[col] = None

    missing_df = fp[(fp["reference"].isna()) | (fp["reference"] == "")]
    assert len(missing_df) == 2


def test_duplicate_reference_organisation_detection():
    df = pd.DataFrame(
        {
            "entity": [1, 2, 1, 2],
            "field": ["reference", "reference", "organisation", "organisation"],
            "value": ["ref1", "ref1", "org1", "org1"],
        }
    )

    dup_ref_org_df = _make_fingerprints(df, except_fields=[], only_fields=["organisation", "reference"])
    duplicated = dup_ref_org_df[dup_ref_org_df.duplicated("fingerprint", keep=False)]

    assert len(duplicated) == 2
    assert len(duplicated["fingerprint"].unique()) == 1
    assert set(duplicated["entity"].values) == {1, 2}


def test_duplicate_in_new_resource_only():
    df = pd.DataFrame(
        {
            "entity": [1, 2, 1, 2],
            "field": ["organisation", "organisation", "reference", "reference"],
            "value": ["org1", "org1", "ref1", "ref1"],
        }
    )

    dup_ref_org_df = _make_fingerprints(df, except_fields=[], only_fields=["organisation", "reference"])
    duplicated = dup_ref_org_df[dup_ref_org_df.duplicated("fingerprint", keep=False)].drop_duplicates("fingerprint")

    assert not duplicated.empty
    assert len(duplicated["fingerprint"].unique()) == 1


def test_fingerprint_preserves_entity_id():
    df = pd.DataFrame(
        {
            "entity": [100, 100, 200, 200],
            "field": ["organisation", "reference", "organisation", "reference"],
            "value": ["org1", "ref1", "org2", "ref2"],
        }
    )
    result = _make_fingerprints(df)
    assert set(result["entity"]) == {100, 200}


def test_fingerprint_handles_large_dataset():
    entities = list(range(1000))
    data = {
        "entity": entities * 3,
        "field": ["organisation"] * 1000 + ["reference"] * 1000 + ["prefix"] * 1000,
        "value": [f"org{i}" for i in range(1000)] + [f"ref{i}" for i in range(1000)] + ["ca"] * 1000,
    }
    df = pd.DataFrame(data)

    result = _make_fingerprints(df)
    assert len(result) == 1000
    assert len(result["fingerprint"].unique()) > 0
    

def test_evening_workflow_installs_gdal_and_uses_ubuntu_22_04():
    p = pathlib.Path(__file__).parent.parent.parent / ".github" / "workflows" / "config-evening-pipeline.yml"
    assert p.exists(), f"Workflow file {p} not found"
    content = p.read_text()

    assert "runs-on: ubuntu-22.04" in content
    assert "sudo add-apt-repository ppa:ubuntugis/ppa -y" in content
    assert "sudo apt-get update" in content
    assert "sudo apt-get install gdal-bin -y" in content
    assert "Run batch assign script" in content
    assert "name: Batch assign (${{ matrix.scope }})" in content
    assert "name: batch-assign-${{ matrix.scope }}-output" in content
    assert "path: batch_assign_summary*.csv" in content
    assert "name: batch-assign-${{ matrix.scope }}-diagnostics" in content
    assert "var/cache/assign_entities/transformed/*.csv" in content
    assert "issue_summary.csv" in content
    assert "invalid_uri_issues.csv" in content
    assert "issue_summary_full.csv" in content


# TEST _possible_reference_churn_fuzzy_match_rows

def test_fuzzy_match_flags_nudged_boundary_and_identical_name():
    old_df = pd.DataFrame(
        [
            {"entity": 1, "field": "geometry", "value": "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))"},
            {"entity": 1, "field": "name", "value": "Riverside Industrial Estate"},
        ]
    )
    new_df = pd.DataFrame(
        [
            {
                "entity": 10,
                "field": "geometry",
                "value": "POLYGON((0.05 0.05, 0.05 10.05, 10.05 10.05, 10.05 0.05, 0.05 0.05))",
            },
            {"entity": 10, "field": "name", "value": "Riverside Industrial Estate"},
        ]
    )

    rows = _possible_reference_churn_fuzzy_match_rows(old_df, new_df, "test-dataset", "resource123", ValidationConfig())

    assert len(rows) == 1
    assert rows[0]["status"] == "warn"
    assert rows[0]["error_code"] == "possible_reference_churn_fuzzy_match"
    assert "Entity 10" in rows[0]["message"]
    assert "entity 1" in rows[0]["message"]


def test_fuzzy_match_name_only_path_when_geometry_absent():
    old_df = pd.DataFrame([{"entity": 1, "field": "name", "value": "Riverside Industrial Estate"}])
    new_df = pd.DataFrame([{"entity": 10, "field": "name", "value": "Riverside Industrial Estate"}])

    rows = _possible_reference_churn_fuzzy_match_rows(old_df, new_df, "test-dataset", "resource123", ValidationConfig())

    assert len(rows) == 1
    assert "name similarity" in rows[0]["message"]


def test_fuzzy_match_does_not_flag_unrelated_entities():
    old_df = pd.DataFrame(
        [
            {"entity": 1, "field": "geometry", "value": "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))"},
            {"entity": 1, "field": "name", "value": "Riverside Industrial Estate"},
        ]
    )
    new_df = pd.DataFrame(
        [
            {"entity": 11, "field": "geometry", "value": "POLYGON((500 500, 500 510, 510 510, 510 500, 500 500))"},
            {"entity": 11, "field": "name", "value": "Brand New Site"},
        ]
    )

    rows = _possible_reference_churn_fuzzy_match_rows(old_df, new_df, "test-dataset", "resource123", ValidationConfig())

    assert rows == []


def test_fuzzy_match_handles_malformed_geometry_without_crashing():
    old_df = pd.DataFrame(
        [
            {"entity": 1, "field": "geometry", "value": "NOT VALID WKT"},
            {"entity": 1, "field": "name", "value": "Some Site"},
        ]
    )
    new_df = pd.DataFrame(
        [
            {"entity": 10, "field": "geometry", "value": "ALSO GARBAGE"},
            {"entity": 10, "field": "name", "value": "Some Site"},
        ]
    )

    rows = _possible_reference_churn_fuzzy_match_rows(old_df, new_df, "test-dataset", "resource123", ValidationConfig())

    # Malformed geometry is skipped, not fatal - the identical name still flags it
    assert len(rows) == 1
    assert "name similarity" in rows[0]["message"]
    assert "geometry overlap" not in rows[0]["message"]


def test_fuzzy_match_returns_empty_for_missing_name_and_geometry_fields():
    old_df = pd.DataFrame([{"entity": 1, "field": "reference", "value": "REF1"}])
    new_df = pd.DataFrame([{"entity": 10, "field": "reference", "value": "REF1"}])

    rows = _possible_reference_churn_fuzzy_match_rows(old_df, new_df, "test-dataset", "resource123", ValidationConfig())

    assert rows == []


def test_fuzzy_match_returns_empty_for_empty_inputs():
    empty_df = pd.DataFrame(columns=["entity", "field", "value"])

    rows = _possible_reference_churn_fuzzy_match_rows(empty_df, empty_df, "test-dataset", "resource123", ValidationConfig())

    assert rows == []


def _write_promote_fixtures(collection_name, cache_lookup_rows_csv):
    """Set up the cache + real lookup.csv/entity-organisation.csv files _promote reads
    and writes, relative to the isolated tmp_path cwd. Returns (cache_dir, lookup_path,
    entity_org_path)."""
    cache_dir = Path("var/cache")
    cache_lookup_path = cache_dir / "assign_entities" / collection_name / "pipeline" / "lookup.csv"
    cache_lookup_path.parent.mkdir(parents=True, exist_ok=True)
    cache_lookup_path.write_text(
        "prefix,resource,endpoint,entry-number,organisation,reference,entity,entry-date,start-date,end-date\n"
        + cache_lookup_rows_csv
    )

    lookup_path = Path("pipeline") / collection_name / "lookup.csv"
    lookup_path.parent.mkdir(parents=True, exist_ok=True)
    lookup_path.write_text(
        "prefix,resource,endpoint,entry-number,organisation,reference,entity,entry-date,start-date,end-date\n"
    )
    entity_org_path = Path("pipeline") / collection_name / "entity-organisation.csv"
    entity_org_path.write_text("dataset,entity-minimum,entity-maximum,organisation\n")

    return cache_dir, lookup_path, entity_org_path


def test_promote_copies_cache_lookup_and_appends_entity_organisation_range():
    cache_dir, lookup_path, entity_org_path = _write_promote_fixtures(
        "test-collection",
        "test-dataset,res1,,1,org-x,ref1,100,,,\n",
    )

    _promote(
        cache_dir,
        "test-collection",
        "test-dataset",
        "org-x",
        "odp",
        lookup_path,
        invalid_uri_entities=set(),
        pre_dataset_entities=set(),
    )

    assert set(pd.read_csv(lookup_path, dtype=str)["entity"]) == {"100"}
    assert entity_org_path.read_text() == (
        "dataset,entity-minimum,entity-maximum,organisation\n"
        "test-dataset,100,100,org-x\n"
    )


def test_promote_excludes_invalid_uri_entities_from_real_lookup():
    cache_dir, lookup_path, entity_org_path = _write_promote_fixtures(
        "test-collection",
        "test-dataset,res1,,1,org-x,ref1,100,,,\n"
        "test-dataset,res1,,2,org-x,ref2,200,,,\n",
    )

    _promote(
        cache_dir,
        "test-collection",
        "test-dataset",
        "org-x",
        "odp",
        lookup_path,
        invalid_uri_entities={"100"},
        pre_dataset_entities=set(),
    )

    # Excluded entity never reaches the real lookup.csv at all - not just status quo,
    # genuinely absent, so it resurfaces as unresolved next run.
    assert set(pd.read_csv(lookup_path, dtype=str)["entity"]) == {"200"}
    # Only the promoted entity (200) gets a range.
    assert entity_org_path.read_text() == (
        "dataset,entity-minimum,entity-maximum,organisation\n"
        "test-dataset,200,200,org-x\n"
    )


def test_promote_backfills_blank_organisation_in_lookup_and_ranges():
    cache_dir, lookup_path, entity_org_path = _write_promote_fixtures(
        "test-collection",
        # Blank organisation - the missing_organisation warn case, now non-blocking.
        "test-dataset,res1,,1,,ref1,100,,,\n",
    )

    _promote(
        cache_dir,
        "test-collection",
        "test-dataset",
        "org-real",
        "odp",
        lookup_path,
        invalid_uri_entities=set(),
        pre_dataset_entities=set(),
    )

    written_df = pd.read_csv(lookup_path, dtype=str)
    assert written_df.loc[written_df["entity"] == "100", "organisation"].iloc[0] == "org-real"
    assert "test-dataset,100,100,org-real" in entity_org_path.read_text()


def test_promote_respects_pb1164_exclusion():
    cache_dir, lookup_path, entity_org_path = _write_promote_fixtures(
        "conservation-area",
        "conservation-area,res1,,1,government-organisation:PB1164,ref1,100,,,\n",
    )

    _promote(
        cache_dir,
        "conservation-area",
        "conservation-area",
        "government-organisation:PB1164",
        "odp",
        lookup_path,
        invalid_uri_entities=set(),
        pre_dataset_entities=set(),
    )

    # The entity is still promoted into lookup.csv...
    assert "100" in set(pd.read_csv(lookup_path, dtype=str)["entity"])
    # ...but PB1164 never gets its own entity-organisation.csv range on this dataset -
    # the owning local authority does, recorded separately.
    assert entity_org_path.read_text() == "dataset,entity-minimum,entity-maximum,organisation\n"


def test_promote_single_source_listed_building_skips_entity_organisation_range():
    cache_dir, lookup_path, entity_org_path = _write_promote_fixtures(
        "listed-building",
        # Blank organisation, deliberate for single-source listed-building.
        "listed-building,res1,,1,,ref1,100,,,\n",
    )

    _promote(
        cache_dir,
        "listed-building",
        "listed-building",
        "",
        "single-source",
        lookup_path,
        invalid_uri_entities=set(),
        pre_dataset_entities=set(),
    )

    # Stays blank - not backfilled for this specific suppression.
    written_df = pd.read_csv(lookup_path, dtype=str)
    assert written_df.loc[written_df["entity"] == "100", "organisation"].isna().all()
    # No entity-organisation.csv range either.
    assert entity_org_path.read_text() == "dataset,entity-minimum,entity-maximum,organisation\n"

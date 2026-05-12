"""Comprehensive test suite for batch_assign_entities.py."""

import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "bin"))

from batch_assign_entities import ( 
    _make_fingerprints,
    download_file,
    download_urls,
    ensure_specification_dir,
    get_old_resource_df,
    get_scope,
    process_csv,
    run_command,
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


@patch("batch_assign_entities.requests.get")
def test_get_old_resource_success_returns_dataframe_with_entities(mock_get):
    historic_response = Mock()
    historic_response.text = "resource,endpoint\nhash123,endpoint456\n"

    transformed_response = Mock()
    transformed_response.text = (
        "entity,field,value\n"
        "1,organisation,org1\n"
        "1,reference,ref1\n"
        "2,organisation,org2\n"
        "2,reference,ref2\n"
    )

    mock_get.side_effect = [historic_response, transformed_response]

    result = get_old_resource_df("endpoint456", "conservation-area", "conservation-area")

    assert isinstance(result, pd.DataFrame)
    assert "entity" in result.columns
    assert len(result) == 4
    assert 1 in result["entity"].values
    assert 2 in result["entity"].values
    assert "org1" in result["value"].values


@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.requests.get")
def test_get_old_resource_no_previous_returns_none(mock_get, mock_read_csv):
    historic_response = Mock()
    historic_response.text = ""
    mock_get.return_value = historic_response
    mock_read_csv.return_value = pd.DataFrame()

    result = get_old_resource_df("endpoint456", "conservation-area", "conservation-area")

    assert result is None


@patch("batch_assign_entities.requests.get")
def test_get_old_resource_fetches_correct_url(mock_get):
    historic_response = Mock()
    historic_response.text = "resource,endpoint\nhash123,endpoint456\n"

    transformed_response = Mock()
    transformed_response.text = "entity,field,value\n1,organisation,org1\n"

    mock_get.side_effect = [historic_response, transformed_response]

    get_old_resource_df("endpoint456", "conservation-area", "conservation-area")

    first_call = mock_get.call_args_list[0]
    assert "endpoint__exact=endpoint456" in first_call[0][0]
    assert "performance/reporting_historic_endpoints.csv" in first_call[0][0]


@patch("batch_assign_entities.requests.get")
def test_get_old_resource_http_error_raises(mock_get):
    mock_get.side_effect = Exception("HTTP 404")
    with pytest.raises(Exception, match="HTTP 404"):
        get_old_resource_df("endpoint456", "conservation-area", "conservation-area")


@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_detects_duplicate_all_fields(
    mock_copy,
    mock_read_csv,
    mock_get_old,
    mock_check,
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

    mock_read_csv.side_effect = [lookup_df, pd.DataFrame({"entity": []}), new_resource_df, lookup_df]
    mock_get_old.return_value = old_resource_df

    failed_downloads, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        new_entity_threshold=10,
        skip_checks=False,
    )

    assert failed_downloads == []
    assert not output_df.empty
    assert "duplicate_entity_all_fields" in output_df["error_code"].values
    dup_rows = output_df[output_df["error_code"] == "duplicate_entity_all_fields"]
    assert "Matches existing entity" in dup_rows.iloc[0]["message"]


@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_detects_duplicate_prefix_reference_organisation(
    mock_copy,
    mock_read_csv,
    mock_get_old,
    mock_check,
    temp_dirs,
):
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

    mock_read_csv.side_effect = [lookup_df, pd.DataFrame({"entity": []}), new_resource_df, lookup_df]
    mock_get_old.return_value = old_resource_df

    _, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        new_entity_threshold=10,
        skip_checks=False,
    )

    assert not output_df.empty
    assert "duplicate_prefix_reference_organisation" in output_df["error_code"].values


@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_detects_large_new_entities(
    mock_copy,
    mock_read_csv,
    mock_get_old,
    mock_check,
    temp_dirs,
):
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
    new_entities = list(range(1, 101))
    new_resource_df = pd.DataFrame(
        {
            "entity": new_entities * 2,
            "field": ["organisation"] * 100 + ["reference"] * 100,
            "value": [f"org{i}" for i in range(1, 101)] + [f"ref{i}" for i in range(1, 101)],
        }
    )
    lookup_df = pd.DataFrame(
        {"prefix": ["ca"] * 100, "organisation": [f"org{i}" for i in range(1, 101)], "entity": new_entities}
    )

    mock_read_csv.side_effect = [lookup_df, pd.DataFrame({"entity": []}), new_resource_df, lookup_df]
    mock_get_old.return_value = old_resource_df

    _, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        new_entity_threshold=10,
        skip_checks=False,
    )

    assert "large_number_of_new_entities" in output_df["error_code"].values
    large_new_rows = output_df[output_df["error_code"] == "large_number_of_new_entities"]
    assert "large number of new entities" in large_new_rows.iloc[0]["message"].lower()


@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_detects_duplicate_reference_organisation_in_new_resource(
    mock_copy,
    mock_read_csv,
    mock_get_old,
    mock_check,
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

    mock_read_csv.side_effect = [lookup_df, pd.DataFrame({"entity": []}), new_resource_df, lookup_df]
    mock_get_old.return_value = None

    _, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        new_entity_threshold=10,
        skip_checks=False,
    )

    assert "duplicate_reference_organisation_in_new_resource" in output_df["error_code"].values


@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_detects_missing_organisation(
    mock_copy,
    mock_read_csv,
    mock_get_old,
    mock_check,
    temp_dirs,
):
    cache_dir, resource_dir = temp_dirs
    resource_file = resource_dir / "resource123"
    resource_file.write_text("test data")

    new_resource_df = pd.DataFrame(
        {"entity": [1, 1, 1], "field": ["reference", "prefix", "organisation"], "value": ["ref1", "ca", ""]}
    )
    lookup_df = pd.DataFrame({"prefix": ["ca"], "organisation": ["org1"], "entity": [1]})

    mock_read_csv.side_effect = [lookup_df, pd.DataFrame({"entity": []}), new_resource_df, lookup_df]
    mock_get_old.return_value = None

    _, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        new_entity_threshold=10,
        skip_checks=False,
    )

    assert "missing_organisation" in output_df["error_code"].values
    missing_rows = output_df[output_df["error_code"] == "missing_organisation"]
    assert "Missing organisation" in missing_rows.iloc[0]["message"]


@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_detects_missing_reference(
    mock_copy,
    mock_read_csv,
    mock_get_old,
    mock_check,
    temp_dirs,
):
    cache_dir, resource_dir = temp_dirs
    resource_file = resource_dir / "resource123"
    resource_file.write_text("test data")

    new_resource_df = pd.DataFrame(
        {"entity": [1, 1, 1], "field": ["organisation", "prefix", "reference"], "value": ["org1", "ca", ""]}
    )
    lookup_df = pd.DataFrame({"prefix": ["ca"], "organisation": ["org1"], "entity": [1]})

    mock_read_csv.side_effect = [lookup_df, pd.DataFrame({"entity": []}), new_resource_df, lookup_df]
    mock_get_old.return_value = None

    _, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        new_entity_threshold=10,
        skip_checks=False,
    )

    assert "missing_reference" in output_df["error_code"].values
    missing_rows = output_df[output_df["error_code"] == "missing_reference"]
    assert "Missing reference" in missing_rows.iloc[0]["message"]


@patch("batch_assign_entities.check_and_assign_entities")
@patch("batch_assign_entities.get_old_resource_df")
@patch("batch_assign_entities.pd.read_csv")
@patch("batch_assign_entities.shutil.copy")
def test_process_csv_skip_checks_bypasses_validation(
    mock_copy,
    mock_read_csv,
    mock_get_old,
    mock_check,
    temp_dirs,
):
    cache_dir, resource_dir = temp_dirs
    resource_file = resource_dir / "resource123"
    resource_file.write_text("test data")

    new_resource_df = pd.DataFrame({"entity": [1], "field": ["prefix"], "value": ["ca"]})
    lookup_df = pd.DataFrame({"prefix": ["ca"], "organisation": ["org1"], "entity": [1]})

    mock_read_csv.side_effect = [lookup_df, pd.DataFrame({"entity": []}), new_resource_df, lookup_df]
    mock_get_old.return_value = None

    _, output_df = process_csv(
        "odp",
        resource_dir,
        _issue_summary_df(resource_file),
        cache_dir,
        new_entity_threshold=10,
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
            new_entity_threshold=10,
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
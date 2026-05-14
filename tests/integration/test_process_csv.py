import requests
import pandas as pd
import pytest
import bin.batch_assign_entities as batch_assign_entities

@pytest.fixture
def setup_test_path(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "collection/test-collection").mkdir(parents=True, exist_ok=True)
    (tmp_path / "specification").mkdir(parents=True, exist_ok=True)
    (tmp_path / "pipeline/test-collection").mkdir(parents=True, exist_ok=True)
    (tmp_path / "pipeline/test-collection/lookup.csv").write_text("prefix,resource,endpoint,entry-number,organisation,reference,entity,entry-date,start-date,end-date\n")
    (tmp_path / "var/cache/organisation.csv").parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / "var/cache/organisation.csv").write_text("organisation\n")

    (tmp_path / "var/cache/assign_entities/test-collection/pipeline").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var/cache/assign_entities/test-collection/pipeline/lookup.csv").write_text("prefix,resource,endpoint,entry-number,organisation,reference,entity,entry-date,start-date,end-date\n")

    # mock check_and_assign_entities
    monkeypatch.setattr(batch_assign_entities, "check_and_assign_entities", lambda *args, **kwargs: None)


@pytest.fixture
def mock_resource_files(tmp_path):
    cache_dir = tmp_path / "var/cache/assign_entities/transformed"
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    success_resource = """entity,field,value
10,name,test_name
10,reference,ref1
10,geometry,POINT()
"""
    (cache_dir / "test-resource.csv").write_text(success_resource)

    success_old_resource = """entity,field,value
1,name,old_name
1,reference,ref1
1,geometry,POINT()
"""
    
    failure_resource = """entity,field,value
10,name,test_name
10,reference,ref10
10,geometry,POINT()
"""
    failure_old_resource = """entity,field,value
1,name,test_name
1,reference,ref1
1,geometry,POINT()
"""

    return {
        "success": {"resource": success_resource, "old_resource": success_old_resource},
        "failure": {"resource": failure_resource, "old_resource": failure_old_resource}
    }

@pytest.fixture
def mock_issue_summary(tmp_path):
    # create mock issue_summary.csv
    content = """issue_type,scope,dataset,collection,resource,endpoint,pipeline,organisation
unknown entity,odp,article-4-direction,test-collection,test-resource,test-endpoint,test-dataset,test-org
"""
    issue_summary_path = tmp_path / "issue_summary.csv"
    issue_summary_path.write_text(content)
    return issue_summary_path


class MockResponse:
    def __init__(self, text_data, status_code=200):
        self.text = text_data
        self._content = text_data.encode('utf-8')
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code != 200:
            raise requests.HTTPError("error")

    @property
    def content(self):
        return self._content
        

def test_process_csv_match_entities(
    capfd, 
    setup_test_path,
    mock_resource_files,
    mock_issue_summary,
    monkeypatch,
    tmp_path,
):
    resource_dir = tmp_path / "resource"
    resource_dir.mkdir(parents=True, exist_ok=True)
    resource_file = resource_dir / "test-resource"
    resource_file.write_text("resource")

    cache_dir = tmp_path / "var/cache"
    transformed_dir = cache_dir / "assign_entities" / "transformed"
    issue_dir = cache_dir / "assign_entities" / "issue"
    transformed_dir.mkdir(parents=True, exist_ok=True)
    issue_dir.mkdir(parents=True, exist_ok=True)
    (transformed_dir / "test-resource.csv").write_text(
        "entity,field,value\n"
        "2,organisation,org1\n"
        "2,name,old_name\n"
        "2,reference,ref1\n"
        "2,geometry,POINT()\n"
    )
    (issue_dir / "test-resource.csv").write_text("entity,issue\n2,unknown entity\n")

    issue_summary_df = pd.read_csv(mock_issue_summary)
    issue_summary_df["download_link"] = "http://example.com/test-resource"
    issue_summary_df["resource_path"] = str(resource_file)
    issue_summary_df["endpoint"] = "test-endpoint"

    monkeypatch.setattr(batch_assign_entities, "get_old_resource_hashes_batch", lambda *args, **kwargs: {"test-endpoint": "test-hash"})
    monkeypatch.setattr(batch_assign_entities, "get_old_resource_df_from_hash", lambda *args, **kwargs: pd.DataFrame(
        {
            "entity": [1, 1, 1, 1],
            "field": ["organisation", "name", "reference", "geometry"],
            "value": ["org1", "old_name", "ref1", "POINT()"],
        }
    ))
    monkeypatch.setattr(batch_assign_entities, "check_and_assign_entities", lambda *args, **kwargs: True)

    failed_downloads, output_df = batch_assign_entities.process_csv(
        scope="odp",
        resource_dir=resource_dir,
        issue_summary_df=issue_summary_df,
        cache_dir=cache_dir,
        new_entity_threshold=100,
    )
    out, err = capfd.readouterr()

    assert failed_downloads == []
    assert "duplicate_entity_all_fields" in output_df["error_code"].values
    assert "Downloaded: test-resource" not in out


def test_process_csv_success(
    capfd,
    setup_test_path,
    mock_resource_files,
    mock_issue_summary,
    monkeypatch,
    tmp_path,
):
    resource_dir = tmp_path / "resource"
    resource_dir.mkdir(parents=True, exist_ok=True)
    resource_file = resource_dir / "test-resource"
    resource_file.write_text("resource")

    cache_dir = tmp_path / "var/cache"
    transformed_dir = cache_dir / "assign_entities" / "transformed"
    issue_dir = cache_dir / "assign_entities" / "issue"
    transformed_dir.mkdir(parents=True, exist_ok=True)
    issue_dir.mkdir(parents=True, exist_ok=True)
    (transformed_dir / "test-resource.csv").write_text(
        "entity,field,value\n"
        "10,organisation,org1\n"
        "10,reference,ref1\n"
        "10,prefix,ca\n"
    )
    (issue_dir / "test-resource.csv").write_text("entity,issue\n10,unknown entity\n")

    # Create entity-organisation.csv so append operations don't fail
    entity_org_file = tmp_path / "pipeline/test-collection/entity-organisation.csv"
    entity_org_file.write_text("dataset,min_entity,max_entity,organisation\n")

    issue_summary_df = pd.read_csv(mock_issue_summary)
    issue_summary_df["download_link"] = "http://example.com/test-resource"
    issue_summary_df["resource_path"] = str(resource_file)
    issue_summary_df["endpoint"] = "test-endpoint"

    def mock_check_and_assign(*args, **kwargs):
        # Simulate check_and_assign_entities by writing entity 10 to the cache lookup
        cache_lookup = tmp_path / "var/cache/assign_entities/test-collection/pipeline/lookup.csv"
        cache_lookup.write_text(
            "prefix,resource,endpoint,entry-number,organisation,reference,entity,entry-date,start-date,end-date\n"
            "test-dataset,test-resource,,1,test-org,ref1,10,,\n"
        )

    monkeypatch.setattr(batch_assign_entities, "get_old_resource_hashes_batch", lambda *args, **kwargs: {"test-endpoint": "old-resource-hash"})
    monkeypatch.setattr(batch_assign_entities, "get_old_resource_df_from_hash", lambda *args, **kwargs: pd.DataFrame(
        {
            "entity": [1],
            "field": ["organisation"],
            "value": ["org1"],
        }
    ))
    monkeypatch.setattr(batch_assign_entities, "check_and_assign_entities", mock_check_and_assign)

    failed_downloads, output_df = batch_assign_entities.process_csv(
        scope="odp",
        resource_dir=resource_dir,
        issue_summary_df=issue_summary_df,
        cache_dir=cache_dir,
        new_entity_threshold=100,
    )
    out, err = capfd.readouterr()

    assert failed_downloads == []
    assert "success" in output_df["status"].values
    assert "Downloaded: test-resource" not in out

    updated_lookup = tmp_path / "pipeline/test-collection/lookup.csv"
    assert updated_lookup.exists(), "Updated lookup.csv should exist after success"

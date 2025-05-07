import requests
import pytest
import bin.batch_assign_entities as batch_assign_entities


@pytest.fixture
def setup_test_path(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "collection/test-collection").mkdir(parents=True, exist_ok=True)
    (tmp_path / "specification").mkdir(parents=True, exist_ok=True)
    (tmp_path / "pipeline/test-collection").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var/cache/organisation.csv").parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / "var/cache/organisation.csv").write_text("organisation\n")

    (tmp_path / "var/cache/assign_entities/test-collection/pipeline").mkdir(parents=True, exist_ok=True)
    (tmp_path / "var/cache/assign_entities/test-collection/pipeline/lookup.csv").write_text("lookups")

    # mock check_and_assign_entities
    monkeypatch.setattr(batch_assign_entities, "check_and_assign_entities", lambda *args, **kwargs: None)


@pytest.fixture
def mock_user_response(monkeypatch):
    monkeypatch.setattr(batch_assign_entities, "get_user_response", lambda _: "no")


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
    mock_user_response, 
    mock_resource_files, 
    mock_issue_summary,
    monkeypatch,
    tmp_path,
):
    
    def mock_get_match_entities(url, *args, **kwargs):
        if "reporting_historic_endpoints.csv" in url:
            return MockResponse('resource\nold-hash\n')
        elif "old-hash.csv" in url:
            return MockResponse(mock_resource_files["failure"]["old_resource"])
        elif "collection/resource/test-resource" in url:
            return MockResponse("resource")
        else:
            raise ValueError("Unexpected URL")

    monkeypatch.setattr(batch_assign_entities.requests, "get", mock_get_match_entities)
    monkeypatch.setattr(batch_assign_entities, "check_and_assign_entities", lambda *args, **kwargs: True)

    failed_downloads, failed_assignments = batch_assign_entities.process_csv(scope="odp")
    out, err = capfd.readouterr()

    assert failed_downloads == []
    assert failed_assignments == []
    assert "Downloaded: test-resource" in out
    assert "Matching entities found (new_entity:matched_current_entity): {10: 1}" in out

    resources_dir = tmp_path / "resource"
    assert not any(resources_dir.glob("*")), "Resource file still exists"


def test_process_csv_success(
    capfd, 
    setup_test_path, 
    mock_user_response, 
    mock_resource_files, 
    mock_issue_summary, 
    monkeypatch,
    tmp_path,
):

    def mock_get_success(url, *args, **kwargs):
        if "reporting_historic_endpoints.csv" in url:
            return MockResponse('resource\nold-hash\n')
        elif "old-hash.csv" in url:
            return MockResponse(mock_resource_files["success"]["old_resource"])
        elif "collection/resource/test-resource" in url:
            return MockResponse("resource")
        else:
            raise ValueError("Unexpected URL")

    monkeypatch.setattr(batch_assign_entities.requests, "get", mock_get_success)
    monkeypatch.setattr(batch_assign_entities, "check_and_assign_entities", lambda *args, **kwargs: True)

    failed_downloads, failed_assignments = batch_assign_entities.process_csv(scope="odp")
    out, err = capfd.readouterr()

    assert failed_downloads == []
    assert failed_assignments == []
    assert "Downloaded: test-resource" in out
    assert "Matching entities found" not in out

    updated_lookup = tmp_path / "pipeline/test-collection/lookup.csv"
    assert updated_lookup.exists(), "Updated lookup.csv should exist after success"

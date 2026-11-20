import io
import json

import boto3
from botocore.stub import Stubber
import pytest

from bin import add_data


@pytest.fixture
def payload():
    return {
        "id": "request-123",
        "type": "add_data",
        "status": "COMPLETE",
        "params": {"collection": "conservation-area", "dataset": "conservation-area"},
        "response": {
            "error": None,
            "data": {
                "pipeline-summary": {"new-entities": []},
                "endpoint-summary": {},
                "source-summary": {},
            },
        },
    }


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("REQUEST_DETAILS_BUCKET_NAME", "request-results-test")
    client = boto3.client(
        "s3",
        region_name="eu-west-2",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    monkeypatch.setattr(boto3, "client", lambda service: client)
    with Stubber(client) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


def queue_payload(s3, payload):
    s3.add_response(
        "get_object",
        {"Body": io.BytesIO(json.dumps(payload).encode())},
        {"Bucket": "request-results-test", "Key": "request-123.json"},
    )


def test_downloads_request_result(s3, payload):
    queue_payload(s3, payload)
    assert add_data.fetch_request("staging", "request-123") == payload


@pytest.mark.parametrize("code", ["NoSuchKey", "AccessDenied"])
def test_missing_or_denied_object_fails(s3, code, capsys):
    s3.add_client_error("get_object", service_error_code=code)
    with pytest.raises(SystemExit):
        add_data.fetch_request("staging", "request-123")
    assert code in capsys.readouterr().err


@pytest.mark.parametrize(
    "invalid",
    [
        "id",
        "status",
        "type",
        "error",
        "not_object",
    ],
)
def test_rejects_invalid_results_before_csv_changes(
    s3, payload, invalid, tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    if invalid == "not_object":
        payload = []
    elif invalid == "error":
        payload["response"]["error"] = {"errMsg": "failed"}
    else:
        payload[invalid] = None
    queue_payload(s3, payload)
    with pytest.raises(SystemExit):
        add_data.run_add_data_async("request-123", environment="staging")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("field", ["params", "response"])
@pytest.mark.parametrize("missing", [True, False])
def test_completed_request_requires_params_and_response(s3, payload, field, missing, capsys):
    if missing:
        del payload[field]
    else:
        payload[field] = None
    queue_payload(s3, payload)
    with pytest.raises(SystemExit):
        add_data.run_add_data_async("request-123", environment="staging")
    assert "must contain params and response objects" in capsys.readouterr().err


def test_invalid_json_fails(s3):
    s3.add_response("get_object", {"Body": io.BytesIO(b"not json")})
    with pytest.raises(SystemExit):
        add_data.fetch_request("staging", "request-123")


def test_missing_bucket_fails(monkeypatch, capsys):
    monkeypatch.delenv("REQUEST_DETAILS_BUCKET_NAME", raising=False)
    with pytest.raises(SystemExit):
        add_data.fetch_request("staging", "request-123")
    assert "REQUEST_DETAILS_BUCKET_NAME" in capsys.readouterr().err


@pytest.mark.parametrize(
    "environment,request_id",
    [
        ("unknown", "request-123"),
        ("staging", "../request-123"),
        ("staging", ""),
    ],
)
def test_invalid_location_fails(environment, request_id, monkeypatch):
    monkeypatch.setenv("REQUEST_DETAILS_BUCKET_NAME", "request-results-test")
    with pytest.raises(SystemExit):
        add_data.fetch_request(environment, request_id)



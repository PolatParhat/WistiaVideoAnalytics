"""AWS adapters and Lambda handlers against moto (a fake AWS that runs in memory: no account, no cost)."""

import json
from datetime import date

import boto3
import pytest
import responses
from moto import mock_aws

from wistia.aws import lambda_handlers
from wistia.aws.adapters import DynamoWatermarkStore, S3Storage, get_secret

D = date.fromisoformat
BUCKET, TABLE, SECRET = "raw-bucket", "watermarks", "wistia/api-token"


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        boto3.client("s3").create_bucket(Bucket=BUCKET)
        boto3.client("dynamodb").create_table(
            TableName=TABLE,
            KeySchema=[{"AttributeName": "media_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "media_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        boto3.client("secretsmanager").create_secret(Name=SECRET, SecretString="fake-token\n")
        monkeypatch.setenv("RAW_BUCKET", BUCKET)
        monkeypatch.setenv("WATERMARK_TABLE", TABLE)
        monkeypatch.setenv("WISTIA_SECRET_ID", SECRET)
        monkeypatch.setenv("MEDIA_CONFIG", json.dumps([{"media_id": "m1", "channel": "Facebook"}]))
        yield


def test_s3_storage_writes_once(aws):
    storage = S3Storage(BUCKET)
    assert storage.put_json("raw/a.json", {"x": 1}) == f"s3://{BUCKET}/raw/a.json"
    body = boto3.client("s3").get_object(Bucket=BUCKET, Key="raw/a.json")["Body"].read()
    assert json.loads(body) == {"x": 1}
    with pytest.raises(FileExistsError):
        storage.put_json("raw/a.json", {"x": 2})


def test_dynamo_watermark_only_moves_forward(aws):
    store = DynamoWatermarkStore(TABLE)
    assert store.get("m1") is None
    assert store.set("m1", D("2026-03-18")) is True
    assert store.set("m1", D("2026-03-17")) is False
    assert store.set("m1", D("2026-03-18")) is False
    assert store.get("m1") == D("2026-03-18")


def test_secret_is_stripped(aws):
    assert get_secret(SECRET) == "fake-token"


@pytest.mark.parametrize(
    "event, expected",
    [
        ({"run_date": "2026-03-17"}, "2026-03-17"),
        ({"scheduled_time": "2026-11-05T02:00:00Z"}, "2026-11-04"),
        ({}, "2026-11-03"),
    ],
)
def test_resolve_run_date(event, expected):
    from datetime import UTC, datetime

    now = datetime(2026, 11, 4, 2, 0, tzinfo=UTC)
    assert lambda_handlers.resolve_run_date(event, now=now) == D(expected)


@responses.activate
def test_ingest_handler_end_to_end(aws):
    responses.add_passthru("https://")  # let moto's fake AWS calls through; Wistia calls are mocked below
    base = "https://api.wistia.com/v1"
    responses.get(f"{base}/medias/m1.json", json={"hashed_id": "m1"})
    responses.get(f"{base}/stats/medias/m1/by_date.json", json=[{"date": "2026-03-17", "load_count": 1}])
    responses.get(f"{base}/stats/events.json", json=[{"event_key": "e1"}])
    responses.get(f"{base}/stats/events.json", json=[])

    out = lambda_handlers.ingest({"run_date": "2026-03-17"})

    assert out["run_date"] == "2026-03-17" and out["media_ids"] == ["m1"] and out["events"] == 1
    assert responses.calls[0].request.headers["Authorization"] == "Bearer fake-token"
    keys = [o["Key"] for o in boto3.client("s3").list_objects_v2(Bucket=BUCKET)["Contents"]]
    assert len(keys) == 3 and all(f"run_id={out['run_id']}" in k for k in keys)
    # ingestion alone never moves the watermark
    assert DynamoWatermarkStore(TABLE).get("m1") is None


def test_advance_watermarks_handler(aws):
    out = lambda_handlers.advance_watermarks({"run_id": "r1", "run_date": "2026-03-17", "media_ids": ["m1"]})
    assert out["moved"] == {"m1": True}
    assert DynamoWatermarkStore(TABLE).get("m1") == D("2026-03-17")

"""AWS Lambda entry points, called by Step Functions.

ingest              pull Wistia -> S3 raw for one run_date (does NOT move watermarks)
advance_watermarks  after the Glue transform and DQ checks succeeded: watermark := run_date

Environment variables (set by the CDK stack):
  RAW_BUCKET, WATERMARK_TABLE, WISTIA_SECRET_ID, MEDIA_CONFIG (JSON list of {media_id, channel})
"""

import json
import os
from datetime import UTC, date, datetime, timedelta

from wistia.api import WistiaClient
from wistia.aws.adapters import DynamoWatermarkStore, S3Storage, get_secret
from wistia.ingestion import run_ingestion
from wistia.logs import get_logger, log_event

log = get_logger("wistia.lambda")


def resolve_run_date(event, now=None):
    """Which day to load.

    {"run_date": "2026-03-17"}                  -> that day (backfill / simulation / manual rerun)
    {"scheduled_time": "2026-11-05T02:00:00Z"}  -> the day before (the daily schedule loads yesterday)
    {}                                          -> yesterday (UTC)
    """
    if event.get("run_date"):
        return date.fromisoformat(event["run_date"])
    if event.get("scheduled_time"):
        scheduled = datetime.fromisoformat(event["scheduled_time"].replace("Z", "+00:00"))
        return scheduled.date() - timedelta(days=1)
    return (now or datetime.now(UTC)).date() - timedelta(days=1)


def media_ids_from_env():
    return [m["media_id"] for m in json.loads(os.environ["MEDIA_CONFIG"])]


def ingest(event, context=None):
    run_date = resolve_run_date(event or {})
    media_ids = media_ids_from_env()
    client = WistiaClient(token=get_secret(os.environ["WISTIA_SECRET_ID"]))
    storage = S3Storage(os.environ["RAW_BUCKET"])
    watermarks = DynamoWatermarkStore(os.environ["WATERMARK_TABLE"])

    result = run_ingestion(client, storage, watermarks, media_ids, run_date)
    return {
        "run_id": result["run_id"],
        "run_date": result["run_date"],
        "media_ids": media_ids,
        "events": sum(m["events"] for m in result["media"]),
        "media": result["media"],
    }


def advance_watermarks(event, context=None):
    run_date = date.fromisoformat(event["run_date"])
    store = DynamoWatermarkStore(os.environ["WATERMARK_TABLE"])
    moved = {m: store.set(m, run_date) for m in event["media_ids"]}
    log_event(log, "watermarks", run_id=event.get("run_id"), run_date=run_date.isoformat(), moved=moved)
    return {"run_id": event.get("run_id"), "run_date": run_date.isoformat(), "moved": moved}

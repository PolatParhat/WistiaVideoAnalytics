"""Stage 3.3: incremental ingestion driven by a run_date and a watermark.

Pulls every endpoint we need for both media and saves the raw responses:
- all three endpoints (media metadata, daily stats, events)
- pagination for events: page 1, 2, 3 ... until Wistia returns an empty list []
- retries with exponential backoff on 429 / 5xx / network errors
- one JSON log line per API call
- raw responses saved under lake/raw/, wrapped with metadata, never overwritten

New in this stage:
- --run-date: the day this run is for (default: yesterday, UTC)
- watermark: per media, the last day loaded successfully (lake/state/watermarks.json)
- window: from (watermark + 1 day - 1 day lookback) to run_date, so only new days are pulled,
  plus the previous day again to catch late-arriving events

Run:  uv run python jobs/ingest.py --run-date 2026-03-17
"""

import argparse
import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(usecwd=True))

API_TOKEN = os.environ["WISTIA_API_TOKEN"]
BASE_URL = "https://api.wistia.com/v1"
MEDIA_IDS = ["8hunphufxp", "9k4tbcdfg0"]
LOOKBACK_DAYS = 1
PER_PAGE = 100  # Wistia's maximum
MAX_ATTEMPTS = 5
LAKE_DIR = Path("lake")
WATERMARK_FILE = LAKE_DIR / "state" / "watermarks.json"


# ---------- logging: one JSON object per line, easy to search in CloudWatch later ----------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        entry.update(getattr(record, "fields", {}))
        return json.dumps(entry)


handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
log = logging.getLogger("ingest")
log.addHandler(handler)
log.setLevel(logging.INFO)


# ---------- HTTP with retries ----------
RETRY_STATUS = {429, 500, 502, 503, 504}
session = requests.Session()
session.headers["Authorization"] = f"Bearer {API_TOKEN}"


def get_json(path, params=None):
    """GET one URL and return the parsed JSON. Retries temporary failures, raises on the rest."""
    url = f"{BASE_URL}{path}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        started = time.monotonic()
        try:
            response = session.get(url, params=params, timeout=30)
        except (requests.ConnectionError, requests.Timeout) as error:
            status, reason = None, type(error).__name__
        else:
            status, reason = response.status_code, response.reason
            fields = {"url": url, "params": params, "status": status, "attempt": attempt,
                      "ms": round((time.monotonic() - started) * 1000)}
            if status == 200:
                log.info("api call", extra={"fields": fields})
                return response.json()
            if status not in RETRY_STATUS:
                # 401 (bad token), 404 (wrong id) ... retrying will not help
                log.error("api call failed", extra={"fields": fields})
                response.raise_for_status()

        if attempt == MAX_ATTEMPTS:
            raise RuntimeError(f"giving up on {url} after {MAX_ATTEMPTS} attempts ({status} {reason})")
        # exponential backoff: 1, 2, 4, 8 s (+ a little random jitter); honour Retry-After on 429
        wait = 2 ** (attempt - 1) + random.random()
        if status == 429 and response.headers.get("Retry-After", "").isdigit():
            wait = int(response.headers["Retry-After"])
        log.warning("retrying", extra={"fields": {"url": url, "status": status, "reason": reason,
                                                  "attempt": attempt, "wait_s": round(wait, 1)}})
        time.sleep(wait)


# ---------- watermark (stage 3.8 moves this to DynamoDB) ----------
def read_watermarks():
    if WATERMARK_FILE.exists():
        return json.loads(WATERMARK_FILE.read_text())
    return {}


def write_watermark(media_id, loaded_date):
    marks = read_watermarks()
    # never move a watermark backwards: re-running an old day must not undo later loads
    if media_id in marks and marks[media_id] >= loaded_date.isoformat():
        return
    marks[media_id] = loaded_date.isoformat()
    WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)
    WATERMARK_FILE.write_text(json.dumps(marks, indent=2, sort_keys=True))


def compute_window(last_loaded, run_date):
    """Return (start, end) dates to pull for one media.

    first run (no watermark)  -> run_date .. run_date
    normal run                -> last_loaded + 1 day - lookback .. run_date
    re-run of an older day    -> never start after run_date
    """
    if last_loaded is None:
        return run_date, run_date
    start = last_loaded + timedelta(days=1) - timedelta(days=LOOKBACK_DAYS)
    return min(start, run_date), run_date


# ---------- raw storage ----------
def save_raw(endpoint, media_id, run_date, run_id, page, request_path, params, data, window):
    """Write one API response as a file. Each run has its own run_id folder, so nothing is overwritten."""
    folder = (LAKE_DIR / "raw" / "wistia" / endpoint / f"media_id={media_id}"
              / f"run_date={run_date}" / f"run_id={run_id}")
    folder.mkdir(parents=True, exist_ok=True)
    record = {
        "run_id": run_id,
        "request_url": f"{BASE_URL}{request_path}",
        "request_params": params,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window_start": window["start_date"],
        "window_end": window["end_date"],
        "data": data,
    }
    file = folder / f"page_{page:04d}.json"
    file.write_text(json.dumps(record, indent=2))
    return file


# ---------- one media ----------
def ingest_media(media_id, run_date, run_id):
    last = read_watermarks().get(media_id)
    start, end = compute_window(date.fromisoformat(last) if last else None, run_date)
    window = {"start_date": start.isoformat(), "end_date": end.isoformat()}
    log.info("window", extra={"fields": {"media_id": media_id, "watermark": last, **window}})

    # 1. metadata (one record, every run: cheap, and `updated` shows when it changed)
    path = f"/medias/{media_id}.json"
    save_raw("medias", media_id, run_date, run_id, 1, path, None, get_json(path), window)

    # 2. daily stats for the window (one row per day)
    path = f"/stats/medias/{media_id}/by_date.json"
    daily = get_json(path, window)
    save_raw("media_stats_by_date", media_id, run_date, run_id, 1, path, window, daily, window)

    # 3. events, paginated until an empty page
    path = "/stats/events.json"
    page, events = 1, 0
    while True:
        params = {"media_id": media_id, **window, "per_page": PER_PAGE, "page": page}
        rows = get_json(path, params)
        if not rows:  # [] means there are no more pages
            break
        save_raw("events", media_id, run_date, run_id, page, path, params, rows, window)
        events += len(rows)
        page += 1

    # Locally the watermark moves right after ingestion. In AWS it moves only after the
    # transform and data-quality checks succeed (a separate Step Functions task).
    write_watermark(media_id, run_date)
    log.info("media done", extra={"fields": {"media_id": media_id, "daily_rows": len(daily),
                                             "events": events, "event_pages": page - 1}})
    return events


def parse_args(argv=None):
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="Pull Wistia stats into lake/raw")
    parser.add_argument("--run-date", type=date.fromisoformat, default=yesterday,
                        help="day this run is for, YYYY-MM-DD (default: yesterday UTC)")
    parser.add_argument("--media", nargs="+", default=MEDIA_IDS, help="media ids (default: both)")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    log.info("run started", extra={"fields": {"run_id": run_id, "run_date": args.run_date.isoformat()}})
    for media_id in args.media:
        ingest_media(media_id, args.run_date, run_id)
    log.info("run finished", extra={"fields": {"run_id": run_id}})


if __name__ == "__main__":
    main()

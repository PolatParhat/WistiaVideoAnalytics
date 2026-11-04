"""One ingestion run: window -> extract -> land raw. Used by the local script and the Lambda handler."""

import uuid
from datetime import datetime, timedelta, timezone

from wistia.config import LOOKBACK_DAYS
from wistia.logs import get_logger, log_event
from wistia.storage import raw_key

log = get_logger("wistia.ingestion")


def new_run_id(now=None):
    now = now or datetime.now(timezone.utc)
    return now.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]


def compute_window(last_loaded, run_date, lookback_days=LOOKBACK_DAYS):
    """Return (start, end) dates to pull for one media.

    first run (no watermark)  -> run_date .. run_date
    normal run                -> last_loaded + 1 day - lookback .. run_date
    re-run of an older day    -> never start after run_date
    """
    if last_loaded is None:
        return run_date, run_date
    start = last_loaded + timedelta(days=1) - timedelta(days=lookback_days)
    return min(start, run_date), run_date


def _envelope(client, run_id, path, params, window, data):
    """Wrap an API response with where/when it came from."""
    return {
        "run_id": run_id,
        "request_url": client.url(path),
        "request_params": params,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window_start": window[0].isoformat(),
        "window_end": window[1].isoformat(),
        "data": data,
    }


def ingest_media(client, storage, media_id, run_date, run_id, last_loaded):
    """Pull all endpoints for one media and land them as raw files. Returns a summary dict."""
    window = compute_window(last_loaded, run_date)
    start, end = (d.isoformat() for d in window)
    log_event(log, "window", media_id=media_id, run_id=run_id,
              watermark=last_loaded.isoformat() if last_loaded else None, start_date=start, end_date=end)

    def land(endpoint, page, path, params, data):
        key = raw_key(endpoint, media_id, run_date.isoformat(), run_id, page)
        storage.put_json(key, _envelope(client, run_id, path, params, window, data))

    # 1. metadata: one record, every run (cheap; `updated` shows when it changed)
    land("medias", 1, f"/medias/{media_id}.json", None, client.media(media_id))

    # 2. daily stats for the window
    daily, params = client.media_stats_by_date(media_id, start, end)
    land("media_stats_by_date", 1, f"/stats/medias/{media_id}/by_date.json", params, daily)

    # 3. events, every page
    events = pages = 0
    for page, rows, page_params in client.event_pages(media_id, start, end):
        land("events", page, "/stats/events.json", page_params, rows)
        events += len(rows)
        pages = page

    summary = {"media_id": media_id, "window_start": start, "window_end": end,
               "daily_rows": len(daily), "events": events, "event_pages": pages}
    log_event(log, "media done", run_id=run_id, **summary)
    return summary


def run_ingestion(client, storage, watermarks, media_ids, run_date, run_id=None):
    """Ingest every media for one run_date. Does NOT advance watermarks (the caller decides when)."""
    run_id = run_id or new_run_id()
    log_event(log, "run started", run_id=run_id, run_date=run_date.isoformat(), media=media_ids)
    results = [ingest_media(client, storage, m, run_date, run_id, watermarks.get(m)) for m in media_ids]
    log_event(log, "run finished", run_id=run_id, events=sum(r["events"] for r in results))
    return {"run_id": run_id, "run_date": run_date.isoformat(), "media": results}

"""Incremental window, watermark store, raw storage and one full ingestion run (fake API client)."""

import json
from datetime import date

import pytest

from wistia.config import load_media
from wistia.ingestion import compute_window, run_ingestion
from wistia.state import LocalWatermarkStore
from wistia.storage import LocalStorage, raw_key

D = date.fromisoformat


# ---------- window ----------
@pytest.mark.parametrize(
    "watermark, run_date, expected",
    [
        (None, "2026-03-17", ("2026-03-17", "2026-03-17")),  # first run: only run_date
        ("2026-03-17", "2026-03-18", ("2026-03-17", "2026-03-18")),  # next day + 1-day lookback
        ("2026-03-17", "2026-03-20", ("2026-03-17", "2026-03-20")),  # missed days are caught up
        ("2026-03-23", "2026-03-20", ("2026-03-20", "2026-03-20")),  # re-run of an older day
        ("2026-03-18", "2026-03-18", ("2026-03-18", "2026-03-18")),  # same day again
    ],
)
def test_compute_window(watermark, run_date, expected):
    start, end = compute_window(D(watermark) if watermark else None, D(run_date))
    assert (start.isoformat(), end.isoformat()) == expected


# ---------- watermark store ----------
def test_watermark_starts_empty_and_advances(tmp_path):
    store = LocalWatermarkStore(tmp_path / "wm.json")
    assert store.get("abc") is None
    assert store.set("abc", D("2026-03-17")) is True
    assert store.get("abc") == D("2026-03-17")


def test_watermark_never_moves_backwards(tmp_path):
    store = LocalWatermarkStore(tmp_path / "wm.json")
    store.set("abc", D("2026-03-23"))
    assert store.set("abc", D("2026-03-20")) is False
    assert store.get("abc") == D("2026-03-23")


# ---------- storage ----------
def test_raw_key_layout():
    assert raw_key("events", "abc", "2026-03-17", "r1", 2) == (
        "raw/wistia/events/media_id=abc/run_date=2026-03-17/run_id=r1/page_0002.json"
    )


def test_local_storage_refuses_to_overwrite(tmp_path):
    storage = LocalStorage(tmp_path)
    storage.put_json("a/b.json", {"x": 1})
    with pytest.raises(FileExistsError):
        storage.put_json("a/b.json", {"x": 2})


# ---------- config ----------
def test_media_config_has_both_media_with_channels():
    media = {m.media_id: m.channel for m in load_media()}
    assert media == {"9k4tbcdfg0": "Facebook", "8hunphufxp": "YouTube"}


# ---------- one full run ----------
class FakeClient:
    """Stands in for WistiaClient: returns canned data, records what was asked."""

    def __init__(self, events_by_page):
        self.events_by_page = events_by_page
        self.calls = []

    def url(self, path):
        return f"https://api.test{path}"

    def media(self, media_id):
        self.calls.append(("media", media_id))
        return {"hashed_id": media_id}

    def media_stats_by_date(self, media_id, start, end):
        self.calls.append(("by_date", media_id, start, end))
        return [{"date": start, "load_count": 3, "play_count": 1}], {"start_date": start, "end_date": end}

    def event_pages(self, media_id, start, end):
        self.calls.append(("events", media_id, start, end))
        for i, rows in enumerate(self.events_by_page, start=1):
            yield i, rows, {"media_id": media_id, "page": i}


def test_run_ingestion_lands_every_response(tmp_path):
    client = FakeClient(events_by_page=[[{"e": 1}, {"e": 2}], [{"e": 3}]])
    storage = LocalStorage(tmp_path)
    watermarks = LocalWatermarkStore(tmp_path / "wm.json")
    watermarks.set("abc", D("2026-03-17"))

    result = run_ingestion(client, storage, watermarks, ["abc"], D("2026-03-18"), run_id="r1")

    assert result["media"][0]["events"] == 3
    assert result["media"][0]["event_pages"] == 2
    assert ("events", "abc", "2026-03-17", "2026-03-18") in client.calls  # lookback window used
    files = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("page_*.json"))
    assert files == [
        "raw/wistia/events/media_id=abc/run_date=2026-03-18/run_id=r1/page_0001.json",
        "raw/wistia/events/media_id=abc/run_date=2026-03-18/run_id=r1/page_0002.json",
        "raw/wistia/media_stats_by_date/media_id=abc/run_date=2026-03-18/run_id=r1/page_0001.json",
        "raw/wistia/medias/media_id=abc/run_date=2026-03-18/run_id=r1/page_0001.json",
    ]
    envelope = json.loads((tmp_path / files[0]).read_text())
    assert envelope["run_id"] == "r1"
    assert envelope["window_start"] == "2026-03-17" and envelope["window_end"] == "2026-03-18"
    assert envelope["data"] == [{"e": 1}, {"e": 2}]
    # run_ingestion itself must not move the watermark (the pipeline does it after the transform)
    assert watermarks.get("abc") == D("2026-03-17")

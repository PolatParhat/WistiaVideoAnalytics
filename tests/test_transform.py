"""Silver and gold transformations on small hand-made inputs (local PySpark, no Iceberg, no files)."""

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from wistia.transform import gold, silver
from wistia.transform.schemas import RAW

CHANNELS = {"m1": "Facebook", "m2": "YouTube"}


def event(key, visitor, at, media="m1", pct=0.5, url="https://site.com/", browser="Chrome", city="Austin"):
    return {
        "event_key": key, "visitor_key": visitor, "received_at": at, "media_id": media, "ip": "1.2.x.x",
        "country": "US", "region": "Texas", "city": city, "lat": 30.0, "lon": -97.0, "org": "ISP",
        "percent_viewed": pct, "embed_url": url,
        "user_agent_details": {"browser": browser, "browser_version": "1", "platform": "Mac", "mobile": False},
    }  # fmt: skip


def raw_events(spark, rows):
    return spark.createDataFrame([("r1", "2026-03-17", "2026-03-18", "now", rows)], RAW["events"])


def raw_media(spark, media_id="m1", duration=100.0):
    data = {
        "hashed_id": media_id, "id": 1, "name": "Video", "type": "Video", "duration": duration,
        "created": "2025-01-04T00:35:10+00:00", "updated": "2026-06-29T22:50:15+00:00",
        "project": {"name": "P"}, "share_link": {"url": "https://x/s/1"},
    }  # fmt: skip
    return spark.createDataFrame([("r1", "a", "b", "now", data)], RAW["medias"])


def raw_daily(spark, media_id, rows):
    df = spark.createDataFrame([("r1", "a", "b", "now", rows)], RAW["media_stats_by_date"])
    return df.withColumn("path_media_id", F.lit(media_id))


# ---------------- silver
@pytest.mark.parametrize(
    "url, expected",
    [
        ("https://site.com/?fbclid=abc", "facebook_ad"),
        ("https://site.com/?utm_source=meta&x=1", "facebook_ad"),
        ("https://site.com/?FBC_ID=1", "facebook_ad"),
        ("https://dataengineerinterviews.com/", "website"),
        ("", "direct"),
        (None, "direct"),
    ],
)
def test_traffic_source(spark, url, expected):
    df = spark.createDataFrame([(url,)], "embed_url string")
    assert df.select(silver.traffic_source(F.col("embed_url"))).first()[0] == expected


def test_silver_events_types_and_dedup(spark):
    rows = [
        event("k1", "v1", "2026-03-17T23:59:59.000Z"),
        event("k1", "v1", "2026-03-17T23:59:59.000Z"),  # exact duplicate (e.g. re-read by the lookback)
        event("k1", "v2", "2026-03-18T00:00:01.000Z"),  # same event_key, different event: must be kept
    ]
    out = silver.events(raw_events(spark, rows), "r1").orderBy("received_at").collect()
    assert len(out) == 2
    assert out[0].received_at == datetime(2026, 3, 17, 23, 59, 59)
    assert [r.event_date for r in out] == [date(2026, 3, 17), date(2026, 3, 18)]  # UTC date
    assert out[0].browser == "Chrome" and out[0].is_mobile is False
    assert out[0]._run_id == "r1"


def test_silver_media_adds_channel(spark):
    row = silver.media(raw_media(spark, "m2"), CHANNELS, "r1").first()
    assert (row.media_id, row.channel, row.title, row.url, row.project_name) == (
        "m2", "YouTube", "Video", "https://x/s/1", "P"
    )  # fmt: skip
    assert row.created_at == datetime(2025, 1, 4, 0, 35, 10)


def test_silver_media_unknown_channel_is_null(spark):
    assert silver.media(raw_media(spark, "zzz"), CHANNELS, "r1").first().channel is None


def test_silver_daily_takes_media_id_from_folder(spark):
    rows = [{"date": "2026-03-17", "load_count": 19, "play_count": 8, "hours_watched": 1.2}]
    out = silver.daily_stats(raw_daily(spark, "m1", rows), "r1").first()
    assert (out.media_id, out.date, out.load_count, out.play_count) == ("m1", date(2026, 3, 17), 19, 8)


def test_empty_day_gives_empty_silver(spark):
    assert silver.events(raw_events(spark, []), "r1").count() == 0


# ---------------- gold
@pytest.fixture
def s_events(spark):
    rows = [
        event("a", "v1", "2026-03-17T10:00:00Z", pct=0.2, url="https://s/?fbclid=1", city="Austin"),
        event("b", "v1", "2026-03-17T11:00:00Z", pct=0.8, url="https://s/", city="Austin"),
        event("c", "v1", "2026-03-18T09:00:00Z", pct=1.0, city="Dallas", browser="Safari"),
        event("d", "v2", "2026-03-17T12:00:00Z", pct=0.5, url=""),
    ]
    return silver.events(raw_events(spark, rows), "r1")


@pytest.fixture
def s_media(spark):
    return silver.media(raw_media(spark, "m1", duration=100.0), CHANNELS, "r1")


def test_fact_media_engagement(s_events, s_media):
    out = {(r.visitor_id, str(r.date)): r for r in gold.fact_media_engagement(s_events, s_media).collect()}
    assert len(out) == 3  # grain: media x visitor x date
    v1 = out[("v1", "2026-03-17")]
    assert v1.play_count == 2
    assert v1.total_watch_time_seconds == pytest.approx(100.0)  # (0.2 + 0.8) x 100 s
    assert v1.watched_percent == 0.8
    assert v1.traffic_source == "facebook_ad"  # from the first event of the day
    assert out[("v2", "2026-03-17")].traffic_source == "direct"


def test_dim_visitor_uses_latest_event(s_events):
    out = {r.visitor_id: r for r in gold.dim_visitor(s_events).collect()}
    assert set(out) == {"v1", "v2"}
    assert out["v1"].city == "Dallas" and out["v1"].browser == "Safari"  # latest event wins
    assert out["v1"].first_seen_at == datetime(2026, 3, 17, 10, 0)
    assert out["v1"].last_seen_at == datetime(2026, 3, 18, 9, 0)


def test_fact_media_daily_play_rate_and_visitors(spark, s_events):
    daily = spark.createDataFrame(
        [("m1", date(2026, 3, 17), 20, 5, 1.0), ("m1", date(2026, 3, 19), 0, 0, 0.0)],
        "media_id string, date date, load_count bigint, play_count bigint, hours_watched double",
    )
    out = {str(r.date): r for r in gold.fact_media_daily(daily, s_events).collect()}
    assert out["2026-03-17"].play_rate == 0.25
    assert out["2026-03-17"].unique_visitors == 2
    assert out["2026-03-19"].play_rate is None  # no loads: rate is unknown, not 0
    assert out["2026-03-19"].unique_visitors == 0


def test_dim_media_columns(s_media):
    assert gold.dim_media(s_media).columns == [
        "media_id", "title", "url", "channel", "project_name", "duration_seconds", "created_at", "updated_at"
    ]  # fmt: skip

"""raw -> silver: typed columns, snake_case names, one row per real-world thing. Pure functions (no I/O)."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

EVENT_KEY = ["event_key", "visitor_key", "received_at"]  # event_key alone is not unique (Step 1 finding 3)


def _audit(run_id: str) -> list[Column]:
    return [F.lit(run_id).alias("_run_id"), F.current_timestamp().alias("_loaded_at")]


def media(raw: DataFrame, channels: dict[str, str], run_id: str) -> DataFrame:
    """One row per media. channel comes from config/media.yaml (Wistia has no channel field)."""
    channel_map = F.create_map(*[F.lit(x) for pair in channels.items() for x in pair])
    d = raw.select("data.*")
    return d.select(
        F.col("hashed_id").alias("media_id"),
        F.col("id").alias("wistia_id"),
        F.col("name").alias("title"),
        F.col("type").alias("media_type"),
        F.col("share_link.url").alias("url"),
        F.col("project.name").alias("project_name"),
        channel_map[F.col("hashed_id")].alias("channel"),
        F.col("duration").alias("duration_seconds"),
        F.to_timestamp("created").alias("created_at"),
        F.to_timestamp("updated").alias("updated_at"),
        *_audit(run_id),
    ).dropDuplicates(["media_id"])


def daily_stats(raw: DataFrame, run_id: str) -> DataFrame:
    """One row per media per day. media_id comes from the file's folder (the rows don't carry it)."""
    rows = raw.select("path_media_id", F.explode("data").alias("r"))
    return rows.select(
        F.col("path_media_id").alias("media_id"),
        F.to_date("r.date").alias("date"),
        F.col("r.load_count").alias("load_count"),
        F.col("r.play_count").alias("play_count"),
        F.col("r.hours_watched").alias("hours_watched"),
        *_audit(run_id),
    ).dropDuplicates(["media_id", "date"])


def traffic_source(embed_url: Column) -> Column:
    """facebook_ad if the page URL has a Facebook/Meta click id, website for any other URL, direct if empty."""
    url = F.lower(F.coalesce(embed_url, F.lit("")))
    return (
        F.when(url == "", "direct").when(url.rlike("fbclid|fbc_id|utm_source=meta"), "facebook_ad").otherwise("website")
    )


def events(raw: DataFrame, run_id: str) -> DataFrame:
    """One row per viewing session, deduplicated on the natural key."""
    e = raw.select(F.explode("data").alias("e")).select("e.*")
    received = F.to_timestamp("received_at")
    return e.select(
        "event_key",
        "visitor_key",
        received.alias("received_at"),
        F.to_date(received).alias("event_date"),  # business date = UTC date (session time zone is UTC)
        "media_id",
        "ip",
        "country",
        "region",
        "city",
        "lat",
        "lon",
        "org",
        "percent_viewed",
        "embed_url",
        traffic_source(F.col("embed_url")).alias("traffic_source"),
        F.col("user_agent_details.browser").alias("browser"),
        F.col("user_agent_details.browser_version").alias("browser_version"),
        F.col("user_agent_details.platform").alias("platform"),
        F.col("user_agent_details.mobile").alias("is_mobile"),
        *_audit(run_id),
    ).dropDuplicates(EVENT_KEY)

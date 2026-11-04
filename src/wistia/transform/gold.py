"""silver -> gold star schema. Pure functions (no I/O)."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def dim_media(media: DataFrame) -> DataFrame:
    """1 row per media, latest values (SCD type 1)."""
    return media.select(
        "media_id", "title", "url", "channel", "project_name", "duration_seconds", "created_at", "updated_at"
    )


def dim_visitor(events: DataFrame) -> DataFrame:
    """1 row per visitor: location/browser from the latest event; first/last seen over all their events."""
    latest = Window.partitionBy("visitor_key").orderBy(F.col("received_at").desc(), F.col("event_key").desc())
    span = Window.partitionBy("visitor_key")
    return (
        events.withColumn("rn", F.row_number().over(latest))
        .withColumn("first_seen_at", F.min("received_at").over(span))
        .withColumn("last_seen_at", F.max("received_at").over(span))
        .where("rn = 1")
        .select(
            F.col("visitor_key").alias("visitor_id"),
            F.col("ip").alias("ip_address"),
            "country",
            "region",
            "city",
            "browser",
            "platform",
            "is_mobile",
            "first_seen_at",
            "last_seen_at",
        )
    )


def fact_media_engagement(events: DataFrame, media: DataFrame) -> DataFrame:
    """media x visitor x UTC date.

    play_count               = number of events (viewing sessions)
    total_watch_time_seconds = sum of percent_viewed x video duration
    watched_percent          = highest percent_viewed that day
    traffic_source           = from the visitor's first event that day
    """
    first = Window.partitionBy("media_id", "visitor_key", "event_date").orderBy("received_at", "event_key")
    e = events.join(media.select("media_id", "duration_seconds"), "media_id", "left").withColumn(
        "first_source", F.first("traffic_source").over(first)
    )
    return e.groupBy("media_id", F.col("visitor_key").alias("visitor_id"), F.col("event_date").alias("date")).agg(
        F.count("*").alias("play_count"),
        F.round(F.sum(F.col("percent_viewed") * F.col("duration_seconds")), 3).alias("total_watch_time_seconds"),
        F.max("percent_viewed").alias("watched_percent"),
        F.first("first_source").alias("traffic_source"),
    )


def fact_media_daily(daily: DataFrame, events: DataFrame) -> DataFrame:
    """media x UTC date from Wistia's daily stats. play_rate = plays / loads (null when there were no loads)."""
    visitors = events.groupBy("media_id", F.col("event_date").alias("date")).agg(
        F.countDistinct("visitor_key").alias("unique_visitors")
    )
    return daily.join(visitors, ["media_id", "date"], "left").select(
        "media_id",
        "date",
        "load_count",
        "play_count",
        F.when(F.col("load_count") > 0, F.round(F.col("play_count") / F.col("load_count"), 4)).alias("play_rate"),
        "hours_watched",
        F.coalesce("unique_visitors", F.lit(0)).cast("bigint").alias("unique_visitors"),
    )

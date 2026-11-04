"""Explicit schemas for the raw files. No guessing, and a day without data still has the right columns."""

from pyspark.sql import types as T


def envelope(data_type):
    """Every raw file is {"run_id", "window_start", "window_end", "fetched_at", "data": <API response>}."""
    return T.StructType(
        [
            T.StructField("run_id", T.StringType()),
            T.StructField("window_start", T.StringType()),
            T.StructField("window_end", T.StringType()),
            T.StructField("fetched_at", T.StringType()),
            T.StructField("data", data_type),
        ]
    )


MEDIA = T.StructType(
    [
        T.StructField("hashed_id", T.StringType()),
        T.StructField("id", T.LongType()),
        T.StructField("name", T.StringType()),
        T.StructField("type", T.StringType()),
        T.StructField("duration", T.DoubleType()),
        T.StructField("created", T.StringType()),
        T.StructField("updated", T.StringType()),
        T.StructField("project", T.StructType([T.StructField("name", T.StringType())])),
        T.StructField("share_link", T.StructType([T.StructField("url", T.StringType())])),
    ]
)

DAILY_STAT = T.StructType(
    [
        T.StructField("date", T.StringType()),
        T.StructField("load_count", T.LongType()),
        T.StructField("play_count", T.LongType()),
        T.StructField("hours_watched", T.DoubleType()),
    ]
)

EVENT = T.StructType(
    [
        T.StructField("event_key", T.StringType()),
        T.StructField("visitor_key", T.StringType()),
        T.StructField("received_at", T.StringType()),
        T.StructField("media_id", T.StringType()),
        T.StructField("ip", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("region", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("org", T.StringType()),
        T.StructField("percent_viewed", T.DoubleType()),
        T.StructField("embed_url", T.StringType()),
        T.StructField(
            "user_agent_details",
            T.StructType(
                [
                    T.StructField("browser", T.StringType()),
                    T.StructField("browser_version", T.StringType()),
                    T.StructField("platform", T.StringType()),
                    T.StructField("mobile", T.BooleanType()),
                ]
            ),
        ),
    ]
)

# endpoint folder name -> schema of its raw files
RAW = {
    "medias": envelope(MEDIA),
    "media_stats_by_date": envelope(T.ArrayType(DAILY_STAT)),
    "events": envelope(T.ArrayType(EVENT)),
}

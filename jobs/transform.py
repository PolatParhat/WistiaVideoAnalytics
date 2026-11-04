"""Stage 3.6: one PySpark file that turns one ingestion run into silver and gold Iceberg tables.

raw (JSON files of one run_id)
  -> silver.media, silver.media_daily_stats, silver.events   (typed, deduplicated, MERGE upsert)
  -> gold.dim_media, gold.dim_visitor,
     gold.fact_media_engagement, gold.fact_media_daily      (star schema, MERGE upsert)
  -> ops.pipeline_runs                                       (one row per run: the 7-day evidence)

MERGE (update if the key exists, insert if not) makes a rerun of the same day give the same result.

Run locally inside the AWS Glue Docker image:
  scripts/spark_local.sh jobs/transform.py --run-date 2026-03-18 --media-config "$(uv run python -m wistia.config)"
"""

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException

CATALOG = "lake"  # Spark catalog name; the same in local and AWS so the SQL is identical
SILVER, GOLD, OPS = f"{CATALOG}.wistia_silver", f"{CATALOG}.wistia_gold", f"{CATALOG}.wistia_ops"


# ---------------------------------------------------------------- raw schemas
# Explicit schemas: no guessing, and an empty day still has the right columns.
def envelope(data_type):
    return T.StructType(
        [
            T.StructField("run_id", T.StringType()),
            T.StructField("window_start", T.StringType()),
            T.StructField("window_end", T.StringType()),
            T.StructField("fetched_at", T.StringType()),
            T.StructField("data", data_type),
        ]
    )


MEDIA_SCHEMA = envelope(
    T.StructType(
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
)

BY_DATE_SCHEMA = envelope(
    T.ArrayType(
        T.StructType(
            [
                T.StructField("date", T.StringType()),
                T.StructField("load_count", T.LongType()),
                T.StructField("play_count", T.LongType()),
                T.StructField("hours_watched", T.DoubleType()),
            ]
        )
    )
)

EVENTS_SCHEMA = envelope(
    T.ArrayType(
        T.StructType(
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
    )
)


# ---------------------------------------------------------------- table definitions
TABLES = {
    f"{SILVER}.media": """
        media_id string, wistia_id bigint, title string, media_type string, url string,
        project_name string, channel string, duration_seconds double,
        created_at timestamp, updated_at timestamp, _run_id string, _loaded_at timestamp""",
    f"{SILVER}.media_daily_stats": """
        media_id string, date date, load_count bigint, play_count bigint, hours_watched double,
        _run_id string, _loaded_at timestamp""",
    f"{SILVER}.events": """
        event_key string, visitor_key string, received_at timestamp, event_date date, media_id string,
        ip string, country string, region string, city string, lat double, lon double, org string,
        percent_viewed double, embed_url string, traffic_source string,
        browser string, browser_version string, platform string, is_mobile boolean,
        _run_id string, _loaded_at timestamp""",
    f"{GOLD}.dim_media": """
        media_id string, title string, url string, channel string, project_name string,
        duration_seconds double, created_at timestamp, updated_at timestamp""",
    f"{GOLD}.dim_visitor": """
        visitor_id string, ip_address string, country string, region string, city string,
        browser string, platform string, is_mobile boolean,
        first_seen_at timestamp, last_seen_at timestamp""",
    f"{GOLD}.fact_media_engagement": """
        media_id string, visitor_id string, date date, play_count bigint,
        total_watch_time_seconds double, watched_percent double, traffic_source string""",
    f"{GOLD}.fact_media_daily": """
        media_id string, date date, load_count bigint, play_count bigint, play_rate double,
        hours_watched double, unique_visitors bigint""",
    f"{OPS}.pipeline_runs": """
        run_id string, run_date date, window_start date, window_end date, status string,
        events_read bigint, events_new bigint, media_rows bigint, daily_stats_rows bigint,
        silver_events_total bigint, dim_visitor_total bigint, fact_engagement_total bigint,
        fact_daily_total bigint, started_at timestamp, finished_at timestamp,
        duration_seconds double, error string""",
}

KEYS = {
    f"{SILVER}.media": ["media_id"],
    f"{SILVER}.media_daily_stats": ["media_id", "date"],
    f"{SILVER}.events": ["event_key", "visitor_key", "received_at"],
    f"{GOLD}.dim_media": ["media_id"],
    f"{GOLD}.dim_visitor": ["visitor_id"],
    f"{GOLD}.fact_media_engagement": ["media_id", "visitor_id", "date"],
    f"{GOLD}.fact_media_daily": ["media_id", "date"],
    f"{OPS}.pipeline_runs": ["run_id"],
}


def create_tables(spark):
    for ns in (SILVER, GOLD, OPS):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
    for name, columns in TABLES.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {name} ({columns}) USING iceberg")


def merge(spark, df, table):
    """Upsert df into table on its natural key: update matching rows, insert new ones."""
    view = "src_" + table.replace(".", "_")
    # localCheckpoint computes df once and keeps the result: MERGE needs a fixed source
    # (it rejects non-deterministic parts such as input_file_name() in the plan).
    df.localCheckpoint().createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} = s.{k}" for k in KEYS[table])
    spark.sql(
        f"MERGE INTO {table} t USING {view} s ON {on} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )


# ---------------------------------------------------------------- read raw
def read_raw(spark, lake, endpoint, run_date, run_id, schema):
    """Read one endpoint's files of this run. A missing folder (e.g. a day with no events) is empty."""
    path = f"{lake}/raw/wistia/{endpoint}/media_id=*/run_date={run_date}/run_id={run_id}/*.json"
    try:
        df = spark.read.schema(schema).option("multiLine", True).json(path)
    except AnalysisException as error:
        if "PATH_NOT_FOUND" in str(error) or "Path does not exist" in str(error):
            return spark.createDataFrame([], schema).withColumn("path_media_id", F.lit(None).cast("string"))
        raise
    # media_id comes from the folder name (by_date rows don't carry it)
    return df.withColumn("path_media_id", F.regexp_extract(F.input_file_name(), r"media_id=([^/]+)/", 1))


# ---------------------------------------------------------------- silver
def to_ts(col):
    return F.to_timestamp(col)  # ISO-8601 with offset/Z; session time zone is UTC


def silver_media(raw, channels, run_id):
    channel_map = F.create_map(*[x for m, c in channels.items() for x in (F.lit(m), F.lit(c))])
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
        to_ts("created").alias("created_at"),
        to_ts("updated").alias("updated_at"),
        F.lit(run_id).alias("_run_id"),
        F.current_timestamp().alias("_loaded_at"),
    ).dropDuplicates(["media_id"])


def silver_daily(raw, run_id):
    rows = raw.select("path_media_id", F.explode("data").alias("r"))
    return rows.select(
        F.col("path_media_id").alias("media_id"),
        F.to_date("r.date").alias("date"),
        F.col("r.load_count").alias("load_count"),
        F.col("r.play_count").alias("play_count"),
        F.col("r.hours_watched").alias("hours_watched"),
        F.lit(run_id).alias("_run_id"),
        F.current_timestamp().alias("_loaded_at"),
    ).dropDuplicates(["media_id", "date"])


def traffic_source(embed_url):
    """facebook_ad if the page URL has a Facebook/Meta click id, website for any other URL, direct if empty."""
    url = F.lower(F.coalesce(embed_url, F.lit("")))
    return (
        F.when(url == "", "direct").when(url.rlike("fbclid|fbc_id|utm_source=meta"), "facebook_ad").otherwise("website")
    )


def silver_events(raw, run_id):
    e = raw.select(F.explode("data").alias("e")).select("e.*")
    received = to_ts("received_at")
    return e.select(
        "event_key",
        "visitor_key",
        received.alias("received_at"),
        F.to_date(received).alias("event_date"),
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
        F.lit(run_id).alias("_run_id"),
        F.current_timestamp().alias("_loaded_at"),
    ).dropDuplicates(["event_key", "visitor_key", "received_at"])  # event_key alone is not unique


# ---------------------------------------------------------------- gold
def gold_dim_media(media):
    return media.select(
        "media_id", "title", "url", "channel", "project_name", "duration_seconds", "created_at", "updated_at"
    )


def gold_dim_visitor(events):
    """One row per visitor: location/browser from the latest event, first/last seen over all events."""
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


def gold_fact_engagement(events, media):
    """media x visitor x date. Watch time = percent viewed x video length."""
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


def gold_fact_daily(daily, events):
    """media x date from Wistia's daily stats; unique visitors counted from the events."""
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


# ---------------------------------------------------------------- the run
def latest_run_id(lake, run_date):
    """Locally: the newest run_id folder for this run_date (run ids start with a UTC timestamp)."""
    ids = {p.name.split("=", 1)[1] for p in Path(lake).glob(f"raw/wistia/medias/*/run_date={run_date}/run_id=*")}
    if not ids:
        raise SystemExit(f"no raw data for run_date={run_date} under {lake}/raw; run jobs/ingest.py first")
    return max(ids)


def build_spark(lake):
    warehouse = str(Path(lake).resolve() / "warehouse")
    return (
        SparkSession.builder.appName("wistia-transform")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def count(spark, table):
    return spark.table(table).count()


def run(spark, lake, run_date, run_id, channels):
    started, t0 = datetime.now(UTC), time.monotonic()
    create_tables(spark)

    # silver from this run's raw files
    media = silver_media(read_raw(spark, lake, "medias", run_date, run_id, MEDIA_SCHEMA), channels, run_id)
    daily = silver_daily(read_raw(spark, lake, "media_stats_by_date", run_date, run_id, BY_DATE_SCHEMA), run_id)
    raw_events = read_raw(spark, lake, "events", run_date, run_id, EVENTS_SCHEMA)
    events = silver_events(raw_events, run_id).cache()
    events_read = events.count()

    before = count(spark, f"{SILVER}.events")
    merge(spark, media, f"{SILVER}.media")
    merge(spark, daily, f"{SILVER}.media_daily_stats")
    merge(spark, events, f"{SILVER}.events")
    events_new = count(spark, f"{SILVER}.events") - before

    # gold: recompute only what this run touched, from all of silver (so late events are included)
    s_media = spark.table(f"{SILVER}.media")
    s_daily = spark.table(f"{SILVER}.media_daily_stats")
    s_events = spark.table(f"{SILVER}.events")
    touched_days = daily.select("media_id", "date")  # every day in the window, zeros included
    touched_visitors = events.select("visitor_key").distinct()

    merge(spark, gold_dim_media(s_media), f"{GOLD}.dim_media")
    merge(spark, gold_dim_visitor(s_events.join(touched_visitors, "visitor_key")), f"{GOLD}.dim_visitor")
    day_events = s_events.join(touched_days.withColumnRenamed("date", "event_date"), ["media_id", "event_date"])
    merge(spark, gold_fact_engagement(day_events, s_media), f"{GOLD}.fact_media_engagement")
    merge(
        spark,
        gold_fact_daily(s_daily.join(touched_days, ["media_id", "date"]), day_events),
        f"{GOLD}.fact_media_daily",
    )

    window = daily.agg(F.min("date").alias("s"), F.max("date").alias("e")).first()
    summary = {
        "run_id": run_id,
        "run_date": run_date,
        "window_start": str(window["s"]) if window["s"] else None,
        "window_end": str(window["e"]) if window["e"] else None,
        "status": "succeeded",
        "events_read": events_read,
        "events_new": events_new,
        "media_rows": media.count(),
        "daily_stats_rows": daily.count(),
        "silver_events_total": count(spark, f"{SILVER}.events"),
        "dim_visitor_total": count(spark, f"{GOLD}.dim_visitor"),
        "fact_engagement_total": count(spark, f"{GOLD}.fact_media_engagement"),
        "fact_daily_total": count(spark, f"{GOLD}.fact_media_daily"),
        "started_at": started,
        "finished_at": datetime.now(UTC),
        "duration_seconds": round(time.monotonic() - t0, 1),
        "error": None,
    }
    write_run(spark, summary)
    return summary


def write_run(spark, summary):
    schema = spark.table(f"{OPS}.pipeline_runs").schema
    row = {**summary}
    for k in ("run_date", "window_start", "window_end"):
        row[k] = datetime.fromisoformat(row[k]).date() if row[k] else None
    merge(spark, spark.createDataFrame([tuple(row[f.name] for f in schema)], schema), f"{OPS}.pipeline_runs")


def main(argv=None):
    parser = argparse.ArgumentParser(description="raw -> silver -> gold for one run")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--run-id", help="ingestion run id (default: newest for run-date)")
    parser.add_argument("--lake", default="lake")
    parser.add_argument("--media-config", required=True, help='JSON: [{"media_id":..,"channel":..}]')
    args = parser.parse_args(argv)

    channels = {m["media_id"]: m["channel"] for m in json.loads(args.media_config)}
    run_id = args.run_id or latest_run_id(args.lake, args.run_date)
    spark = build_spark(args.lake)
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary = run(spark, args.lake, args.run_date, run_id, channels)
    except Exception as error:
        write_run(
            spark,
            {
                **{f.name: None for f in spark.table(f"{OPS}.pipeline_runs").schema},
                "run_id": run_id,
                "run_date": args.run_date,
                "status": "failed",
                "finished_at": datetime.now(UTC),
                "error": str(error)[:2000],
            },
        )
        raise
    print(json.dumps(summary, default=str))


if __name__ == "__main__":
    main()

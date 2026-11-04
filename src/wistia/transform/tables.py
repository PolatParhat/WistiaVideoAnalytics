"""Iceberg tables: names, columns, natural keys, CREATE and MERGE."""

from pyspark.sql import DataFrame, SparkSession

CATALOG = "lake"  # Spark catalog name, the same locally and in AWS so all SQL is identical
SILVER, GOLD, OPS = "wistia_silver", "wistia_gold", "wistia_ops"


def fq(namespace: str, table: str) -> str:
    """Fully qualified name: lake.wistia_gold.dim_media"""
    return f"{CATALOG}.{namespace}.{table}"


# name -> (columns, natural key)
TABLES = {
    fq(SILVER, "media"): (
        """media_id string, wistia_id bigint, title string, media_type string, url string,
        project_name string, channel string, duration_seconds double,
        created_at timestamp, updated_at timestamp, _run_id string, _loaded_at timestamp""",
        ["media_id"],
    ),
    fq(SILVER, "media_daily_stats"): (
        """media_id string, date date, load_count bigint, play_count bigint, hours_watched double,
        _run_id string, _loaded_at timestamp""",
        ["media_id", "date"],
    ),
    fq(SILVER, "events"): (
        """event_key string, visitor_key string, received_at timestamp, event_date date, media_id string,
        ip string, country string, region string, city string, lat double, lon double, org string,
        percent_viewed double, embed_url string, traffic_source string,
        browser string, browser_version string, platform string, is_mobile boolean,
        _run_id string, _loaded_at timestamp""",
        ["event_key", "visitor_key", "received_at"],
    ),
    fq(GOLD, "dim_media"): (
        """media_id string, title string, url string, channel string, project_name string,
        duration_seconds double, created_at timestamp, updated_at timestamp""",
        ["media_id"],
    ),
    fq(GOLD, "dim_visitor"): (
        """visitor_id string, ip_address string, country string, region string, city string,
        browser string, platform string, is_mobile boolean, first_seen_at timestamp, last_seen_at timestamp""",
        ["visitor_id"],
    ),
    fq(GOLD, "fact_media_engagement"): (
        """media_id string, visitor_id string, date date, play_count bigint,
        total_watch_time_seconds double, watched_percent double, traffic_source string""",
        ["media_id", "visitor_id", "date"],
    ),
    fq(GOLD, "fact_media_daily"): (
        """media_id string, date date, load_count bigint, play_count bigint, play_rate double,
        hours_watched double, unique_visitors bigint""",
        ["media_id", "date"],
    ),
    fq(OPS, "pipeline_runs"): (
        """run_id string, run_date date, window_start date, window_end date, status string,
        events_read bigint, events_new bigint, media_rows bigint, daily_stats_rows bigint,
        silver_events_total bigint, dim_visitor_total bigint, fact_engagement_total bigint,
        fact_daily_total bigint, dq_failed_critical bigint, dq_failed_warning bigint,
        started_at timestamp, finished_at timestamp, duration_seconds double, error string""",
        ["run_id"],
    ),
    fq(OPS, "dq_results"): (
        """run_id string, run_date date, check_name string, table_name string, severity string,
        passed boolean, failed_rows bigint, details string, checked_at timestamp""",
        ["run_id", "check_name"],
    ),
}


def create_tables(spark: SparkSession, location: str | None = None) -> None:
    """Create namespaces and tables if they don't exist (safe to call on every run)."""
    for ns in (SILVER, GOLD, OPS):
        loc = f" LOCATION '{location}/{ns}.db'" if location else ""
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}{loc}")
    for name, (columns, _) in TABLES.items():
        spark.sql(f"CREATE TABLE IF NOT EXISTS {name} ({columns}) USING iceberg TBLPROPERTIES ('format-version'='2')")


def merge(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Upsert df into table on its natural key: update rows whose key exists, insert the rest.

    This is what makes a rerun idempotent: loading the same rows twice leaves one copy.
    """
    view = "src_" + table.replace(".", "_")
    # localCheckpoint computes df once and keeps the result: MERGE needs a fixed source
    # (it rejects non-deterministic parts such as input_file_name() in the plan).
    df.localCheckpoint().createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} = s.{k}" for k in TABLES[table][1])
    spark.sql(
        f"MERGE INTO {table} t USING {view} s ON {on} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )

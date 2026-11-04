"""One transform run: raw files of one ingestion run -> silver -> gold, then data-quality checks.

Used by the local job (jobs/transform.py) and the AWS Glue job (jobs/glue_transform.py).
`raw_root` is a folder ("lake") locally and a bucket ("s3://...") in AWS; everything else is the same.
"""

import logging
import time
from dataclasses import asdict
from datetime import UTC, date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from wistia.logs import get_logger, log_event
from wistia.transform import gold, quality, silver
from wistia.transform.schemas import RAW
from wistia.transform.tables import GOLD, OPS, SILVER, create_tables, fq, merge

log = get_logger("wistia.transform")


class DataQualityError(RuntimeError):
    """A critical data-quality check failed."""


def read_raw(spark: SparkSession, raw_root: str, endpoint: str, run_date: str, run_id: str) -> DataFrame:
    """Read one endpoint's raw files of this run. No files (e.g. a day without events) -> empty DataFrame."""
    path = f"{raw_root}/raw/wistia/{endpoint}/media_id=*/run_date={run_date}/run_id={run_id}/*.json"
    schema = RAW[endpoint]
    try:
        df = spark.read.schema(schema).option("multiLine", True).json(path)
    except AnalysisException as error:
        if "PATH_NOT_FOUND" not in str(error) and "Path does not exist" not in str(error):
            raise
        df = spark.createDataFrame([], schema)
        return df.withColumn("path_media_id", F.lit(None).cast("string"))
    # the media id is in the folder name; by_date rows don't carry it
    return df.withColumn("path_media_id", F.regexp_extract(F.input_file_name(), r"media_id=([^/]+)/", 1))


def run(spark, raw_root, run_date, run_id, channels, warehouse=None):
    """Transform one ingestion run. Returns the summary row written to ops.pipeline_runs."""
    started, t0 = datetime.now(UTC), time.monotonic()
    log_event(log, "transform started", run_id=run_id, run_date=run_date)
    create_tables(spark, warehouse)

    # ---- silver from this run's raw files
    raw_daily = read_raw(spark, raw_root, "media_stats_by_date", run_date, run_id)
    windows = {
        r.path_media_id: (date.fromisoformat(r.window_start), date.fromisoformat(r.window_end))
        for r in raw_daily.select("path_media_id", "window_start", "window_end").distinct().collect()
    }
    media = silver.media(read_raw(spark, raw_root, "medias", run_date, run_id), channels, run_id)
    daily = silver.daily_stats(raw_daily, run_id).localCheckpoint()
    events = silver.events(read_raw(spark, raw_root, "events", run_date, run_id), run_id).localCheckpoint()

    before = spark.table(fq(SILVER, "events")).count()
    merge(spark, media, fq(SILVER, "media"))
    merge(spark, daily, fq(SILVER, "media_daily_stats"))
    merge(spark, events, fq(SILVER, "events"))
    s_media = spark.table(fq(SILVER, "media"))
    s_daily = spark.table(fq(SILVER, "media_daily_stats"))
    s_events = spark.table(fq(SILVER, "events"))
    events_new = s_events.count() - before

    # ---- gold: rebuild only the keys this run touched, from all of silver (so late events count too)
    touched_days = daily.select("media_id", F.col("date").alias("event_date"))
    touched_visitors = events.select("visitor_key").distinct()
    day_events = s_events.join(touched_days, ["media_id", "event_date"])

    merge(spark, gold.dim_media(s_media), fq(GOLD, "dim_media"))
    merge(spark, gold.dim_visitor(s_events.join(touched_visitors, "visitor_key")), fq(GOLD, "dim_visitor"))
    merge(spark, gold.fact_media_engagement(day_events, s_media), fq(GOLD, "fact_media_engagement"))
    run_daily = s_daily.join(touched_days.withColumnRenamed("event_date", "date"), ["media_id", "date"])
    merge(spark, gold.fact_media_daily(run_daily, day_events), fq(GOLD, "fact_media_daily"))

    # ---- data quality
    g = {t: spark.table(fq(GOLD, t)) for t in ("dim_media", "dim_visitor", "fact_media_engagement", "fact_media_daily")}
    results = quality.run_checks(
        events=s_events,
        run_daily=daily,
        dim_media=g["dim_media"],
        dim_visitor=g["dim_visitor"],
        fact_engagement=g["fact_media_engagement"],
        fact_daily=g["fact_media_daily"],
        media_ids=list(channels),
        windows=windows,
    )
    write_dq_results(spark, results, run_id, run_date)
    failed = [r for r in results if not r.passed]
    for r in failed:
        log_event(log, "dq check failed", level=logging.WARNING, run_id=run_id, **asdict(r))
    critical = [r for r in failed if r.severity == "critical"]

    summary = {
        "run_id": run_id,
        "run_date": run_date,
        "window_start": min((w[0] for w in windows.values()), default=None),
        "window_end": max((w[1] for w in windows.values()), default=None),
        "status": "failed" if critical else "succeeded",
        "events_read": events.count(),
        "events_new": events_new,
        "media_rows": media.count(),
        "daily_stats_rows": daily.count(),
        "silver_events_total": s_events.count(),
        "dim_visitor_total": g["dim_visitor"].count(),
        "fact_engagement_total": g["fact_media_engagement"].count(),
        "fact_daily_total": g["fact_media_daily"].count(),
        "dq_failed_critical": len(critical),
        "dq_failed_warning": len(failed) - len(critical),
        "started_at": started,
        "finished_at": datetime.now(UTC),
        "duration_seconds": round(time.monotonic() - t0, 1),
        "error": "; ".join(f"{r.check_name}: {r.details}" for r in critical) or None,
    }
    write_run(spark, summary)
    log_event(log, "transform finished", **summary)
    if critical:
        raise DataQualityError(summary["error"])
    return summary


def write_dq_results(spark, results, run_id, run_date):
    schema = spark.table(fq(OPS, "dq_results")).schema
    now = datetime.now(UTC)
    rows = [
        (
            run_id,
            date.fromisoformat(run_date),
            r.check_name,
            r.table_name,
            r.severity,
            r.passed,
            r.failed_rows,
            r.details,
            now,
        )
        for r in results
    ]
    merge(spark, spark.createDataFrame(rows, schema), fq(OPS, "dq_results"))


def write_run(spark, summary):
    """Upsert one row into ops.pipeline_runs (missing fields become null)."""
    schema = spark.table(fq(OPS, "pipeline_runs")).schema
    row = dict(summary)
    if isinstance(row.get("run_date"), str):
        row["run_date"] = date.fromisoformat(row["run_date"])
    merge(spark, spark.createDataFrame([tuple(row.get(f.name) for f in schema)], schema), fq(OPS, "pipeline_runs"))


def record_failure(spark, run_id, run_date, error):
    """Called when a run crashes before it could write its own summary."""
    try:
        write_run(
            spark,
            {
                "run_id": run_id,
                "run_date": run_date,
                "status": "failed",
                "finished_at": datetime.now(UTC),
                "error": str(error)[:2000],
            },
        )
    except Exception as nested:  # never hide the original error
        log_event(log, "could not record failure", level=logging.ERROR, error=str(nested))

"""Data-quality checks. Each check looks at DataFrames and returns a CheckResult; nothing is written here.

severity "critical": a failure stops the run (and the watermark does not move).
severity "warning":  recorded in ops.dq_results for follow-up, the run continues.
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    check_name: str
    table_name: str
    severity: str
    passed: bool
    failed_rows: int
    details: str


def _result(name, table, severity, failed, details=""):
    return CheckResult(name, table, severity, failed == 0, int(failed), details)


def not_null(df: DataFrame, table: str, columns: list[str], severity="critical") -> CheckResult:
    cond = " OR ".join(f"{c} IS NULL" for c in columns)
    return _result(f"not_null:{table}", table, severity, df.where(cond).count(), ",".join(columns))


def unique(df: DataFrame, table: str, key: list[str], severity="critical") -> CheckResult:
    dupes = df.groupBy(*key).count().where("count > 1").count()
    return _result(f"unique:{table}", table, severity, dupes, ",".join(key))


def in_range(df: DataFrame, table: str, column: str, low: float, high: float, severity="critical") -> CheckResult:
    bad = df.where(F.col(column).isNotNull() & ~F.col(column).between(low, high)).count()
    return _result(f"range:{table}.{column}", table, severity, bad, f"{low}..{high}")


def references(child: DataFrame, table: str, column: str, parent: DataFrame, parent_column: str) -> CheckResult:
    """Every child.column value exists in parent.parent_column (no orphan fact rows)."""
    orphans = (
        child.select(column).distinct().join(parent.select(F.col(parent_column).alias(column)), column, "left_anti")
    )
    return _result(f"fk:{table}.{column}", table, "critical", orphans.count(), f"-> {parent_column}")


def all_media_present(dim_media: DataFrame, expected: list[str]) -> CheckResult:
    found = {r.media_id for r in dim_media.select("media_id").collect()}
    missing = sorted(set(expected) - found)
    return _result("present:dim_media", "dim_media", "critical", len(missing), ",".join(missing))


def one_row_per_day(daily: DataFrame, windows: dict) -> CheckResult:
    """Wistia returns one by_date row per day (zeros included): each media has every day of its window.

    windows: {media_id: (start_date, end_date)} as requested by the ingestion.
    """
    counts = {r.media_id: r["count"] for r in daily.groupBy("media_id").count().collect()}
    problems = []
    for media_id, (start, end) in sorted(windows.items()):
        expected, actual = (end - start).days + 1, counts.get(media_id, 0)
        if expected != actual:
            problems.append(f"{media_id}: expected {expected}, got {actual}")
    return _result("row_count:media_daily_stats", "media_daily_stats", "critical", len(problems), "; ".join(problems))


def plays_match_events(daily: DataFrame, events: DataFrame) -> CheckResult:
    """Wistia's daily play_count vs the number of events we loaded. Small differences are normal (warning)."""
    counted = events.groupBy("media_id", F.col("event_date").alias("date")).agg(F.count("*").alias("events"))
    diff = (
        daily.join(counted, ["media_id", "date"], "left")
        .withColumn("events", F.coalesce("events", F.lit(0)))
        .where(F.col("play_count") != F.col("events"))
    )
    rows = diff.select("media_id", "date", "play_count", "events").orderBy("media_id", "date").collect()
    details = "; ".join(f"{r.media_id} {r.date}: plays={r.play_count} events={r.events}" for r in rows[:10])
    return _result("reconcile:plays_vs_events", "fact_media_daily", "warning", len(rows), details)


def run_checks(*, events, run_daily, dim_media, dim_visitor, fact_engagement, fact_daily, media_ids, windows):
    """All checks for one run.

    run_daily is this run's daily stats; the other DataFrames are full silver/gold tables (they are small).
    """
    return [
        not_null(events, "silver.events", ["event_key", "visitor_key", "received_at", "media_id"]),
        unique(events, "silver.events", ["event_key", "visitor_key", "received_at"]),
        in_range(events, "silver.events", "percent_viewed", 0.0, 1.0),
        one_row_per_day(run_daily, windows),
        all_media_present(dim_media, media_ids),
        not_null(dim_media, "dim_media", ["media_id", "channel"]),
        unique(dim_visitor, "dim_visitor", ["visitor_id"]),
        unique(fact_engagement, "fact_media_engagement", ["media_id", "visitor_id", "date"]),
        references(fact_engagement, "fact_media_engagement", "media_id", dim_media, "media_id"),
        references(fact_engagement, "fact_media_engagement", "visitor_id", dim_visitor, "visitor_id"),
        unique(fact_daily, "fact_media_daily", ["media_id", "date"]),
        in_range(fact_daily, "fact_media_daily", "play_rate", 0.0, 1.0, severity="warning"),
        plays_match_events(run_daily, events),
    ]

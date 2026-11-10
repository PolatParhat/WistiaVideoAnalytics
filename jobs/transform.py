"""Local transform job: one ingestion run -> silver and gold Iceberg tables in lake/warehouse.

The logic lives in the library (src/wistia/transform/); this script builds a local Spark session and calls it.

Run inside the AWS Glue Docker image (same Spark/Iceberg as AWS):
  scripts/spark_local.sh jobs/transform.py --run-date 2026-03-18 --media-config "$(uv run python -m wistia.config)"
"""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from wistia.transform.pipeline import DataQualityError, record_failure, run
from wistia.transform.tables import CATALOG


def latest_run_id(lake, run_date):
    """The newest ingestion run for this run_date (run ids start with a UTC timestamp, so max = newest)."""
    ids = {p.name.split("=", 1)[1] for p in Path(lake).glob(f"raw/wistia/medias/*/run_date={run_date}/run_id=*")}
    if not ids:
        raise SystemExit(f"no raw data for run_date={run_date} under {lake}/raw; run jobs/ingest.py first")
    return max(ids)


def build_spark(lake):
    """Local Spark with an Iceberg catalog stored in plain folders (lake/warehouse)."""
    return (
        SparkSession.builder.appName("wistia-transform")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", str(Path(lake).resolve() / "warehouse"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description="raw -> silver -> gold for one ingestion run")
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--run-id", help="ingestion run id (default: newest for run-date)")
    parser.add_argument("--lake", default="lake")
    parser.add_argument("--media-config", required=True, help='JSON: [{"media_id": "...", "channel": "..."}]')
    args = parser.parse_args(argv)

    channels = {m["media_id"]: m["channel"] for m in json.loads(args.media_config)}
    run_id = args.run_id or latest_run_id(args.lake, args.run_date)
    spark = build_spark(args.lake)
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary = run(spark, args.lake, args.run_date, run_id, channels)
    except DataQualityError:
        raise  # already recorded with its check results
    except Exception as error:
        record_failure(spark, run_id, args.run_date, error)
        raise
    print(json.dumps(summary, default=str))


if __name__ == "__main__":
    main()

"""Run SQL against the local Iceberg tables (lake/warehouse). Runs inside the Glue Docker image.

Usage:
  scripts/spark_local.sh scripts/query_local.py "SELECT * FROM lake.wistia_gold.fact_media_daily ORDER BY date"
  scripts/spark_local.sh scripts/query_local.py            # row counts of every table
"""

import sys

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "hadoop")
    .config("spark.sql.catalog.lake.warehouse", "/work/lake/warehouse")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

if len(sys.argv) > 1:
    for sql in sys.argv[1:]:
        print(f"\n>>> {sql}")
        spark.sql(sql).show(50, truncate=40)
else:
    for ns in ("wistia_silver", "wistia_gold", "wistia_ops"):
        for row in spark.sql(f"SHOW TABLES IN lake.{ns}").collect():
            name = f"lake.{ns}.{row.tableName}"
            print(f"{name:45s} {spark.table(name).count():>6} rows")

"""Run SQL in Athena (workgroup "wistia") and print the results as tables.

uv run python scripts/athena_query.py sql/evidence.sql                 # every query in the file
uv run python scripts/athena_query.py "SELECT * FROM wistia_gold.dim_media"

--local  runs the same SQL against the local Iceberg tables instead (Spark in the Glue Docker image).
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def split_queries(text):
    """Split a .sql file into (name, sql) pairs. A query's name is its '-- name:' line."""
    queries, name, lines = [], None, []
    for line in text.splitlines():
        if line.startswith("-- name:"):
            name = line.removeprefix("-- name:").strip()
        elif line.strip().startswith("--") or not line.strip():
            continue
        else:
            lines.append(line)
            if line.rstrip().endswith(";"):
                queries.append((name, "\n".join(lines).rstrip().rstrip(";")))
                name, lines = None, []
    if lines:
        queries.append((name, "\n".join(lines)))
    return queries


def run_athena(sql, workgroup, region):
    import boto3

    athena = boto3.client("athena", region_name=region)
    qid = athena.start_query_execution(QueryString=sql, WorkGroup=workgroup)["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(status.get("StateChangeReason", status["State"]))
    rows = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        rows += [[c.get("VarCharValue", "") for c in r["Data"]] for r in page["ResultSet"]["Rows"]]
    return rows[0], rows[1:]


def print_table(header, rows):
    widths = [max(len(str(x)) for x in col) for col in zip(header, *rows, strict=False)] if rows else map(len, header)
    widths = list(widths)
    print("  ".join(h.ljust(w) for h, w in zip(header, widths, strict=False)))
    print("  ".join("-" * w for w in widths))
    for r in rows:
        print("  ".join(str(x).ljust(w) for x, w in zip(r, widths, strict=False)))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("sql", help="a .sql file or a query")
    parser.add_argument("--workgroup", default="wistia")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--local", action="store_true")
    args = parser.parse_args(argv)

    path = Path(args.sql)
    queries = split_queries(path.read_text()) if path.suffix == ".sql" and path.exists() else [(None, args.sql)]

    if args.local:  # Spark needs the catalog prefix: wistia_gold.x -> lake.wistia_gold.x
        local_sql = [q.replace(" wistia_", " lake.wistia_") for _, q in queries]
        cmd = ["scripts/spark_local.sh", "scripts/query_local.py", *local_sql]
        return subprocess.run(cmd, cwd=REPO_ROOT).returncode

    for name, sql in queries:
        print(f"\n=== {name or sql[:80]}")
        header, rows = run_athena(sql, args.workgroup, args.region)
        print_table(header, rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())

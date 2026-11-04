#!/usr/bin/env bash
# Run a PySpark script inside the AWS Glue 5.1 Docker image (same Spark, Python and Iceberg as AWS Glue).
# The project folder is mounted at /work, and src/ is on PYTHONPATH so `import wistia` works.
#
# Usage: scripts/spark_local.sh jobs/transform.py --run-date 2026-03-18 --media-config "$(uv run python -m wistia.config)"
set -euo pipefail

IMAGE="public.ecr.aws/glue/aws-glue-libs:5.1.0"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

docker run --rm \
  -v "$PROJECT_DIR":/work \
  -w /work \
  -e PYTHONPATH=/work/src \
  -e PIPELINE_ENV=local \
  --entrypoint spark-submit \
  "$IMAGE" \
  --conf spark.driver.memory=2g \
  "$@"

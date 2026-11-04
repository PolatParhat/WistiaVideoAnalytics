"""AWS Glue job entry point: the same transform as jobs/transform.py, with Glue's Spark session.

Glue passes arguments as --name value. Step Functions sets --run_date and --run_id on every run;
the CDK stack sets the fixed ones (--raw_root, --media_config) and the Iceberg catalog (--conf).
The library (src/wistia) is attached to the job with --extra-py-files.
"""

import json
import sys

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from wistia.transform.pipeline import DataQualityError, record_failure, run

args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_date", "run_id", "raw_root", "media_config"])

spark = GlueContext(SparkContext.getOrCreate()).spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
channels = {m["media_id"]: m["channel"] for m in json.loads(args["media_config"])}

try:
    summary = run(spark, args["raw_root"], args["run_date"], args["run_id"], channels)
except DataQualityError:
    raise  # recorded in ops.pipeline_runs / ops.dq_results; failing the job fails the Step Functions run
except Exception as error:
    record_failure(spark, args["run_id"], args["run_date"], error)
    raise

print(json.dumps(summary, default=str))

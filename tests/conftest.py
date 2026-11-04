"""Shared test fixtures. The Spark session is created once for the whole test run (it takes a few seconds)."""

import os
import time

import pytest

# Everything in UTC, like AWS Glue. Without this, PySpark converts timestamps to the laptop's
# local time zone when it hands them to Python, and the expected values in the tests would not match.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("wistia-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

"""Data-quality checks: each check must pass on good data and catch the problem it is written for."""

from datetime import date

from wistia.transform import quality

D = date.fromisoformat


def test_not_null(spark):
    df = spark.createDataFrame([("a", "x"), (None, "y")], "k string, v string")
    assert quality.not_null(df, "t", ["k"]).failed_rows == 1
    assert quality.not_null(df, "t", ["v"]).passed


def test_unique(spark):
    df = spark.createDataFrame([("a", 1), ("a", 1), ("b", 1)], "k string, n int")
    result = quality.unique(df, "t", ["k"])
    assert not result.passed and result.failed_rows == 1
    assert quality.unique(df.dropDuplicates(), "t", ["k"]).passed


def test_in_range_ignores_nulls(spark):
    df = spark.createDataFrame([(0.0,), (1.0,), (None,), (1.5,)], "p double")
    assert quality.in_range(df, "t", "p", 0.0, 1.0).failed_rows == 1


def test_references_finds_orphans(spark):
    fact = spark.createDataFrame([("m1",), ("m9",)], "media_id string")
    dim = spark.createDataFrame([("m1",)], "media_id string")
    result = quality.references(fact, "fact", "media_id", dim, "media_id")
    assert result.failed_rows == 1 and result.severity == "critical"


def test_one_row_per_day(spark):
    daily = spark.createDataFrame(
        [("m1", D("2026-03-17")), ("m1", D("2026-03-18")), ("m2", D("2026-03-18"))], "media_id string, date date"
    )
    windows = {"m1": (D("2026-03-17"), D("2026-03-18")), "m2": (D("2026-03-17"), D("2026-03-18"))}
    result = quality.one_row_per_day(daily, windows)
    assert not result.passed and "m2: expected 2, got 1" in result.details


def test_all_media_present(spark):
    dim = spark.createDataFrame([("m1",)], "media_id string")
    assert quality.all_media_present(dim, ["m1", "m2"]).details == "m2"


def test_plays_match_events_is_a_warning(spark):
    daily = spark.createDataFrame([("m1", D("2026-03-17"), 2)], "media_id string, date date, play_count bigint")
    events = spark.createDataFrame([("m1", D("2026-03-17"))], "media_id string, event_date date")
    result = quality.plays_match_events(daily, events)
    assert result.severity == "warning" and result.failed_rows == 1
    assert "plays=2 events=1" in result.details

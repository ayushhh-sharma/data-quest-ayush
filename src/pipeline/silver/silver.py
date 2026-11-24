"""Silver: current AllData and population, series labels, Current-vs-AllData check.

CDC source is a view of the latest bronze snapshot (SCD type 1). A Python
next_snapshot function cannot read other pipeline tables in this runtime.
Code tables are not copied into silver. Labels join the latest bronze snapshot.
"""

import logging

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession, functions as F

log = logging.getLogger("silver")

CATALOG = "pr_population"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# Only the two current fact tables. Code files stay in bronze and are joined
# into pr_series_label. Current is a subset check, not a silver table.
CDC = [
    {
        "name": "pr_all_data",
        "keys": ["series_id", "year", "period"],
        "columns": ["series_id", "year", "period", "value", "footnote_codes"],
    },
    {
        "name": "population",
        "keys": ["nation", "year"],
        "columns": ["nation_id", "nation", "year", "population"],
    },
]


def _spark() -> SparkSession:
    return SparkSession.active()


def _latest(name: str) -> DataFrame:
    # Bronze can hold more than one snapshot_date. Silver current is the latest.
    src = f"{BRONZE}.{name}"
    return _spark().read.table(src).where(f"snapshot_date = (SELECT max(snapshot_date) FROM {src})")


def _missing_current_keys() -> DataFrame:
    """Current keys that are not in AllData. Latest bronze snapshots only."""
    return _latest("pr_current").join(
        _latest("pr_all_data"), ["series_id", "year", "period"], "left_anti"
    )


def _gated_latest(bronze_name: str, columns: list[str]) -> DataFrame:
    """Latest bronze snapshot. Fails this query if Current is not a subset of AllData.

    expect_or_fail only fails its own flow. An empty snapshot would SCD-delete
    every silver key. assert_true fails the snapshot query instead.
    """
    missing = _missing_current_keys().agg(F.count(F.lit(1)).alias("missing_keys"))
    return (
        _latest(bronze_name)
        .select(*columns)
        .crossJoin(missing)
        .withColumn(
            "_ok",
            F.assert_true(F.col("missing_keys") == 0, "Current keys are missing from AllData"),
        )
        .drop("missing_keys", "_ok")
    )


def _register_cdc(spec: dict) -> None:
    # Pipeline views cannot be three-part names. The CDC target still is.
    snap = f"{spec['name']}_snapshot"
    target = f"{SILVER}.{spec['name']}"

    def snapshot() -> DataFrame:
        log.info("building silver snapshot view %s from bronze.%s", snap, spec["name"])
        return _gated_latest(spec["name"], spec["columns"])

    snapshot.__name__ = f"{spec['name']}_snapshot"
    dp.view(name=snap)(snapshot)
    dp.create_streaming_table(target)
    dp.create_auto_cdc_from_snapshot_flow(
        target=target,
        source=snap,
        keys=spec["keys"],
        stored_as_scd_type=1,
    )
    log.info("registered CDC %s source=%s keys=%s", target, snap, spec["keys"])


for _spec in CDC:
    _register_cdc(_spec)


@dp.table(name=f"{SILVER}.pr_series_label")
def pr_series_label() -> DataFrame:
    log.info("building %s.pr_series_label from latest bronze series and code files", SILVER)
    s = _latest("pr_series").alias("s")
    # pr.series column is seasonal, not seasonal_code. pr.seasonal key is seasonal_code.
    labels = (
        s.join(_latest("pr_sector").alias("sec"), F.col("s.sector_code") == F.col("sec.sector_code"), "left")
        .join(_latest("pr_measure").alias("m"), F.col("s.measure_code") == F.col("m.measure_code"), "left")
        .join(_latest("pr_class").alias("c"), F.col("s.class_code") == F.col("c.class_code"), "left")
        .join(_latest("pr_duration").alias("d"), F.col("s.duration_code") == F.col("d.duration_code"), "left")
        .join(_latest("pr_seasonal").alias("sea"), F.col("s.seasonal") == F.col("sea.seasonal_code"), "left")
        .select(
            F.col("s.series_id"),
            F.col("s.series_title"),
            F.col("sec.sector_name"),
            F.col("m.measure_text"),
            F.col("c.class_text"),
            F.col("d.duration_text"),
            F.col("sea.seasonal_text"),
        )
    )
    # Empty when Current is not a subset of AllData. No collect() in the table body.
    gate = _missing_current_keys().agg(F.count(F.lit(1)).alias("n")).where("n = 0")
    return labels.crossJoin(gate.drop("n"))


@dp.table(name=f"{SILVER}.current_outside_all_data")
@dp.expect_or_fail("current_keys_in_all_data", "missing_keys = 0")
def current_outside_all_data() -> DataFrame:
    # Latest BLS files, not CDC targets. Current is only a subset check.
    log.info("checking Current keys against AllData")
    return _missing_current_keys().agg(F.count(F.lit(1)).alias("missing_keys"))

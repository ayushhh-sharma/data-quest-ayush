"""Gold PySpark alternative, plus a check that it matches the SQL gold tables.

SQL in gold.sql in this folder is primary. These tables are the other implementation the brief asks for.
"""

import logging

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

log = logging.getLogger("gold")

CATALOG = "pr_population"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"
QUARTERS = ["Q01", "Q02", "Q03", "Q04"]


def _spark() -> SparkSession:
    return SparkSession.active()


def _q1(population: DataFrame) -> DataFrame:
    return (
        population.where((F.col("nation") == "United States") & F.col("year").between(2013, 2018))
        .agg(
            F.lit("United States").alias("nation"),
            F.lit(2013).alias("year_start"),
            F.lit(2018).alias("year_end"),
            F.count(F.lit(1)).alias("n_years"),
            F.avg("population").alias("mean_population"),
            F.stddev_samp("population").alias("stddev_sample"),
            F.stddev_pop("population").alias("stddev_population"),
        )
    )


def _q2(facts: DataFrame, labels: DataFrame) -> DataFrame:
    quarterly = (
        facts.where(F.col("period").isin(QUARTERS))
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("year_value"))
    )
    annual = (
        facts.where(F.col("period") == "Q05")
        .join(quarterly.select("series_id").distinct(), "series_id", "left_anti")
        .groupBy("series_id", "year")
        .agg(F.sum("value").alias("year_value"))
    )
    w = Window.partitionBy("series_id").orderBy(F.col("year_value").desc(), F.col("year").desc())
    return (
        quarterly.unionByName(annual)
        .withColumn("rn", F.row_number().over(w))
        .where("rn = 1")
        .join(labels.select("series_id", "series_title"), "series_id", "left")
        .select("series_id", "series_title", F.col("year").alias("best_year"), "year_value")
    )


def _q3(facts: DataFrame, population: DataFrame) -> DataFrame:
    return (
        facts.where((F.col("series_id") == "PRS30006032") & (F.col("period") == "Q01"))
        .join(population.select("year", "population"), "year", "left")
        .select("series_id", "period", "year", "value", "population")
    )


@dp.table(name=f"{GOLD}.q1_population_2013_2018_pyspark")
def q1_population_2013_2018_pyspark() -> DataFrame:
    log.info("building %s.q1_population_2013_2018_pyspark", GOLD)
    return _q1(_spark().read.table(f"{SILVER}.population"))


@dp.table(name=f"{GOLD}.q2_series_best_year_pyspark")
def q2_series_best_year_pyspark() -> DataFrame:
    log.info("building %s.q2_series_best_year_pyspark", GOLD)
    return _q2(
        _spark().read.table(f"{SILVER}.pr_all_data"),
        _spark().read.table(f"{SILVER}.pr_series_label"),
    )


@dp.table(name=f"{GOLD}.q3_prs30006032_q01_pyspark")
def q3_prs30006032_q01_pyspark() -> DataFrame:
    log.info("building %s.q3_prs30006032_q01_pyspark", GOLD)
    return _q3(
        _spark().read.table(f"{SILVER}.pr_all_data"),
        _spark().read.table(f"{SILVER}.population"),
    )


@dp.table(name=f"{GOLD}.sql_pyspark_match")
@dp.expect_or_fail("q1_match", "q1_match")
@dp.expect_or_fail("q2_match", "q2_match")
@dp.expect_or_fail("q3_match", "q3_match")
def sql_pyspark_match() -> DataFrame:
    """One row. Each flag is true when the SQL gold table equals the PySpark table."""
    log.info("comparing SQL gold to PySpark gold")
    spark = _spark()

    q1s = spark.read.table(f"{GOLD}.q1_population_2013_2018").alias("s")
    q1p = spark.read.table(f"{GOLD}.q1_population_2013_2018_pyspark").alias("p")
    q1 = q1s.crossJoin(q1p).select(
        (
            F.col("s.mean_population").eqNullSafe(F.col("p.mean_population"))
            & F.col("s.stddev_sample").eqNullSafe(F.col("p.stddev_sample"))
            & F.col("s.stddev_population").eqNullSafe(F.col("p.stddev_population"))
            & F.col("s.n_years").eqNullSafe(F.col("p.n_years"))
        ).alias("q1_match")
    )

    q2s = spark.read.table(f"{GOLD}.q2_series_best_year").select(
        "series_id",
        F.col("series_title").alias("s_title"),
        F.col("best_year").alias("s_year"),
        F.col("year_value").alias("s_value"),
    )
    q2p = spark.read.table(f"{GOLD}.q2_series_best_year_pyspark").select(
        "series_id",
        F.col("series_title").alias("p_title"),
        F.col("best_year").alias("p_year"),
        F.col("year_value").alias("p_value"),
    )
    q2 = (
        q2s.join(q2p, "series_id", "full_outer")
        .where(
            ~F.col("s_year").eqNullSafe(F.col("p_year"))
            | ~F.col("s_value").eqNullSafe(F.col("p_value"))
            | ~F.col("s_title").eqNullSafe(F.col("p_title"))
        )
        .agg(F.count(F.lit(1)).alias("n"))
        .select((F.col("n") == 0).alias("q2_match"))
    )

    q3s = spark.read.table(f"{GOLD}.q3_prs30006032_q01").select(
        "year",
        F.col("value").alias("s_value"),
        F.col("population").alias("s_pop"),
    )
    q3p = spark.read.table(f"{GOLD}.q3_prs30006032_q01_pyspark").select(
        "year",
        F.col("value").alias("p_value"),
        F.col("population").alias("p_pop"),
    )
    q3 = (
        q3s.join(q3p, "year", "full_outer")
        .where(~F.col("s_value").eqNullSafe(F.col("p_value")) | ~F.col("s_pop").eqNullSafe(F.col("p_pop")))
        .agg(F.count(F.lit(1)).alias("n"))
        .select((F.col("n") == 0).alias("q3_match"))
    )

    return q1.crossJoin(q2).crossJoin(q3)

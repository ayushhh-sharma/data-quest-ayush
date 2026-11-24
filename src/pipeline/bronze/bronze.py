"""Bronze: typed copies of the raw volume files.

Checked on the 2026-11-23 snapshot:
- BLS files are tab-separated with a header. series_id is padded to 17 chars.
  Header names are padded too. pr.txt and pr.contacts stay files.
"""

import logging
from typing import NotRequired, TypedDict

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType

log = logging.getLogger("bronze")

# Pipeline YAML default catalog/schema match these. Three-part names keep
# silver and gold tables in this same pipeline from landing in the wrong place.
CATALOG = "pr_population"
BRONZE_SCHEMA = "bronze"


class BlsTableSpec(TypedDict):
    name: str
    file: str
    keys: list[str]
    comment: str
    casts: NotRequired[dict[str, str]]


# Table name, source file, not-null keys, optional casts.
# Filenames come from the BLS listing. Keys are the join columns in pr.txt.
# pr_ is the BLS productivity survey, not a layer prefix. ACS is population.
BLS_TABLES: list[BlsTableSpec] = [
    {
        "name": "pr_all_data",
        "file": "pr.data.1.AllData",
        "keys": ["series_id", "year", "period"],
        "casts": {"year": "int", "value": "double"},
        "comment": "BLS PR AllData observations.",
    },
    {
        "name": "pr_current",
        "file": "pr.data.0.Current",
        "keys": ["series_id", "year", "period"],
        "casts": {"year": "int", "value": "double"},
        "comment": "BLS PR Current observations. Used later to check they sit inside AllData.",
    },
    {
        "name": "pr_series",
        "file": "pr.series",
        "keys": ["series_id"],
        "comment": "BLS PR series catalog, including series_title.",
    },
    {
        "name": "pr_sector",
        "file": "pr.sector",
        "keys": ["sector_code"],
        "comment": "BLS PR sector codes.",
    },
    {
        "name": "pr_measure",
        "file": "pr.measure",
        "keys": ["measure_code"],
        "comment": "BLS PR measure codes.",
    },
    {
        "name": "pr_class",
        "file": "pr.class",
        "keys": ["class_code"],
        "comment": "BLS PR class codes.",
    },
    {
        "name": "pr_duration",
        "file": "pr.duration",
        "keys": ["duration_code"],
        "comment": "BLS PR duration codes.",
    },
    {
        "name": "pr_seasonal",
        "file": "pr.seasonal",
        "keys": ["seasonal_code"],
        "comment": "BLS PR seasonal adjustment codes.",
    },
    {
        "name": "pr_period",
        "file": "pr.period",
        "keys": ["period"],
        "comment": "BLS PR period codes. Q05 is the annual average, not a fifth quarter.",
    },
    {
        "name": "pr_footnote",
        "file": "pr.footnote",
        "keys": ["footnote_code"],
        "comment": "BLS PR footnote codes.",
    },
]


def _spark() -> SparkSession:
    return SparkSession.active()


def _raw_root() -> str:
    # Set in resources/pr_population.pipeline.yml. Fallback is the volume ingest created.
    return _spark().conf.get("raw_volume", "/Volumes/pr_population/bronze/raw")


def _with_file_meta(df: DataFrame) -> DataFrame:
    # File readers expose _metadata. Ingest wrote .../snapshot_date=YYYY-MM-DD/<file>.
    path = F.col("_metadata.file_path")
    return (
        df.withColumn("snapshot_date", F.regexp_extract(path, r"snapshot_date=(\d{4}-\d{2}-\d{2})", 1))
        .withColumn("_source_file", path)
        .withColumn("_ingested_at", F.current_timestamp())
    )


def read_bls_tsv(filename: str, casts: dict[str, str] | None = None) -> DataFrame:
    """Read one BLS file from every snapshot_date folder.

    Schema comes from the header. Names are stripped and lowercased so
    'series_id        ' and 'Seasonal_code' become series_id and seasonal_code.
    """
    root = f"{_raw_root()}/pr"
    log.info("reading BLS %s from %s", filename, root)
    df = (
        _spark().read.format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .option("pathGlobFilter", filename)
        .load(root)
    )
    df = df.toDF(*[c.strip().lower() for c in df.columns])
    log.info("BLS %s columns=%s", filename, list(df.columns))
    for col_name in df.columns:
        df = df.withColumn(col_name, F.trim(col_name))
    for col_name, dtype in (casts or {}).items():
        log.debug("cast %s.%s to %s", filename, col_name, dtype)
        df = df.withColumn(col_name, F.col(col_name).cast(dtype))
    return _with_file_meta(df)


def _register_bls(spec: BlsTableSpec) -> None:
    # SDP wants a named function per table. Same reader, different file and keys.
    def _body(spec: BlsTableSpec = spec) -> DataFrame:
        return read_bls_tsv(spec["file"], spec.get("casts"))

    fn = _body
    fn.__name__ = spec["name"]
    # Brief: non-null keys on the way in. Drop the row; do not fail the update.
    for key in spec["keys"]:
        fn = dp.expect_or_drop(f"{key}_not_null", f"{key} IS NOT NULL")(fn)
    qualified = f"{CATALOG}.{BRONZE_SCHEMA}.{spec['name']}"
    dp.table(name=qualified, comment=spec["comment"])(fn)
    log.info("registered %s file=%s keys=%s casts=%s", qualified, spec["file"], spec["keys"], spec.get("casts") or {})


for _spec in BLS_TABLES:
    _register_bls(_spec)


# ACS payload is one object: { "data": [ {"Nation ID", "Nation", "Year", "Population"}, ... ] }.
POPULATION_ROWS = ArrayType(
    StructType(
        [
            StructField("Nation ID", StringType()),
            StructField("Nation", StringType()),
            StructField("Year", IntegerType()),
            StructField("Population", DoubleType()),
        ]
    )
)


# @dp.table is what registers this as a pipeline table. Same wrapper BLS gets
# inside _register_bls. Population cannot use that list: it is nested JSON, not a TSV.
@dp.table(name=f"{CATALOG}.{BRONZE_SCHEMA}.population", comment="Census ACS 1-year US population. One row per year from the JSON data array.")
# Brief: non-null keys on the way in. Grain of this extract is nation + year.
@dp.expect_or_drop("year_not_null", "year IS NOT NULL")
@dp.expect_or_drop("nation_not_null", "nation IS NOT NULL")
def population() -> DataFrame:
    root = f"{_raw_root()}/population"
    log.info("reading population json from %s", root)
    raw = _with_file_meta(
        _spark().read.format("json")
        .option("multiLine", "true")
        .option("pathGlobFilter", "population.json")
        .load(root)
    )
    # Batch json infers `data` as ARRAY. from_json is only for a STRING column.
    data_type = dict(raw.dtypes).get("data", "")
    log.info("population data type=%s", data_type)
    if data_type.startswith("string"):
        rows_col = F.from_json(F.col("data").cast("string"), POPULATION_ROWS)
    else:
        rows_col = F.col("data")
    parsed = raw.withColumn("rows", rows_col)
    log.info("exploding ACS data array into one row per year")
    return parsed.select(
        F.explode("rows").alias("row"),
        "snapshot_date",
        "_source_file",
        "_ingested_at",
    ).select(
        F.col("row.`Nation ID`").alias("nation_id"),
        F.col("row.Nation").alias("nation"),
        F.col("row.Year").cast("int").alias("year"),
        F.col("row.Population").cast("double").alias("population"),
        "snapshot_date",
        "_source_file",
        "_ingested_at",
    )

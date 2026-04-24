"""
utils.py — Single utility file for all Databricks engines.

FIX — CAST_INVALID_INPUT on order_reviews (Bronze batch 1):
─────────────────────────────────────────────────────────────
Error seen:
  CAST_INVALID_INPUT: value '2018-04-02 13:48:07' of type STRING
  cannot be cast to INT

Root cause:
  The old apply_casts() used col(column).cast(IntegerType()) — a HARD cast.
  When Databricks Photon physically writes the file, it validates every value.
  order_reviews has review_score declared as cast_int in table_config.
  Spark's inferSchema sometimes infers review_creation_date as the first
  column it tries to cast if column ordering shifts, OR Photon validates the
  entire row during write and surfaces any malformed value.

  The real issue: hard .cast(IntegerType()) is strict at write time.

Fix:
  Use try_cast via spark SQL expr() — returns NULL for unparseable values
  instead of raising an exception. This is EXACTLY what silver_engine.py
  already does in _apply_rule() for cast_int and cast_double.

  cast_string is always safe — col.cast("string") never fails.
  to_timestamp is NOT applied at Bronze — timestamps stay as raw strings
  in Bronze and are cast in Silver via _apply_rule("to_timestamp").
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, trim, initcap, upper,
    when, coalesce, expr
)
from delta.tables import DeltaTable


# ============================================================================
# I/O UTILITIES
# ============================================================================

def read_csv(spark, s3_path):
    """Read CSV from S3 with inferred schema."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(s3_path)
    )


def read_delta(spark, s3_path):
    """Read Delta table from S3 path."""
    return spark.read.format("delta").load(s3_path)


def write_delta(df, s3_path, mode="overwrite", partition_by=None, catalog_table=None):
    """
    Write DataFrame to Delta and optionally register in catalog.
    Drops stale catalog entry before saveAsTable — same fix as original engines.
    """
    spark = df.sparkSession

    if catalog_table:
        spark.sql(f"DROP TABLE IF EXISTS {catalog_table}")

    writer = (
        df.write
        .format("delta")
        .mode(mode)
        .option("path", s3_path)
        .option("overwriteSchema", "true")
    )

    if partition_by:
        writer = writer.partitionBy(partition_by)

    if catalog_table:
        writer.saveAsTable(catalog_table)
    else:
        writer.save(s3_path)


def write_parquet(df, s3_path):
    """Write DataFrame to Parquet (for Snowflake COPY INTO)."""
    df.write.format("parquet").mode("overwrite").save(s3_path)


def register_catalog(spark, table_name, s3_path):
    """
    Register Delta path in catalog using IF NOT EXISTS.
    Avoids DELTA_SCHEMA_NOT_PROVIDED on newer Databricks runtimes.
    Safe to call even if table is already registered (no-op).
    """
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} "
        f"USING DELTA LOCATION '{s3_path}'"
    )


# ============================================================================
# DATAFRAME UTILITIES
# ============================================================================

def add_audit_columns(df, source_file, batch_id):
    """Add ingestion_timestamp, source_file, batch_id to every row."""
    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file",          lit(source_file))
        .withColumn("batch_id",             lit(batch_id))
    )


def drop_audit_columns(df):
    """
    Drop Bronze audit columns before writing to Silver.
    Only drops columns that actually exist in the DataFrame.
    """
    audit_cols = ["batch_id", "source_file", "ingestion_timestamp"]
    cols_to_drop = [c for c in audit_cols if c in df.columns]
    return df.drop(*cols_to_drop) if cols_to_drop else df


def apply_casts(df, table_config):
    """
    Apply type casts from cleaning_rules at Bronze read time.

    Only handles cast_int, cast_double, cast_string.
    All other actions (initcap_trim, fill_null, to_timestamp, etc.)
    are Silver-layer operations handled by apply_cleaning_rule().

    FIX — uses try_cast via expr() instead of hard .cast(DataType()):
      try_cast returns NULL for unparseable values instead of raising
      CAST_INVALID_INPUT during the physical write.

    Why NOT to_timestamp at Bronze:
      Timestamp columns stay as strings in Bronze. Silver's _apply_rule()
      casts them via try_cast AS TIMESTAMP. Casting at Bronze caused the
      original error because Photon tried to cast timestamp strings to INT.
    """
    for rule in table_config.get("cleaning_rules", []):
        action = rule.get("action")
        column = rule.get("column")

        if column not in df.columns:
            continue  # column absent in this CSV file — skip silently

        if action == "cast_int":
            # try_cast: '2018-04-02 13:48:07' → NULL (not an exception)
            df = df.withColumn(column, expr(f"try_cast(`{column}` AS INT)"))

        elif action == "cast_double":
            # try_cast: any non-numeric string → NULL (not an exception)
            df = df.withColumn(column, expr(f"try_cast(`{column}` AS DOUBLE)"))

        elif action == "cast_string":
            # String cast always succeeds — safe to use hard cast here
            df = df.withColumn(column, col(column).cast("string"))

        # All other actions (to_timestamp, fill_null, replace_value,
        # initcap_trim, upper_trim, rename) are Silver-only — skip at Bronze

    return df


def apply_cleaning_rule(df, rule):
    """
    Apply a single Silver cleaning rule to a DataFrame.
    Mirrors silver_engine._apply_rule() exactly.
    Uses try_cast for numeric and timestamp conversions.
    """
    c = rule["column"]
    a = rule["action"]

    if   a == "initcap_trim"  : return df.withColumn(c, initcap(trim(col(c))))
    elif a == "upper_trim"    : return df.withColumn(c, upper(trim(col(c))))
    elif a == "cast_string"   : return df.withColumn(c, col(c).cast("string"))
    elif a == "cast_double"   : return df.withColumn(c, expr(f"try_cast(`{c}` AS DOUBLE)"))
    elif a == "cast_int"      : return df.withColumn(c, expr(f"try_cast(`{c}` AS INT)"))
    elif a == "to_timestamp"  : return df.withColumn(c, expr(f"try_cast(`{c}` AS TIMESTAMP)"))
    elif a == "fill_null"     : return df.withColumn(c, coalesce(col(c), lit(rule.get("default", ""))))
    elif a == "replace_value" : return df.withColumn(c, when(col(c) == rule["old"], rule["new"]).otherwise(col(c)))
    elif a == "rename"        : return df.withColumnRenamed(c, rule["new_name"])
    else:
        print(f"    WARNING: Unknown action '{a}' for column '{c}' — skipping")
        return df


def deduplicate(df, keys):
    """
    Deduplicate DataFrame on given key columns.
    Returns (deduped_df, before_count, after_count).
    Called before MERGE to prevent DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW.
    """
    before    = df.count()
    df_deduped = df.dropDuplicates(keys)
    after     = df_deduped.count()
    return df_deduped, before, after


def get_row_count(df):
    """Return row count of DataFrame."""
    return df.count()


# ============================================================================
# LOGGING UTILITIES
# ============================================================================

def log_stage(stage_name, batch_id):
    """Print stage header — matches original engine style."""
    print("=" * 70)
    print(f"{stage_name.upper()} — batch: {batch_id}")
    print("=" * 70)


def log_table(table_name, row_count, operation="ingested"):
    """Log one table result — aligned columns like original engines."""
    print(f"    {table_name:35s} — {row_count:>10,} rows {operation}")


def log_error(table_name, error):
    """Log a table-level error."""
    print(f"    ERROR [{table_name}]: {str(error)}")


def log_summary(stage_name, results, errors):
    """Print stage completion summary."""
    total = sum(results.values())
    print(f"\n{stage_name.upper()} COMPLETE — {total:,} total rows")
    for table, cnt in results.items():
        print(f"    {table}: {cnt:,}")
    if errors:
        print(f"FAILED TABLES: {errors}")


# ============================================================================
# DATA QUALITY UTILITIES
# (used by run_dq.py — not called inside Bronze/Silver/Gold engines)
# ============================================================================

def check_null(df, column):
    """Count NULL values in a column."""
    return df.filter(col(column).isNull()).count()


def check_duplicates(df, keys):
    """Count key groups that have more than one row."""
    from pyspark.sql.functions import count as _count
    return (
        df.groupBy(*keys)
        .agg(_count("*").alias("cnt"))
        .filter(col("cnt") > 1)
        .count()
    )


def check_range(df, column, min_val, max_val):
    """Count values outside [min_val, max_val]."""
    return df.filter(
        (col(column) < min_val) | (col(column) > max_val)
    ).count()


def check_negative_values(df, column):
    """Count rows where column < 0."""
    return df.filter(col(column) < 0).count()


def check_zero_values(df, column):
    """Count rows where column == 0."""
    return df.filter(col(column) == 0).count()


def quarantine_bad_rows(df, bad_condition, table_name, quarantine_path):
    """
    Split df into good/bad rows using bad_condition.
    Appends bad rows (with quarantine metadata) to Delta at quarantine_path.
    Returns (df_good, bad_count, good_count).
    """
    df_bad = (
        df.filter(bad_condition)
        .withColumn("quarantine_reason",    lit("failed_dq_checks"))
        .withColumn("quarantine_table",     lit(table_name))
        .withColumn("quarantine_timestamp", current_timestamp())
    )
    df_good = df.filter(~bad_condition)

    bad_count  = df_bad.count()
    good_count = df_good.count()

    if bad_count > 0:
        (
            df_bad.write
            .format("delta")
            .mode("append")
            .option("path", quarantine_path)
            .option("mergeSchema", "true")
            .save(quarantine_path)
        )

    return df_good, bad_count, good_count
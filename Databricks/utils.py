"""
utils.py — Shared utilities for Bronze/Silver/Gold engines
FIXED: Handles partition mismatch for append operations
FIXED: Schema inference issues with CSV reading
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, to_timestamp as to_ts
from delta.tables import DeltaTable


# ══════════════════════════════════════════════════════════════════════════════
# READ FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def read_csv(spark, s3_path, table_config=None):
    """
    Read CSV with headers and schema enforcement
    
    CRITICAL: Read as strings first, then apply proper types
    """
    from pyspark.sql.functions import to_timestamp, col
    
    # Step 1: Always read as strings first - NO SCHEMA INFERENCE
    df = (spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("multiLine", True)          # ← handles newlines inside quoted fields
            .option("quote", '"')               # ← standard quote char
            .option("escape", '"')              # ← handles "" escaped quotes inside fields
            .option("mode", "PERMISSIVE")       # ← bad rows become nulls instead of crashing
            .csv(s3_path))    
    # Step 2: Apply schema from config if provided
    if table_config and "schema" in table_config:
        schema_dict = table_config["schema"]
        
        for col_name, dtype in schema_dict.items():
            if col_name in df.columns:
                dtype_lower = str(dtype).lower()
                
                # Apply type casting based on config
                if dtype_lower == "timestamp":
                    # to_timestamp returns NULL for unparseable values — safe by default
                    df = df.withColumn(col_name, to_timestamp(col(col_name)))
                elif dtype_lower in ["int", "integer"]:
                    from pyspark.sql.functions import when, col as _col
                    df = df.withColumn(
                        col_name,
                        when(
                            col(col_name).rlike(r"^\d+$"),   # only cast if it looks like a number
                            col(col_name).cast("int")
                        ).otherwise(None)
                    )
                elif dtype_lower in ["double", "float"]:
                    # Cast to double
                    df = df.withColumn(col_name, col(col_name).cast("double"))
                elif dtype_lower != "string":
                    # Cast to other specified types
                    df = df.withColumn(col_name, col(col_name).cast(dtype))
    
    return df


def read_delta(spark, s3_path):
    """Read Delta table from S3 path"""
    return spark.read.format("delta").load(s3_path)


# ══════════════════════════════════════════════════════════════════════════════
# WRITE FUNCTIONS - FIXED FOR PARTITION MISMATCH
# ══════════════════════════════════════════════════════════════════════════════

def write_delta(df, s3_path, mode="overwrite", partition_by=None, catalog_table=None):
    writer = df.write.format("delta").mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
    
    if mode == "append":
        writer.option("mergeSchema", "false").save(s3_path)
        if catalog_table:
            try:
                spark = SparkSession.builder.getOrCreate()
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {catalog_table}
                    USING DELTA LOCATION '{s3_path}'
                """)
            except:
                pass
    else:
        # ADD overwriteSchema here — critical for first runs and reruns
        if catalog_table:
            (writer
                .option("overwriteSchema", "true")   # ← ADD THIS
                .option("path", s3_path)
                .saveAsTable(catalog_table))
        else:
            writer.option("overwriteSchema", "true").save(s3_path)  # ← AND HERE
            

def register_catalog(spark, catalog_table, s3_path):
    """Register Delta table in Unity Catalog"""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog_table}
        USING DELTA
        LOCATION '{s3_path}'
    """)


# ══════════════════════════════════════════════════════════════════════════════
# TRANSFORMATIONS
# ══════════════════════════════════════════════════════════════════════════════

def apply_casts(df, table_config):
    """
    Apply column type casts from table_config
    Enhanced to handle timestamps properly
    """
    for col_name, dtype in table_config.get("schema", {}).items():
        if col_name in df.columns:
            if dtype.lower() == "timestamp":
                # Special handling for timestamps
                df = df.withColumn(col_name, to_ts(col(col_name)))
            else:
                df = df.withColumn(col_name, col(col_name).cast(dtype))
    return df


def add_audit_columns(df, source_file, batch_id):
    """Add ingestion_timestamp, source_file, batch_id columns"""
    return (df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", lit(source_file))
        .withColumn("batch_id", lit(batch_id))
    )


def drop_audit_columns(df):
    """Drop audit columns before writing to Silver"""
    audit_cols = ["ingestion_timestamp", "source_file", "batch_id"]
    existing_audit = [c for c in audit_cols if c in df.columns]
    return df.drop(*existing_audit) if existing_audit else df


def apply_cleaning_rule(df, rule):
    """
    Apply a single cleaning rule from table_config
    Supports: cast_*, to_timestamp, rename, fill_null, replace_value, trim operations
    """
    from pyspark.sql.functions import trim, upper, lower, initcap
    
    action = rule.get("action")
    column = rule.get("column")
    
    # Support both "action" style and "type" style rules
    rule_type = rule.get("type")
    
    if action and column and column in df.columns:
        # New action-based style
        if action == "cast_string":
            df = df.withColumn(column, col(column).cast("string"))
        
        elif action == "cast_int":
            df = df.withColumn(column, col(column).cast("int"))
        
        elif action == "cast_double":
            df = df.withColumn(column, col(column).cast("double"))
        
        elif action == "to_timestamp":
            df = df.withColumn(column, to_ts(col(column)))
        
        elif action == "trim":
            df = df.withColumn(column, trim(col(column)))
        
        elif action == "upper":
            df = df.withColumn(column, upper(col(column)))
        
        elif action == "lower":
            df = df.withColumn(column, lower(col(column)))
        
        elif action == "initcap":
            df = df.withColumn(column, initcap(col(column)))
        
        elif action == "upper_trim":
            df = df.withColumn(column, upper(trim(col(column))))
        
        elif action == "initcap_trim":
            df = df.withColumn(column, initcap(trim(col(column))))
        
        elif action == "rename":
            new_name = rule.get("new_name")
            if new_name:
                df = df.withColumnRenamed(column, new_name)
        
        elif action == "fill_null":
            default_val = rule.get("default", "")
            df = df.withColumn(column, 
                when(col(column).isNull(), lit(default_val))
                .otherwise(col(column)))
        
        elif action == "replace_value":
            old_val = rule.get("old")
            new_val = rule.get("new")
            if old_val is not None and new_val is not None:
                df = df.withColumn(column,
                    when(col(column) == old_val, lit(new_val))
                    .otherwise(col(column)))
    
    # Legacy type-based style
    elif rule_type == "trim_whitespace":
        for col_name in rule.get("columns", []):
            if col_name in df.columns:
                df = df.withColumn(col_name, trim(col(col_name)))
    
    elif rule_type == "uppercase":
        for col_name in rule.get("columns", []):
            if col_name in df.columns:
                df = df.withColumn(col_name, upper(col(col_name)))
    
    elif rule_type == "replace_nulls":
        col_name = rule.get("column")
        default_val = rule.get("default")
        if col_name in df.columns:
            df = df.withColumn(col_name, 
                when(col(col_name).isNull(), lit(default_val))
                .otherwise(col(col_name)))
    
    elif rule_type == "remove_duplicates":
        subset = rule.get("subset", [])
        df = df.dropDuplicates(subset) if subset else df.dropDuplicates()
    
    return df


def deduplicate(df, keys):
    """Remove duplicates based on key columns, keep latest by ingestion_timestamp"""
    before = df.count()
    
    if "ingestion_timestamp" in df.columns:
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number, desc
        
        window = Window.partitionBy(*keys).orderBy(desc("ingestion_timestamp"))
        df = df.withColumn("_row_num", row_number().over(window)) \
               .filter(col("_row_num") == 1) \
               .drop("_row_num")
    else:
        df = df.dropDuplicates(keys)
    
    after = df.count()
    return df, before, after


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def get_row_count(df):
    """Get DataFrame row count"""
    return df.count()


def path_exists(s3_path):
    """Check if S3 path exists"""
    try:
        spark = SparkSession.builder.getOrCreate()
        dbutils.fs.ls(s3_path)
        return True
    except:
        return False


def is_delta_table(spark, s3_path):
    """Check if path contains a valid Delta table"""
    try:
        return DeltaTable.isDeltaTable(spark, s3_path)
    except:
        return False


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

def log_stage(stage_name, batch_number):
    """Print stage header"""
    print("=" * 70)
    print(f"{stage_name} — batch: {batch_number}")
    print("=" * 70)


def log_table(table_name, count, action):
    """Log table processing"""
    print(f"  ✓ {table_name}: {count:,} rows {action}")


def log_error(table_name, error):
    """Log table error"""
    print(f"    ERROR [{table_name}]: {error}")


def log_summary(stage_name, results, errors):
    """Print summary footer"""
    total_rows = sum(results.values()) if results else 0
    print(f"\n{stage_name} COMPLETE — {total_rows:,} total rows")
    if errors:
        print(f"FAILED TABLES: {errors}")
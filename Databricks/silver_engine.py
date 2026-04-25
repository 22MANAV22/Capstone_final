"""
Silver Engine — PRODUCTION VERSION with correct operation order
Cleaning rules run BEFORE schema enforcement to handle type conversions properly
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampType
from delta.tables import DeltaTable
import utils
from table_config import REFERENCE_TABLES, TRANSACTIONAL_TABLES
from table_config import S3_DELTA_BRONZE, S3_DELTA_SILVER
from checkpoint_manager import CheckpointManager


class SilverEngine:

    def __init__(self, spark):
        self.spark = spark
        self.results = {}
        self.errors = []
        self.checkpoint = CheckpointManager(spark)

    def _enforce_schema(self, df, table_name, config):
        """
        Enforce schema AFTER cleaning rules have run
        Only enforce on columns that still need type conversion
        Skip timestamp columns that were already converted by cleaning rules
        """
        schema_def = config.get("schema", {})
        cleaning_rules = config.get("cleaning_rules", [])
        
        # Get columns that are converted by cleaning rules
        cleaned_columns = set()
        for rule in cleaning_rules:
            if rule.get("action") in ["to_timestamp", "cast_int", "cast_double", "cast_string"]:
                cleaned_columns.add(rule.get("column"))
        
        for col_name, dtype in schema_def.items():
            if col_name not in df.columns:
                continue
            
            # Skip if this column is already handled by cleaning rules
            if col_name in cleaned_columns:
                continue
            
            dtype_lower = str(dtype).lower()
            
            try:
                if dtype_lower in ["int", "integer"]:
                    df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
                elif dtype_lower in ["double", "float"]:
                    df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
                elif dtype_lower in ["string", "str"]:
                    df = df.withColumn(col_name, col(col_name).cast(StringType()))
                elif "timestamp" in dtype_lower:
                    df = df.withColumn(col_name, col(col_name).cast(TimestampType()))
                    
            except Exception as e:
                print(f"    Warning: Could not cast {col_name} to {dtype}: {e}")
        
        return df

    def _clean_table(self, df, config):
        """Apply all cleaning rules from table_config."""
        for rule in config.get("cleaning_rules", []):
            df = utils.apply_cleaning_rule(df, rule)
        return utils.drop_audit_columns(df)

    def _write_merge(self, df, table_name, merge_keys, config):
        """
        MERGE into Silver Delta table with schema enforcement
        Handles type mismatches and creates table if missing
        """
        s3_path = f"{S3_DELTA_SILVER}/{table_name}"

        # Deduplicate before merge
        df, before, after = utils.deduplicate(df, merge_keys)
        if before != after:
            print(f"    Deduped: {before:,} → {after:,} rows")

        try:
            # Check if Delta table exists
            if DeltaTable.isDeltaTable(self.spark, s3_path):
                # Read existing schema
                existing_df = self.spark.read.format("delta").load(s3_path)
                existing_schema = existing_df.schema
                
                # Align new data schema with existing schema
                df = self._align_schemas(df, existing_schema, table_name)
                
                # Now perform merge
                target = DeltaTable.forPath(self.spark, s3_path)
                condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])

                (target.alias("t").merge(df.alias("s"), condition)
                 .whenMatchedUpdateAll()
                 .whenNotMatchedInsertAll()
                 .execute())

                utils.register_catalog(self.spark, f"silver.{table_name}", s3_path)
                count = utils.get_row_count(utils.read_delta(self.spark, s3_path))
                
            else:
                # Table doesn't exist yet — first write
                print(f"    Creating new Silver table: {table_name}")
                utils.write_delta(
                    df, 
                    s3_path, 
                    mode="overwrite",
                    catalog_table=f"silver.{table_name}"
                )
                count = utils.get_row_count(df)

        except Exception as e:
            # If merge fails, try overwrite with schema merge
            print(f"    Merge failed: {str(e)[:100]}")
            print(f"    Attempting overwrite with schema evolution...")
            
            try:
                # Drop existing table
                self.spark.sql(f"DROP TABLE IF EXISTS silver.{table_name}")
                
                # Write fresh with new schema
                df.write.format("delta") \
                    .mode("overwrite") \
                    .option("overwriteSchema", "true") \
                    .save(s3_path)
                
                # Re-register
                self.spark.sql(f"""
                    CREATE TABLE silver.{table_name}
                    USING DELTA
                    LOCATION '{s3_path}'
                """)
                
                count = utils.get_row_count(df)
                print(f"    ✓ Table recreated with new schema")
                
            except Exception as e2:
                print(f"    ✗ Fallback also failed: {e2}")
                raise

        self.results[table_name] = count
        utils.log_table(table_name, count, "merged")

    def _align_schemas(self, new_df, existing_schema, table_name):
        """
        Align new DataFrame schema to match existing table schema
        Casts columns to match existing types
        """
        from pyspark.sql.functions import lit
        
        # Get field name to type mapping from existing schema
        existing_types = {field.name: field.dataType for field in existing_schema.fields}
        
        # Cast each column in new_df to match existing type
        for field_name, field_type in existing_types.items():
            if field_name in new_df.columns:
                current_type = new_df.schema[field_name].dataType
                
                # Only cast if types differ
                if current_type != field_type:
                    print(f"    Casting {field_name}: {current_type} → {field_type}")
                    new_df = new_df.withColumn(field_name, col(field_name).cast(field_type))
            else:
                # Add missing column with nulls
                print(f"    Adding missing column: {field_name}")
                new_df = new_df.withColumn(field_name, lit(None).cast(field_type))
        
        # Remove extra columns not in existing schema
        extra_cols = set(new_df.columns) - set(existing_types.keys())
        if extra_cols:
            print(f"    Removing extra columns: {extra_cols}")
            new_df = new_df.drop(*extra_cols)
        
        # Reorder columns to match existing schema
        new_df = new_df.select(*existing_types.keys())
        
        return new_df

    def run(self, batch_number):
        """
        Main entry point
        Operation order: Read → Clean → Enforce Schema → Merge
        """
        utils.log_stage("SILVER ENGINE", batch_number)
        self.results = {}
        self.errors = []

        # Check checkpoint
        if self.checkpoint.is_done(batch_number, "silver"):
            print(f"\nBatch {batch_number} already processed in Silver. Skipping.")
            return

        # Combine all tables for processing
        all_tables = {**REFERENCE_TABLES, **TRANSACTIONAL_TABLES}

        for table_name, config in all_tables.items():
            try:
                # Read from Bronze
                bronze_path = f"{S3_DELTA_BRONZE}/{table_name}"
                
                # Check if Bronze table exists
                if not DeltaTable.isDeltaTable(self.spark, bronze_path):
                    print(f"  {table_name}: Bronze table doesn't exist yet — skipping")
                    continue

                df = utils.read_delta(self.spark, bronze_path)

                # Filter by batch_id if column exists
                if "batch_id" in df.columns:
                    df = df.filter(col("batch_id") == f"batch_{batch_number}")

                row_count = utils.get_row_count(df)
                
                if row_count == 0:
                    print(f"  {table_name}: no rows for batch_{batch_number} — skipping")
                    continue

                # CRITICAL: Clean BEFORE schema enforcement
                # This allows to_timestamp and cast_* actions to run first
                df = self._clean_table(df, config)
                
                # Then enforce schema on remaining columns
                df = self._enforce_schema(df, table_name, config)

                # Write with merge if merge keys defined, otherwise overwrite
                merge_keys = config.get("merge_keys", [])
                if merge_keys:
                    self._write_merge(df, table_name, merge_keys, config)
                else:
                    # For reference tables, just overwrite
                    utils.write_delta(
                        df, 
                        f"{S3_DELTA_SILVER}/{table_name}",
                        mode="overwrite",
                        catalog_table=f"silver.{table_name}"
                    )
                    count = utils.get_row_count(df)
                    self.results[table_name] = count
                    utils.log_table(table_name, count, "cleaned")

            except Exception as e:
                print(f"    ERROR [{table_name}]: {str(e)[:200]}")
                utils.log_error(table_name, e)
                self.errors.append(table_name)

        # Mark checkpoint if no errors
        if not self.errors:
            total_rows = sum(self.results.values())
            self.checkpoint.mark_done(batch_number, "silver", rows=total_rows)
        else:
            print(f"\nWARNING: {len(self.errors)} tables failed — checkpoint NOT written")

        utils.log_summary("SILVER", self.results, self.errors)
"""
Bronze Engine — refactored to use utils.py
No DQ checks here — just ingestion.
FIXED: Pass table_config to read_csv for proper schema handling
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from delta.tables import DeltaTable
import utils
from table_config import REFERENCE_TABLES, TRANSACTIONAL_TABLES
from table_config import S3_RAW, S3_LIVE, S3_DELTA_BRONZE
from checkpoint_manager import CheckpointManager


class BronzeEngine:

    def __init__(self, spark):
        self.spark = spark
        self.results = {}
        self.errors = []
        self.checkpoint = CheckpointManager(spark)

    def _ingest_table(self, table_name, s3_source, batch_id, table_config, 
                 write_mode="overwrite", partition=None):

        try:
            # FIXED: Pass table_config to read_csv for proper schema enforcement
            df = utils.read_csv(self.spark, s3_source, table_config)
            
            # Apply any additional casts (now redundant but kept for safety)
            # df = utils.apply_casts(df, table_config)
            
            # Add audit columns
            df = utils.add_audit_columns(df, s3_source.split('/')[-1], batch_id)

            s3_path = f"{S3_DELTA_BRONZE}/{table_name}"
            table_name_full = f"bronze.{table_name}"

            # Check if path exists and is Delta
            path_exists = utils.path_exists(s3_path)
            is_delta = utils.is_delta_table(self.spark, s3_path)

            if path_exists and not is_delta:
                print(f"⚠️ Non-delta data found at {s3_path}, cleaning...")
                try:
                    dbutils.fs.rm(s3_path, True)
                except:
                    # If dbutils not available, skip cleanup
                    pass

            # Write Delta table
            utils.write_delta(
                df,
                s3_path,
                mode=write_mode,
                partition_by=partition,
                catalog_table=table_name_full
            )

            count = utils.get_row_count(df)
            self.results[table_name] = count
            utils.log_table(table_name, count, "ingested")

        except Exception as e:
            utils.log_error(table_name, e)
            self.errors.append(table_name)

    def ingest_reference(self):
        """Load reference tables."""
        print("\n  Reference tables (full overwrite):")
        for name, cfg in REFERENCE_TABLES.items():
            self._ingest_table(name, f"{S3_RAW}/batch_1/{cfg['source_file']}", 
                             "batch_1", cfg)

    def ingest_transactional_batch1(self):
        """Batch 1 transactional."""
        print("\n  Transactional tables — batch 1:")
        for name, cfg in TRANSACTIONAL_TABLES.items():
            self._ingest_table(name, f"{S3_RAW}/batch_1/{cfg['source_file']}", 
                             "batch_1", cfg, partition="batch_id")

    def ingest_transactional_append(self, batch_number):
        """Batches 2/3/4."""
        print(f"\n  Transactional tables — batch {batch_number}:")
        for name, cfg in TRANSACTIONAL_TABLES.items():
            self._ingest_table(name, f"{S3_RAW}/batch_{batch_number}/{cfg['source_file']}", 
                             f"batch_{batch_number}", cfg, 
                             write_mode="append", partition="batch_id")

    def ingest_live(self):
        """Live ingestion (robust + schema-safe)."""
        print("\n  Live stream:")

        for name, cfg in TRANSACTIONAL_TABLES.items():
            try:
                print(f"\nProcessing live table: {name}")

                # Read matching files
                file_pattern = f"{S3_LIVE}/*{cfg['source_file']}*"

                # FIXED: Use updated read_csv with config
                df = utils.read_csv(self.spark, file_pattern, cfg)

                count = df.count()
                print(f"Row count: {count}")

                if count == 0:
                    print(f"⚠️ No data found for {name}, skipping...")
                    continue

                # Apply base transformations
                df = utils.apply_casts(df, cfg)

                # Add audit columns
                df = utils.add_audit_columns(df, cfg['source_file'], "live_stream")

                table_name_full = f"bronze.{name}"
                s3_path = f"{S3_DELTA_BRONZE}/{name}"

                # Check if Delta table exists at S3 path (not catalog)
                if DeltaTable.isDeltaTable(self.spark, s3_path):
                    print(f"Appending to existing Delta table: {table_name_full}")
                    
                    (df.write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "false")
                    .save(s3_path))
                    
                else:
                    print(f"Creating new Delta table: {table_name_full}")
                    
                    # Drop stale catalog entry if exists
                    self.spark.sql(f"DROP TABLE IF EXISTS {table_name_full}")
                    
                    (df.write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .option("path", s3_path)
                    .partitionBy("batch_id")
                    .saveAsTable(table_name_full))

                # Logging
                self.results[name] = count
                utils.log_table(name, count, "live_ingested")

            except Exception as e:
                utils.log_error(name, e)
                self.errors.append(name)

    def run(self, batch_number):
        """Main entry point."""
        utils.log_stage("BRONZE ENGINE", batch_number)
        self.results = {}
        self.errors = []

        if batch_number == "live":
            self.ingest_live()
        elif batch_number in ("1", "2", "3", "4"):
            if self.checkpoint.is_done(batch_number, "bronze"):
                print(f"\nBatch {batch_number} already ingested. Skipping.")
                return

            if batch_number == "1":
                self.ingest_reference()
                self.ingest_transactional_batch1()
            else:
                self.ingest_transactional_append(batch_number)

            if not self.errors:
                total_rows = sum(self.results.values())
                self.checkpoint.mark_done(batch_number, "bronze", rows=total_rows)
            else:
                print(f"\nWARNING: {len(self.errors)} tables failed — checkpoint NOT written")

        utils.log_summary("BRONZE", self.results, self.errors)
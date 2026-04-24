"""
Bronze Engine — refactored to use utils.py
No DQ checks here — just ingestion.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
        """Generic table ingestion using utils."""
        try:
            df = utils.read_csv(self.spark, s3_source)
            df = utils.apply_casts(df, table_config)
            df = utils.add_audit_columns(df, s3_source.split('/')[-1], batch_id)
            
            s3_path = f"{S3_DELTA_BRONZE}/{table_name}"
            utils.write_delta(df, s3_path, mode=write_mode, 
                            partition_by=partition, 
                            catalog_table=f"bronze.{table_name}")
            
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
        """Live stream."""
        print("\n  Live stream:")
        for name, cfg in TRANSACTIONAL_TABLES.items():
            self._ingest_table(name, f"{S3_LIVE}/*{cfg['source_file']}", 
                             "live_stream", cfg, 
                             write_mode="append", partition="batch_id")

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


"""
CDC Engine — refactored to use utils.py
DQ checks integrated.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable
import utils
from table_config import TRANSACTIONAL_TABLES, S3_DELTA_BRONZE, S3_DELTA_SILVER
from silver_engine import SilverEngine


class CDCEngine:

    def __init__(self, spark):
        self.spark = spark
        self.results = {}
        self.silver = SilverEngine(spark)

    def _merge_table(self, table_name, df_live, config):
        """MERGE live rows into Silver."""
        merge_keys = config.get("merge_keys", [])
        if not merge_keys:
            print(f"    SKIP {table_name}: no merge keys")
            return

        # Deduplicate
        df_live, before, after = utils.deduplicate(df_live, merge_keys)
        if before != after:
            print(f"    Deduped: {before:,} → {after:,} rows")

        df_live = df_live.withColumn("cdc_merge_timestamp", current_timestamp())
        
        s3_path = f"{S3_DELTA_SILVER}/{table_name}"

        try:
            target = DeltaTable.forPath(self.spark, s3_path)
            condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
            
            (target.alias("t").merge(df_live.alias("s"), condition)
             .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
            
            utils.register_catalog(self.spark, f"silver.{table_name}", s3_path)
            count = utils.get_row_count(utils.read_delta(self.spark, s3_path))
            self.results[table_name] = count
            utils.log_table(table_name, count, "merged")
            
        except Exception as e:
            utils.log_error(table_name, e)

    def run(self):
        """Main entry point."""
        utils.log_stage("CDC ENGINE", "live_stream")
        self.results = {}

        for table_name, config in TRANSACTIONAL_TABLES.items():
            try:
                df_bronze = utils.read_delta(self.spark, f"{S3_DELTA_BRONZE}/{table_name}")
                df_live = df_bronze.filter(col("batch_id") == "live_stream")
                
                if utils.get_row_count(df_live) == 0:
                    print(f"  {table_name}: no live rows — skipping")
                    continue
                
                self._merge_table(table_name, df_live, config)
                
            except Exception as e:
                utils.log_error(table_name, e)

        utils.log_summary("CDC", self.results, [])


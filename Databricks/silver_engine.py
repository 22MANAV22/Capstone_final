"""
Silver Engine — refactored to use utils.py
DQ is handled separately by run_dq.py (between Silver and Gold in DAG).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
import utils
from table_config import REFERENCE_TABLES, TRANSACTIONAL_TABLES
from table_config import S3_DELTA_BRONZE, S3_DELTA_SILVER


class SilverEngine:

    def __init__(self, spark):
        self.spark = spark
        self.results = {}

    def _clean_table(self, df, config):
        """Apply all cleaning rules from table_config."""
        for rule in config.get("cleaning_rules", []):
            df = utils.apply_cleaning_rule(df, rule)
        return utils.drop_audit_columns(df)

    def _write_merge(self, df, table_name, merge_keys):
        """MERGE into Silver Delta table."""
        s3_path = f"{S3_DELTA_SILVER}/{table_name}"

        df, before, after = utils.deduplicate(df, merge_keys)
        if before != after:
            print(f"    Deduped: {before:,} → {after:,} rows")

        try:
            target = DeltaTable.forPath(self.spark, s3_path)
            condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])

            (target.alias("t").merge(df.alias("s"), condition)
             .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

            utils.register_catalog(self.spark, f"silver.{table_name}", s3_path)
            count = utils.get_row_count(utils.read_delta(self.spark, s3_path))

        except Exception:
            # Table doesn't exist yet — first write
            utils.write_delta(df, s3_path, catalog_table=f"silver.{table_name}")
            count = utils.get_row_count(df)

        self.results[table_name] = count
        utils.log_table(table_name, count, "merged")

    def run(self, batch_number, tables_config):
        """Main entry point. Clean and merge only — DQ handled by run_dq.py."""
        utils.log_stage("SILVER ENGINE", batch_number)
        self.results = {}

        for table_name, config in tables_config.items():
            try:
                df = utils.read_delta(self.spark, f"{S3_DELTA_BRONZE}/{table_name}")
                df = df.filter(col("batch_id") == f"batch_{batch_number}")

                if utils.get_row_count(df) == 0:
                    print(f"  {table_name}: no rows — skipping")
                    continue

                # CLEAN only — no DQ here
                df = self._clean_table(df, config)

                merge_keys = config.get("merge_keys", [])
                if merge_keys:
                    self._write_merge(df, table_name, merge_keys)
                else:
                    utils.write_delta(df, f"{S3_DELTA_SILVER}/{table_name}",
                                     catalog_table=f"silver.{table_name}")
                    count = utils.get_row_count(df)
                    self.results[table_name] = count
                    utils.log_table(table_name, count, "cleaned")

            except Exception as e:
                utils.log_error(table_name, e)

        utils.log_summary("SILVER", self.results, [])
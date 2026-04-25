"""
Gold Engine — refactored to use utils.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg as _avg, datediff, when
import utils
from table_config import S3_DELTA_SILVER, S3_DELTA_GOLD, S3_SNOWFLAKE
from pyspark.sql.functions import col, count, sum as _sum, avg as _avg, datediff, when, max, unix_timestamp, from_unixtime

class GoldEngine:

    def __init__(self, spark):
        self.spark = spark
        self.results = {}

    def _write_gold(self, df, table_name):
        """Write to Delta (Gold) + Parquet (Snowflake)."""
        delta_path = f"{S3_DELTA_GOLD}/{table_name}"
        parquet_path = f"{S3_SNOWFLAKE}/{table_name}"
        
        utils.write_delta(df, delta_path, catalog_table=f"gold.{table_name}")
        utils.write_parquet(df, parquet_path)
        
        count = utils.get_row_count(df)
        self.results[table_name] = count
        utils.log_table(table_name, count, "built")

    def build_dim_customers(self):
        """Build customer dimension."""
        df = utils.read_delta(self.spark, f"{S3_DELTA_SILVER}/customers")
        self._write_gold(df, "dim_customers")

    def build_dim_sellers(self):
        """Build seller dimension with geo enrichment."""
        sellers = utils.read_delta(self.spark, f"{S3_DELTA_SILVER}/sellers")
        geo = utils.read_delta(self.spark, f"{S3_DELTA_SILVER}/geolocation")
        
        df = (sellers
            .join(geo, sellers.seller_zip_code_prefix == geo.geolocation_zip_code_prefix, "left")
            .select(
                "seller_id", "seller_city", "seller_state", "seller_zip_code_prefix",
                col("geolocation_lat").alias("seller_lat"),
                col("geolocation_lng").alias("seller_lng")
            ))
        
        self._write_gold(df, "dim_sellers")

    def build_dim_products(self):
        """Build product dimension."""
        df = utils.read_delta(self.spark, f"{S3_DELTA_SILVER}/products")
        self._write_gold(df, "dim_products")

    def build_dim_geolocation(self):
        """Build geolocation dimension."""
        df = utils.read_delta(self.spark, f"{S3_DELTA_SILVER}/geolocation")
        self._write_gold(df, "dim_geolocation")

    def build_fact_order_items(self):
        """Build central fact table."""
        orders = utils.read_delta(self.spark, f"{S3_DELTA_SILVER}/orders")
        items = utils.read_delta(self.spark, f"{S3_DELTA_SILVER}/order_items")
        payments = utils.read_delta(self.spark, f"{S3_DELTA_SILVER}/order_payments")
        reviews = utils.read_delta(self.spark, f"{S3_DELTA_SILVER}/order_reviews")

        pay_agg = payments.groupBy("order_id").agg(
            _sum("payment_value").alias("payment_value"),
            _avg("payment_installments").alias("avg_installments")
        )

        rev_agg = reviews.groupBy("order_id").agg(
            _avg("review_score").alias("review_score")
        )

        fact = (items.join(orders, "order_id")
                    .join(pay_agg, "order_id", "left")
                    .join(rev_agg, "order_id", "left")
                    .select(
                        "order_id", "order_item_id", "customer_id", "product_id", "seller_id",
                        "price", "freight_value",
                        (col("price") + col("freight_value")).alias("total_amount"),
                        "payment_value", "avg_installments", "order_status",
                        col("order_purchase_timestamp").alias("purchase_timestamp"),
                        col("order_delivered_customer_date").alias("delivered_date"),
                        "order_estimated_delivery_date",
                        col("review_score").alias("review_score"),
                        datediff(
                            col("order_delivered_customer_date"),
                            col("order_purchase_timestamp")
                        ).alias("delivery_days"),
                        # EXISTING
                        when(col("order_delivered_customer_date").isNull(), None)
                        .when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1)
                        .otherwise(0).alias("delay_flag"),
                        # NEW — on_time_flag (inverse of delay, null when not delivered)
                        when(col("order_delivered_customer_date").isNull(), None)
                        .when(col("order_delivered_customer_date") <= col("order_estimated_delivery_date"), 1)
                        .otherwise(0).alias("on_time_flag")
                    ))

        self._write_gold(fact, "fact_order_items")

    def build_order_summary(self):
        """Build order summary aggregates."""
        fact = utils.read_delta(self.spark, f"{S3_DELTA_GOLD}/fact_order_items")

        df = fact.groupBy(
            "order_id",
            "order_status",
            "purchase_timestamp",
            "delivered_date"
        ).agg(
            count("order_item_id").alias("total_items"),
            _sum("price").alias("total_price"),
            _sum("freight_value").alias("total_freight"),
            _sum("total_amount").alias("total_amount"),
            _avg("delivery_days").alias("delivery_days")
        )

        self._write_gold(df, "order_summary")

    def build_customer_metrics(self):
        """Build customer metrics."""
        fact = utils.read_delta(self.spark, f"{S3_DELTA_GOLD}/fact_order_items")
        df = fact.groupBy("customer_id").agg(
            count("order_id").alias("total_orders"),
            _sum("total_amount").alias("total_spent"),
            _avg("total_amount").alias("avg_order_value"),
            _avg("review_score").alias("avg_review_score"),
            from_unixtime(max(unix_timestamp(col("purchase_timestamp")))).alias("last_order_date"),
        )
        self._write_gold(df, "customer_metrics")

    def build_seller_performance(self):
        """Build seller performance."""
        fact = utils.read_delta(self.spark, f"{S3_DELTA_GOLD}/fact_order_items")
        df = fact.groupBy("seller_id").agg(
            count("order_id").alias("total_orders"),
            _sum("price").alias("total_revenue"),
            _avg("review_score").alias("avg_review_score"),
            _avg("delivery_days").alias("avg_delivery_days"),
            # NEW — on_time_delivery_rate as a percentage
            (_sum(when(col("on_time_flag") == 1, 1).otherwise(0)) * 100.0 / count("*"))
                .alias("on_time_delivery_rate"),
        )
        self._write_gold(df, "seller_performance")

    def build_product_performance(self):
        """Build product performance."""
        fact = utils.read_delta(self.spark, f"{S3_DELTA_GOLD}/fact_order_items")
        df = fact.groupBy("product_id").agg(
            count("*").alias("total_quantity"),
            _sum("price").alias("total_sales"),
            _avg("price").alias("avg_price"),
            _avg("review_score").alias("avg_review_score"),
        )
        self._write_gold(df, "product_performance")

    def run(self):
        """Main entry point."""
        utils.log_stage("GOLD ENGINE", "all")
        
        self.build_dim_customers()
        self.build_dim_sellers()
        self.build_dim_products()
        self.build_dim_geolocation()
        self.build_fact_order_items()
        self.build_order_summary()
        self.build_customer_metrics()
        self.build_seller_performance()
        self.build_product_performance()
        
        utils.log_summary("GOLD", self.results, [])
"""
Batch Pipeline DAG — Complete pipeline with all 9 Snowflake tables
Matches snowflake_setup.sql schema exactly
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/home/ubuntu/airflow/plugins')

from operators import DatabricksOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Configuration
BUCKET = "capstone-ecomm-team8"
SNS_ARN = "arn:aws:sns:us-east-1:868859238853:Capstone-team8"
SNOWFLAKE_CONN_ID = "snowflake_default"
NOTEBOOK_BASE_PATH = "/Workspace/Shared/Capstone_final/Databricks/Notebooks"

def move_files(**context):
    """Move files from staging/batch_N/ to raw/batch_N/"""
    batch_number = context['dag_run'].conf.get('batch_number', '2')
    hook = S3Hook(aws_conn_id="aws_default")
    
    source_prefix = f"staging/batch_{batch_number}/"
    keys = hook.list_keys(bucket_name=BUCKET, prefix=source_prefix) or []
    files = [k for k in keys if not k.endswith("/")]
    
    print(f"Found {len(files)} files in {source_prefix}")
    
    for source_key in files:
        filename = source_key.split("/")[-1]
        dest_key = f"raw/batch_{batch_number}/{filename}"
        
        hook.copy_object(
            source_bucket_name=BUCKET,
            source_bucket_key=source_key,
            dest_bucket_name=BUCKET,
            dest_bucket_key=dest_key,
        )
        hook.delete_objects(bucket=BUCKET, keys=[source_key])
        print(f"✓ Moved: {filename}")
    
    return len(files)

def sense_files(**context):
    """Check if files exist in raw/batch_N/"""
    batch_number = context['dag_run'].conf.get('batch_number', '2')
    hook = S3Hook(aws_conn_id="aws_default")
    
    prefix = f"raw/batch_{batch_number}/"
    keys = hook.list_keys(bucket_name=BUCKET, prefix=prefix) or []
    files = [k for k in keys if not k.endswith("/")]
    
    print(f"Found {len(files)} files in {prefix}")
    return len(files) > 0

def notify_complete(**context):
    """Send completion notification"""
    batch_number = context['dag_run'].conf.get('batch_number', '2')
    print(f"✅ Batch {batch_number} pipeline complete!")
    
    hook = S3Hook(aws_conn_id="aws_default")
    creds = hook.get_credentials()
    
    import boto3
    sns = boto3.client(
        "sns",
        region_name="us-east-1",
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
    )
    
    sns.publish(
        TopicArn=SNS_ARN,
        Subject=f"✅ Batch {batch_number} Complete",
        Message=f"Batch pipeline completed at {datetime.now()}"
    )

default_args = {
    "owner": "capstone-team8",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "batch_pipeline",
    default_args=default_args,
    description="Complete E-commerce pipeline: Bronze → Silver → DQ → Gold → Snowflake (9 tables)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "batch", "production"],
) as dag:
    
    # ═══════════════════════════════════════════════════════════════════════
    # PART 1: DATA MOVEMENT & DATABRICKS PROCESSING
    # ═══════════════════════════════════════════════════════════════════════
    
    move = PythonOperator(task_id="move_files", python_callable=move_files)
    
    sense = PythonSensor(
        task_id="sense_files",
        python_callable=sense_files,
        poke_interval=30,
        timeout=300,
    )
    
    bronze = DatabricksOperator(
        task_id="run_bronze",
        notebook_path=f"{NOTEBOOK_BASE_PATH}/run_bronze",
        base_parameters={"batch_number": '{{ dag_run.conf.get("batch_number", "2") }}'},
    )
    
    silver = DatabricksOperator(
        task_id="run_silver",
        notebook_path=f"{NOTEBOOK_BASE_PATH}/run_silver",
        base_parameters={"batch_number": '{{ dag_run.conf.get("batch_number", "2") }}'},
    )
    
    dq = DatabricksOperator(
        task_id="run_dq",
        notebook_path=f"{NOTEBOOK_BASE_PATH}/run_dq",
        base_parameters={"batch_number": '{{ dag_run.conf.get("batch_number", "2") }}'},
    )
    
    gold = DatabricksOperator(
        task_id="run_gold",
        notebook_path=f"{NOTEBOOK_BASE_PATH}/run_gold",
    )
    
    # ═══════════════════════════════════════════════════════════════════════
    # PART 2: SNOWFLAKE LOADS - 9 TABLES (4 Dims + 1 Fact + 4 Aggregates)
    # Load order: Dims first, then Fact, then Aggregates (all parallel)
    # ═══════════════════════════════════════════════════════════════════════
    
    # ── DIMENSION TABLES (load first - fact references them) ──────────────
    
    sf_dim_customers = SQLExecuteQueryOperator(
        task_id="sf_load_dim_customers",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            TRUNCATE TABLE dim_customers;
            COPY INTO dim_customers
            FROM @gold_s3_stage/dim_customers/
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            ON_ERROR = 'CONTINUE';
        """,
    )
    
    sf_dim_sellers = SQLExecuteQueryOperator(
        task_id="sf_load_dim_sellers",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            TRUNCATE TABLE dim_sellers;
            COPY INTO dim_sellers
            FROM @gold_s3_stage/dim_sellers/
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            ON_ERROR = 'CONTINUE';
        """,
    )
    
    sf_dim_products = SQLExecuteQueryOperator(
        task_id="sf_load_dim_products",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            TRUNCATE TABLE dim_products;
            COPY INTO dim_products
            FROM @gold_s3_stage/dim_products/
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            ON_ERROR = 'CONTINUE';
        """,
    )
    
    sf_dim_geolocation = SQLExecuteQueryOperator(
        task_id="sf_load_dim_geolocation",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            TRUNCATE TABLE dim_geolocation;
            COPY INTO dim_geolocation
            FROM @gold_s3_stage/dim_geolocation/
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            ON_ERROR = 'CONTINUE';
        """,
    )
    
    # ── FACT TABLE (load after dims) ──────────────────────────────────────
    
    sf_fact_order_items = SQLExecuteQueryOperator(
        task_id="sf_load_fact_order_items",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            TRUNCATE TABLE fact_order_items;
            COPY INTO fact_order_items
            FROM @gold_s3_stage/fact_order_items/
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            ON_ERROR = 'CONTINUE';
        """,
    )
    
    # ── AGGREGATE TABLES (load after fact) ────────────────────────────────
    
    sf_order_summary = SQLExecuteQueryOperator(
        task_id="sf_load_order_summary",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            TRUNCATE TABLE order_summary;
            COPY INTO order_summary
            FROM @gold_s3_stage/order_summary/
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            ON_ERROR = 'CONTINUE';
        """,
    )
    
    sf_customer_metrics = SQLExecuteQueryOperator(
        task_id="sf_load_customer_metrics",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            TRUNCATE TABLE customer_metrics;
            COPY INTO customer_metrics
            FROM @gold_s3_stage/customer_metrics/
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            ON_ERROR = 'CONTINUE';
        """,
    )
    
    sf_seller_performance = SQLExecuteQueryOperator(
        task_id="sf_load_seller_performance",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            TRUNCATE TABLE seller_performance;
            COPY INTO seller_performance
            FROM @gold_s3_stage/seller_performance/
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            ON_ERROR = 'CONTINUE';
        """,
    )
    
    sf_product_performance = SQLExecuteQueryOperator(
        task_id="sf_load_product_performance",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            TRUNCATE TABLE product_performance;
            COPY INTO product_performance
            FROM @gold_s3_stage/product_performance/
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE
            ON_ERROR = 'CONTINUE';
        """,
    )
    
    # ── NOTIFICATION ───────────────────────────────────────────────────────
    
    notify = PythonOperator(
        task_id="notify_complete",
        python_callable=notify_complete,
    )
    
    # ═══════════════════════════════════════════════════════════════════════
    # TASK DEPENDENCIES
    # ═══════════════════════════════════════════════════════════════════════
    
    # Linear flow through Databricks
    move >> sense >> bronze >> silver >> dq >> gold
    
    # Snowflake loads in 3 stages: Dims → Fact → Aggregates
    gold >> [sf_dim_customers, sf_dim_sellers, sf_dim_products, sf_dim_geolocation]
    
    [sf_dim_customers, sf_dim_sellers, sf_dim_products, sf_dim_geolocation] >> sf_fact_order_items
    
    sf_fact_order_items >> [
        sf_order_summary,
        sf_customer_metrics,
        sf_seller_performance,
        sf_product_performance
    ]
    
    [sf_order_summary, sf_customer_metrics, sf_seller_performance, sf_product_performance] >> notify
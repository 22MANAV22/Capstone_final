"""
Live Merge DAG — Process live stream and load all 9 Snowflake tables
Matches snowflake_setup.sql schema exactly
"""

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator, PythonOperator
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

def check_live_files():
    """Check if there are files in live/ directory"""
    hook = S3Hook(aws_conn_id="aws_default")
    keys = hook.list_keys(bucket_name=BUCKET, prefix="live/") or []
    files = [k for k in keys if not k.endswith("/")]
    
    print(f"Found {len(files)} files in live/")
    return len(files) > 0

def archive_live_files(**context):
    """Archive processed live files to prevent duplicate processing"""
    hook = S3Hook(aws_conn_id="aws_default")
    
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    archive_prefix = f"live_archive/{timestamp}/"
    
    keys = hook.list_keys(bucket_name=BUCKET, prefix="live/") or []
    files = [k for k in keys if not k.endswith("/")]
    
    print(f"Archiving {len(files)} files from live/")
    
    for source_key in files:
        filename = source_key.split("/")[-1]
        dest_key = f"{archive_prefix}{filename}"
        
        hook.copy_object(
            source_bucket_name=BUCKET,
            source_bucket_key=source_key,
            dest_bucket_name=BUCKET,
            dest_bucket_key=dest_key,
        )
        hook.delete_objects(bucket=BUCKET, keys=[source_key])
        print(f"✓ Archived: {filename}")
    
    print(f"All files archived to: s3://{BUCKET}/{archive_prefix}")
    return len(files)

def notify_success(**context):
    """Send success notification"""
    print("✅ Live merge completed successfully")
    
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
        Subject="✅ Live Merge Complete",
        Message=f"Live merge completed at {datetime.now()}"
    )

default_args = {
    "owner": "capstone-team8",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "live_merge",
    default_args=default_args,
    description="Process live stream and refresh all 9 Snowflake tables",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "live", "streaming"],
) as dag:
    
    check = ShortCircuitOperator(
        task_id="check_live_files",
        python_callable=check_live_files,
    )
    
    bronze = DatabricksOperator(
        task_id="bronze_ingest_live",
        notebook_path=f"{NOTEBOOK_BASE_PATH}/run_bronze",
        base_parameters={"batch_number": "live"},
    )
    
    cdc = DatabricksOperator(
        task_id="cdc_merge",
        notebook_path=f"{NOTEBOOK_BASE_PATH}/run_cdc",
    )
    
    gold = DatabricksOperator(
        task_id="rebuild_gold",
        notebook_path=f"{NOTEBOOK_BASE_PATH}/run_gold",
    )
    
    archive = PythonOperator(
        task_id="archive_live_files",
        python_callable=archive_live_files,
    )
    
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
    
    notify = PythonOperator(
        task_id="notify_complete",
        python_callable=notify_success,
    )
    
    # Task dependencies
    check >> bronze >> cdc >> gold >> archive
    
    archive >> [sf_dim_customers, sf_dim_sellers, sf_dim_products, sf_dim_geolocation]
    
    [sf_dim_customers, sf_dim_sellers, sf_dim_products, sf_dim_geolocation] >> sf_fact_order_items
    
    sf_fact_order_items >> [
        sf_order_summary,
        sf_customer_metrics,
        sf_seller_performance,
        sf_product_performance
    ]
    
    [sf_order_summary, sf_customer_metrics, sf_seller_performance, sf_product_performance] >> notify
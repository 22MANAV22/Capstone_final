"""
Live Merge DAG — ONLY imports + DAG definition.
"""

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/home/ubuntu/airflow/plugins')

from operators import SNSOperator, DatabricksOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def check_live_files():
    hook = S3Hook(aws_conn_id="aws_default")
    keys = hook.list_keys(bucket_name="capstone-ecomm-team8", prefix="live/") or []
    files = [k for k in keys if not k.endswith("/")]
    return len(files) > 0

default_args = {
    "owner": "capstone-team8",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "live_merge",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "live"],
    is_paused_upon_creation=True,
) as dag:
    
    check = ShortCircuitOperator(
        task_id="check_live_files",
        python_callable=check_live_files,
    )
    
    bronze = DatabricksOperator(
        task_id="bronze_ingest_live",
        notebook_path="/Shared/Capstone/run_bronze",
        base_parameters={"batch_number": "live"},
    )
    
    cdc = DatabricksOperator(
        task_id="cdc_merge",
        notebook_path="/Shared/Capstone/run_cdc",
    )
    
    gold = DatabricksOperator(
        task_id="rebuild_gold",
        notebook_path="/Shared/Capstone/run_gold",
    )
    
    snowflake = DatabricksOperator(
        task_id="load_snowflake",
        notebook_path="/Shared/Capstone/load_snowflake",
    )
    
    notify = SNSOperator(
        task_id="notify_complete",
        sns_arn="arn:aws:sns:us-east-1:868859238853:Capstone-team8",
        subject="✅ Live merge complete",
        message="Live data updated at {{ ts }}",
    )
    
    check >> bronze >> cdc >> gold >> snowflake >> notify

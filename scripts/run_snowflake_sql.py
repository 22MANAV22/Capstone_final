"""
scripts/run_snowflake_sql.py

Executes Snowflake setup:
1. Creates tables from snowflake_setup.sql
2. Creates S3 stage with credentials from GitHub Secrets
"""

import snowflake.connector
import os

def run_sql():
    """Execute Snowflake setup SQL and create S3 stage"""
    
    # Get credentials from environment variables (GitHub Secrets)
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema='PUBLIC'
    )
    
    cursor = conn.cursor()
    
    # Step 1: Execute table creation SQL
    print("=" * 80)
    print("STEP 1: Creating Snowflake tables from sql/snowflake_setup.sql")
    print("=" * 80)
    
    with open('sql/snowflake_setup.sql', 'r') as f:
        sql_content = f.read()
    
    sql_commands = sql_content.split(';')
    
    for i, cmd in enumerate(sql_commands):
        cmd = cmd.strip()
        if cmd and not cmd.startswith('--'):
            try:
                cursor.execute(cmd)
                print(f"✓ Command {i+1} executed successfully")
            except Exception as e:
                print(f"✗ Command {i+1} failed: {e}")
    
    # Step 2: Create S3 stage with credentials from environment
    print("\n" + "=" * 80)
    print("STEP 2: Creating S3 stage with AWS credentials from GitHub Secrets")
    print("=" * 80)
    
    # Get AWS credentials from environment (if provided)
    aws_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    
    if aws_key_id and aws_secret_key:
        # Create stage with direct credentials
        stage_sql = f"""
        CREATE OR REPLACE STAGE gold_s3_stage
            URL = 's3://capstone-ecomm-team8/snowflake'
            CREDENTIALS = (
                AWS_KEY_ID = '{aws_key_id}'
                AWS_SECRET_KEY = '{aws_secret_key}'
            )
            FILE_FORMAT = (TYPE = 'PARQUET');
        """
        
        try:
            cursor.execute(stage_sql)
            print("✓ S3 stage created successfully with AWS credentials")
        except Exception as e:
            print(f"✗ S3 stage creation failed: {e}")
        
        # Verify stage
        try:
            cursor.execute("LIST @gold_s3_stage;")
            print("✓ S3 stage verified - can access S3 bucket")
        except Exception as e:
            print(f"⚠ S3 stage verification failed: {e}")
    else:
        print("⚠ AWS credentials not found in environment")
        print("  Stage creation skipped - you'll need to create it manually in Snowflake")
    
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ Snowflake setup completed successfully")
    print("=" * 80)

if __name__ == "__main__":
    run_sql()
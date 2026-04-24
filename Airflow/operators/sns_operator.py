"""SNS + S3 file operations in one operator."""

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import boto3

class SNSOperator(BaseOperator):
    """Send SNS notifications + S3 file ops."""
    
    def __init__(self, sns_arn=None, subject=None, message=None, 
                 s3_operation=None, bucket=None, source_key=None, dest_key=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.sns_arn = sns_arn
        self.subject = subject
        self.message = message
        self.s3_operation = s3_operation
        self.bucket = bucket
        self.source_key = source_key
        self.dest_key = dest_key
    
    def execute(self, context):
        """Execute SNS or S3 operation."""
        
        if self.sns_arn:
            # SNS notification
            try:
                hook = S3Hook(aws_conn_id="aws_default")
                creds = hook.get_credentials()
                sns = boto3.client(
                    "sns",
                    region_name="us-east-1",
                    aws_access_key_id=creds.access_key,
                    aws_secret_access_key=creds.secret_key,
                )
                sns.publish(
                    TopicArn=self.sns_arn,
                    Subject=self.subject[:100],
                    Message=self.message
                )
                self.log.info(f"✓ SNS sent: {self.subject}")
            except Exception as e:
                self.log.error(f"✗ SNS failed: {e}")
        
        if self.s3_operation:
            # S3 operations
            hook = S3Hook(aws_conn_id="aws_default")
            
            if self.s3_operation == "copy":
                hook.copy_object(
                    source_bucket_name=self.bucket,
                    source_bucket_key=self.source_key,
                    dest_bucket_name=self.bucket,
                    dest_bucket_key=self.dest_key,
                )
                self.log.info(f"✓ Copied: {self.source_key} → {self.dest_key}")
            
            elif self.s3_operation == "delete":
                hook.delete_objects(bucket=self.bucket, keys=[self.source_key])
                self.log.info(f"✓ Deleted: {self.source_key}")
            
            elif self.s3_operation == "list":
                keys = hook.list_keys(bucket_name=self.bucket, prefix=self.source_key) or []
                files = [k for k in keys if not k.endswith("/")]
                self.log.info(f"✓ Found {len(files)} files in {self.source_key}")
                return files

"""Databricks notebook execution wrapper."""

from airflow.models import BaseOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

class DatabricksOperator(BaseOperator):
    """Simple wrapper for Databricks notebook execution."""
    
    def __init__(self, notebook_path, base_parameters=None, **kwargs):
        super().__init__(**kwargs)
        self.notebook_path = notebook_path
        self.base_parameters = base_parameters or {}
    
    def execute(self, context):
        """Execute notebook."""
        op = DatabricksSubmitRunOperator(
            task_id=self.task_id,
            databricks_conn_id="databricks_default",
            notebook_task={
                "notebook_path": self.notebook_path,
                "base_parameters": self.base_parameters,
            },
        )
        return op.execute(context)
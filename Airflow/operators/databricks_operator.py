"""Databricks notebook execution wrapper — serverless compute."""
from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook


class DatabricksOperator(BaseOperator):
    """
    Runs a Databricks notebook on serverless compute.
    Uses the /api/2.1/jobs/runs/submit endpoint with a tasks array
    (required for serverless — single-task submit does not support serverless).
    """

    def __init__(self, notebook_path, base_parameters=None, **kwargs):
        super().__init__(**kwargs)
        self.notebook_path = notebook_path
        self.base_parameters = base_parameters or {}

    def execute(self, context):
        hook = DatabricksHook(databricks_conn_id="databricks_default")

        run_config = {
            "run_name": f"airflow_{self.task_id}",
            "tasks": [
                {
                    "task_key": self.task_id,
                    "notebook_task": {
                        "notebook_path": self.notebook_path,
                        "base_parameters": self.base_parameters,
                    },
                    # No cluster spec = serverless compute
                }
            ],
            "queue": {"enabled": True},
        }

        run_id = hook.submit_run(run_config)
        self.log.info(f"Submitted Databricks run_id={run_id}")

        # Wait for completion
        hook.wait_for_run(run_id, verbose=True)

        run_state = hook.get_run_state(run_id)
        self.log.info(f"Run state: {run_state}")

        if not run_state.is_successful:
            raise Exception(
                f"Databricks notebook failed. run_id={run_id}, state={run_state}"
            )

        return run_id

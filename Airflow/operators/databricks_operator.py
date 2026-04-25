"""Databricks notebook execution wrapper — serverless compute."""
import time
from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook


# Terminal states from Databricks API
TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
SUCCESS_STATE = "SUCCESS"

class DatabricksOperator(BaseOperator):
    
    # ADD THESE TWO LINES — tells Airflow to render Jinja in these fields
    template_fields = ("notebook_path", "base_parameters")
    template_fields_renderers = {"base_parameters": "json"}

    def __init__(self, notebook_path, base_parameters=None, poll_interval=30, **kwargs):
        super().__init__(**kwargs)
        self.notebook_path = notebook_path
        self.base_parameters = base_parameters or {}
        self.poll_interval = poll_interval

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
                    # No cluster key = serverless compute
                }
            ],
            "queue": {"enabled": True},
        }

        run_id = hook.submit_run(run_config)
        self.log.info(f"Submitted Databricks run_id={run_id} for notebook {self.notebook_path}")

        # Poll until terminal state
        while True:
            run_info = hook.get_run(run_id)
            life_cycle_state = run_info["state"]["life_cycle_state"]
            result_state = run_info["state"].get("result_state", "")
            state_message = run_info["state"].get("state_message", "")

            self.log.info(f"run_id={run_id} | state={life_cycle_state} | result={result_state} | msg={state_message}")

            if life_cycle_state in TERMINAL_STATES:
                if result_state == SUCCESS_STATE:
                    self.log.info(f"Run {run_id} completed successfully.")
                    return run_id
                else:
                    raise Exception(
                        f"Databricks run failed. run_id={run_id}, "
                        f"life_cycle_state={life_cycle_state}, "
                        f"result_state={result_state}, "
                        f"message={state_message}"
                    )

            time.sleep(self.poll_interval)
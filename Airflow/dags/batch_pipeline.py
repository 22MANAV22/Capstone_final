"""
batch_pipeline.py — updated to include run_dq between Silver and Gold.
Only the task definitions and wire section change.
Everything else (SNS, S3, class, __init__) stays identical.
"""

# ── Only the relevant section shown — paste into your existing batch_pipeline.py ──

# Inside build_dag(), replace the task definitions + wire section with this:

            bronze = self._db_task(
                "run_bronze", "run_bronze",
                extra_params={
                    "batch_number": '{{ dag_run.conf.get("batch_number", "2") }}'
                }
            )

            silver = self._db_task(
                "run_silver", "run_silver",
                extra_params={
                    "batch_number": '{{ dag_run.conf.get("batch_number", "2") }}'
                }
            )

            # ── NEW: DQ checks between Silver and Gold ────────────────────────
            dq = self._db_task(
                "run_dq", "run_dq",
                extra_params={
                    "batch_number": '{{ dag_run.conf.get("batch_number", "2") }}'
                }
            )
            # ─────────────────────────────────────────────────────────────────

            gold = self._db_task("run_gold", "run_gold")

            sf_load = self.snowflake_load()

            notify = PythonOperator(
                task_id="notify_complete",
                python_callable=self.notify_complete,
            )

            # ── Wire — DQ sits between Silver and Gold ────────────────────────
            move >> sense >> bronze >> silver >> dq >> gold >> sf_load >> notify
            #                                      ^^^^ only change to the wire
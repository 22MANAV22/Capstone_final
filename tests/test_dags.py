import pytest
import sys
import os

# Add Airflow dags to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'Airflow'))

from airflow.models import DagBag

def test_dag_loaded():
    """Test that DAGs are loaded without errors"""
    dagbag = DagBag(dag_folder='Airflow/dags/', include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    assert len(dagbag.dags) >= 2, "Expected at least 2 DAGs"

def test_batch_pipeline_dag():
    """Test batch_pipeline DAG structure"""
    dagbag = DagBag(dag_folder='Airflow/dags/', include_examples=False)
    dag = dagbag.get_dag('batch_pipeline')
    assert dag is not None
    assert len(dag.tasks) > 0

def test_live_merge_dag():
    """Test live_merge DAG structure"""
    dagbag = DagBag(dag_folder='Airflow/dags/', include_examples=False)
    dag = dagbag.get_dag('live_merge')
    assert dag is not None
    assert len(dag.tasks) > 0
import pytest
from airflow.models import DagBag
from datetime import datetime


def test_dag_loaded():
    """Test that the DAG can be loaded properly"""
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag(dag_id="dag_bronze_breweries_ingestion")
    assert dag is not None
    assert len(dag_bag.import_errors) == 0


def test_dag_structure():
    """Test the DAG structure and task dependencies"""
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag(dag_id="dag_bronze_breweries_ingestion")

    # Check task IDs
    task_ids = [task.task_id for task in dag.tasks]
    assert "ingest_breweries_data" in task_ids
    assert "trigger_silver_dag" in task_ids

    # Check task dependencies
    ingest_task = dag.get_task("ingest_breweries_data")
    trigger_task = dag.get_task("trigger_silver_dag")

    # Verify that ingest task triggers the silver DAG
    assert trigger_task in ingest_task.downstream_list

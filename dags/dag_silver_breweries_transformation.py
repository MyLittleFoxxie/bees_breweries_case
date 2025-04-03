from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


@dag(
    schedule=None,  # Will be triggered by bronze DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["breweries", "spark", "transformations", "silver"],
)
def dag_silver_breweries_transformation():
    transform_data_to_silver = SparkSubmitOperator(
        task_id="dag_transform_data_to_silver",
        application="./include/scripts/script_silver_breweries_transformation.py",
        conn_id="my_spark_conn",
        verbose=True,
        packages="io.delta:delta-spark_2.12:3.3.0",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
    )

    # Trigger gold DAG after silver transformation completes
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_dag",
        trigger_dag_id="dag_gold_breweries_aggregation",
        wait_for_completion=False,
    )

    transform_data_to_silver >> trigger_gold


dag_silver_breweries_transformation()

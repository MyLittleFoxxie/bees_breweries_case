from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    schedule=None,
    catchup=False,
    tags=["breweries", "spark", "transformations", "gold"],
)
def dag_gold_breweries_aggregation():
    transform_data_to_gold = SparkSubmitOperator(
        task_id="dag_gold_breweries_aggregation",
        application="./include/scripts/script_gold_breweries_aggregation.py",
        conn_id="my_spark_conn",
        verbose=True,
        packages="io.delta:delta-spark_2.12:3.3.0",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        },
    )

    transform_data_to_gold


dag_gold_breweries_aggregation()

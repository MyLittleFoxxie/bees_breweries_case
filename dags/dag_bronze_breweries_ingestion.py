from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from loguru import logger

# Configure loguru to write to both file and console
log_dir = "/usr/local/airflow/include/logs"
os.makedirs(log_dir, exist_ok=True)
current_date = datetime.now()
log_file = os.path.join(
    log_dir, f"dag_bronze_breweries_ingestion_{current_date.strftime('%Y%m%d')}.log"
)

# Remove default handler and add custom handlers
logger.remove()
logger.add(
    log_file,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO",
)
logger.add(
    lambda msg: print(msg),
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
)

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define the dag
dag = DAG(
    "dag_bronze_breweries_ingestion",
    default_args=default_args,
    description="Ingest breweries data into the bronze layer",
    schedule_interval="@daily",
    catchup=False,
    tags=["breweries", "ingestion", "bronze"],
)


def ingest_bronze_breweries_data(**context):
    import pandas as pd
    from loguru import logger
    import requests as req
    import time
    import math

    start_time = time.time()
    pages_processed = 0

    try:
        # Get the current date
        current_date = datetime.now()
        current_date_str = current_date.strftime("%Y-%m-%d")
        logger.info(f"Starting breweries data ingestion for date: {current_date_str}")

        # Use the breweries metadata api to get the total number of breweries
        response = req.get("https://api.openbrewerydb.org/v1/breweries/meta")
        total_breweries = response.json()["total"]
        per_page = 100
        total_pages = math.ceil(total_breweries / per_page)

        logger.info(
            f"Total breweries to process: {total_breweries} across {total_pages} pages"
        )

        # Get the data from the breweries api
        all_data = []
        for page in range(1, int(total_pages) + 1):
            try:
                logger.info(f"Processing page {page} of {total_pages:.0f}")
                response = req.get(
                    f"https://api.openbrewerydb.org/v1/breweries?page={page}&per_page={per_page}&sort=type,name:asc"
                )
                response.raise_for_status()  # Raise an exception for bad status codes

                df = pd.DataFrame(response.json())
                df["ingested_date"] = pd.Timestamp.now()
                all_data.append(df)
                pages_processed += 1

            except req.exceptions.RequestException as e:
                logger.error(f"Error processing page {page}: {str(e)}")
                raise

        # Combine all data into a single DataFrame
        final_df = pd.concat(all_data, ignore_index=True)

        # Save the complete dataset
        output_path = f"/usr/local/airflow/include/data/bronze/openbrewerydb/breweries/full/year={current_date.year}/month={current_date.month}/day={current_date.day}/{current_date.strftime('%Y%m%d_%H%M%S')}_breweries.json"

        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Log the output path for debugging
        logger.info(f"Saving data to: {output_path}")

        # Convert DataFrame to JSON and save with proper formatting
        final_df.to_json(output_path, orient="records", date_format="iso")

        logger.success(f"Successfully saved {len(final_df)} records to {output_path}")
        execution_time = time.time() - start_time
        logger.info(
            f"""
        Ingestion completed successfully:
        - Total records processed: {len(final_df)}
        - Total execution time: {execution_time:.2f} seconds
        """
        )

        # Store metrics in XCom for monitoring
        context["task_instance"].xcom_push(key="records_processed", value=len(final_df))
        context["task_instance"].xcom_push(key="execution_time", value=execution_time)

    except Exception as e:
        logger.exception("Failed to ingest breweries data: {}", str(e))
        raise


# Define the task that will be used to ingest the data into the bronze layer
ingest_bronze_breweries_data_task = PythonOperator(
    task_id="ingest_breweries_data",
    python_callable=ingest_bronze_breweries_data,
    dag=dag,
)

# Trigger silver DAG after successful ingestion
trigger_silver = TriggerDagRunOperator(
    task_id="trigger_silver_dag",
    trigger_dag_id="dag_silver_breweries_transformation",
    wait_for_completion=False,
    dag=dag,
)

# Set up the task dependencies
ingest_bronze_breweries_data_task >> trigger_silver

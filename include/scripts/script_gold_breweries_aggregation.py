from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    min,
    max,
    first,
    last,
    lit,
    when,
    to_date,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)
import os
from datetime import datetime
from loguru import logger

# Configure loguru for main script logging
log_dir = "/usr/local/airflow/include/logs"
os.makedirs(log_dir, exist_ok=True)
current_date = datetime.now()
log_file = os.path.join(
    log_dir,
    f"script_gold_breweries_star_schema_{current_date.strftime('%Y%m%d')}.log",
)

logger.remove()  # Remove default handler
logger.add(
    log_file,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level="INFO",
)
logger.add(
    lambda msg: print(msg),
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    level="DEBUG",
)


def main():
    logger.info("Starting gold layer transformation process")

    # Initialize Spark session with Delta Lake support
    spark = (
        SparkSession.builder.appName("Breweries Gold Layer Transformation")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    # Get Spark logger
    sc = spark.sparkContext
    sc.setLogLevel("INFO")

    logger.info("Spark session initialized successfully")

    # Define input and output paths
    silver_path = "/usr/local/airflow/include/data/prata/openbrewerydb"
    gold_path = "/usr/local/airflow/include/data/ouro/openbrewerydb"

    # Create output directories if they don't exist
    try:
        # Create parent directory first with proper permissions
        os.makedirs(gold_path, exist_ok=True)
        os.chmod(gold_path, 0o777)  # Set permissions to 777

        # Create subdirectories
        output_dirs = [
            f"{gold_path}/dim_location",
            f"{gold_path}/dim_brewery_type",
            f"{gold_path}/fact_breweries",
            f"{gold_path}/agg_breweries_by_type_location",
        ]

        for dir_path in output_dirs:
            os.makedirs(dir_path, exist_ok=True)
            os.chmod(dir_path, 0o777)  # Set permissions to 777
            logger.info(f"Created directory: {dir_path}")
    except Exception as e:
        logger.error(f"Failed to create directories: {str(e)}")
        raise

    try:
        # Read the silver layer Delta table
        logger.info(f"Reading silver layer data from: {silver_path}")
        silver_df = spark.read.format("delta").load(silver_path)

        # Create dimension tables
        logger.info("Creating dimension tables")

        # Location Dimension
        location_dim = silver_df.select(
            col("country").alias("country_key"),
            col("state").alias("state_key"),
            col("city").alias("city_key"),
            col("country").alias("country_name"),
            col("state").alias("state_name"),
            col("city").alias("city_name"),
            col("postal_code").alias("postal_code"),
            col("longitude").alias("longitude"),
            col("latitude").alias("latitude"),
        ).distinct()

        # Brewery Type Dimension
        brewery_type_dim = silver_df.select(
            col("brewery_type").alias("brewery_type_key"),
            col("brewery_type").alias("brewery_type_name"),
        ).distinct()

        # Create fact table
        logger.info("Creating fact table")
        fact_breweries = silver_df.select(
            col("id").alias("brewery_id"),
            col("name").alias("brewery_name"),
            col("brewery_type").alias("brewery_type_key"),
            col("country").alias("country_key"),
            col("state").alias("state_key"),
            col("city").alias("city_key"),
            col("phone").alias("phone"),
            col("website_url").alias("website_url"),
            col("updated_at").alias("updated_at"),
            col("updated_date").alias("updated_date"),
            col("valid_from").alias("valid_from"),
            col("valid_to").alias("valid_to"),
            col("is_current").alias("is_current"),
        )

        # Create aggregated view for breweries by type and location
        logger.info("Creating aggregated view")
        breweries_aggregated = silver_df.groupBy(
            "country", "state", "city", "brewery_type"
        ).agg(
            count("*").alias("brewery_count"), first("updated_at").alias("last_updated")
        )

        # Write dimension tables to Delta format
        logger.info("Writing dimension tables")
        location_dim.write.format("delta").mode("overwrite").save(
            f"{gold_path}/dim_location"
        )
        brewery_type_dim.write.format("delta").mode("overwrite").save(
            f"{gold_path}/dim_brewery_type"
        )

        # Write fact table to Delta format
        logger.info("Writing fact table")
        fact_breweries.write.format("delta").mode("overwrite").save(
            f"{gold_path}/fact_breweries"
        )

        # Write aggregated view to Delta format
        logger.info("Writing aggregated view")
        breweries_aggregated.write.format("delta").mode("overwrite").save(
            f"{gold_path}/agg_breweries_by_type_location"
        )

        # Create views for easier querying
        logger.info("Creating SQL views")
        spark.sql(
            f"CREATE OR REPLACE VIEW breweries_star_schema AS "
            f"SELECT f.*, "
            f"l.country_name, l.state_name, l.city_name, l.postal_code, l.longitude, l.latitude, "
            f"t.brewery_type_name "
            f"FROM delta.`{gold_path}/fact_breweries` f "
            f"JOIN delta.`{gold_path}/dim_location` l "
            f"ON f.country_key = l.country_key AND f.state_key = l.state_key AND f.city_key = l.city_key "
            f"JOIN delta.`{gold_path}/dim_brewery_type` t "
            f"ON f.brewery_type_key = t.brewery_type_key"
        )

        spark.sql(
            f"CREATE OR REPLACE VIEW breweries_aggregated AS "
            f"SELECT * FROM delta.`{gold_path}/agg_breweries_by_type_location`"
        )

        # Log success
        logger.info("Gold layer transformation completed successfully")
        logger.info(f"Location dimension count: {location_dim.count()}")
        logger.info(f"Brewery type dimension count: {brewery_type_dim.count()}")
        logger.info(f"Fact table count: {fact_breweries.count()}")
        logger.info(f"Aggregated view count: {breweries_aggregated.count()}")

    except Exception as e:
        logger.error(f"Error during gold layer transformation: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        logger.error(f"Error traceback: {e.__traceback__}")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"An error occurred during execution: {str(e)}")
        raise

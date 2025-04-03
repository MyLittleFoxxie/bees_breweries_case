# read.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    lit,
    when,
    concat,
    regexp_replace,
    count,
    udf,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
)
import os
from datetime import datetime
from loguru import logger
import unicodedata
import re
from pyspark import SparkContext

# Configure loguru for main script logging
log_dir = "/usr/local/airflow/include/logs"
os.makedirs(log_dir, exist_ok=True)
current_date = datetime.now()
log_file = os.path.join(
    log_dir,
    f"script_silver_breweries_transformation_{current_date.strftime('%Y%m%d')}.log",
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


def sanitize_path_component(value):
    """
    Sanitize a string to be used as a path component.

    This function:
    1. Normalizes Unicode characters to their ASCII equivalents
    2. Replaces special characters with underscores
    3. Ensures the result is a valid path component

    Args:
        value (str): The string to sanitize

    Returns:
        str: A sanitized string suitable for use in file paths
    """
    if value is None:
        return "no_value"

    try:
        # Convert to string if not already
        if not isinstance(value, str):
            value = str(value)

        # Normalize Unicode characters (NFKD form)
        normalized = unicodedata.normalize("NFKD", value)

        # Replace special characters with underscores
        # First, replace known problematic characters
        replacements = {
            # German umlauts - ensure these are converted first
            "ö": "oe",
            "Ö": "Oe",
            "ä": "ae",
            "Ä": "Ae",
            "ü": "ue",
            "Ü": "Ue",
            "ß": "ss",
            # French accents
            "à": "a",
            "â": "a",
            "ç": "c",
            "é": "e",
            "è": "e",
            "ê": "e",
            "ë": "e",
            "î": "i",
            "ï": "i",
            "ô": "o",
            "ù": "u",
            "û": "u",
            "ÿ": "y",
            "À": "A",
            "Â": "A",
            "Ç": "C",
            "É": "E",
            "È": "E",
            "Ê": "E",
            "Ë": "E",
            "Î": "I",
            "Ï": "I",
            "Ô": "O",
            "Ù": "U",
            "Û": "U",
            "Ÿ": "Y",
            # Other special characters
            " ": "_",  # Replace spaces with underscores
            "/": "_",  # Replace slashes with underscores
            "\\": "_",  # Replace backslashes with underscores
            ":": "_",  # Replace colons with underscores
            "*": "_",  # Replace asterisks with underscores
            "?": "_",  # Replace question marks with underscores
            '"': "_",  # Replace quotes with underscores
            "<": "_",  # Replace less than with underscores
            ">": "_",  # Replace greater than with underscores
            "|": "_",  # Replace pipes with underscores
            "&": "_",  # Replace ampersands with underscores
            "%": "_",  # Replace percent signs with underscores
            "#": "_",  # Replace hash signs with underscores
            "@": "_",  # Replace at signs with underscores
            "!": "_",  # Replace exclamation marks with underscores
            "~": "_",  # Replace tildes with underscores
            "`": "_",  # Replace backticks with underscores
            "+": "_",  # Replace plus signs with underscores
            "=": "_",  # Replace equals signs with underscores
            "[": "_",  # Replace square brackets with underscores
            "]": "_",  # Replace square brackets with underscores
            "{": "_",  # Replace curly braces with underscores
            "}": "_",  # Replace curly braces with underscores
            "(": "_",  # Replace parentheses with underscores
            ")": "_",  # Replace parentheses with underscores
        }

        # Apply replacements in order
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)

        # Remove any remaining non-ASCII characters
        ascii_only = "".join(c for c in normalized if ord(c) < 128)

        # Remove any consecutive underscores
        cleaned = re.sub(r"_+", "_", ascii_only)

        # Remove leading/trailing underscores
        cleaned = cleaned.strip("_")

        # Ensure the result is not empty
        if not cleaned:
            return "no_value"

        return cleaned  # Keep original case for city names

    except Exception as e:
        # Use print instead of logger since this runs in a UDF
        print(f"Error sanitizing value '{value}': {str(e)}")
        return "no_value"


def main():
    logger.info("Starting breweries transformation process")

    # Initialize Spark session with Delta Lake support
    spark = (
        SparkSession.builder.appName("Breweries Silver Layer Transformation")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

    # Get Spark logger
    sc = spark.sparkContext
    sc.setLogLevel("INFO")

    logger.info("Spark session initialized successfully")

    # Read the latest JSON file from bronze layer
    bronze_file = get_latest_bronze_file()
    logger.info(f"Reading from bronze file: {bronze_file}")

    # Define schema for JSON data
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("address_1", StringType(), True),
            StructField("address_2", StringType(), True),
            StructField("address_3", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state_province", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("phone", StringType(), True),
            StructField("website_url", StringType(), True),
            StructField("state", StringType(), True),  # Keep both fields in schema
            StructField("street", StringType(), True),
            StructField("ingested_date", StringType(), True),
            StructField("valid_from", StringType(), True),  # SCD2 columns
            StructField("valid_to", StringType(), True),
            StructField("is_current", BooleanType(), True),
        ]
    )

    # Read JSON data with explicit schema
    logger.info("Reading JSON file with schema:")
    logger.info(schema)

    # Read the JSON file
    df = spark.read.option("multiline", "true").json(bronze_file)
    logger.info("Raw DataFrame schema:")
    df.printSchema()

    # Log the raw data to understand its structure
    logger.info("Sample of raw data:")
    df.select("country", "state", "state_province", "city").show(5, truncate=False)

    # Check for null values
    logger.info("Null value counts:")
    df.select(
        [
            count(when(col(c).isNull(), c)).alias(c)
            for c in ["country", "state", "state_province", "city"]
        ]
    ).show()

    # Perform necessary transformations
    transformed_df = df.select(
        col("id"),
        col("name"),
        col("brewery_type"),
        col("address_1"),
        col("address_2"),
        col("address_3"),
        col("city"),
        # Use state_province if state is null, otherwise use state
        when(col("state").isNotNull(), col("state"))
        .otherwise(col("state_province"))
        .alias("state"),
        col("postal_code"),
        # Ensure country is not null
        when(col("country").isNotNull(), col("country"))
        .otherwise(lit("no_country"))
        .alias("country"),
        # Clean and standardize phone numbers
        when(
            col("phone").isNotNull(),
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        col("phone"), "[^0-9+]", ""
                    ),  # Remove non-numeric chars except +
                    "^\\+",
                    "",  # Remove leading +
                ),
                "^1",
                "",  # Remove leading 1 if present
            ),
        )
        .otherwise(None)
        .alias("phone"),
        # Clean website URLs
        regexp_replace(col("website_url"), "\\\\/", "/").alias("website_url"),
        # Create full address by combining address fields
        concat(
            col("address_1"),
            when(
                col("address_2").isNotNull(), concat(lit(", "), col("address_2"))
            ).otherwise(lit("")),
            when(
                col("address_3").isNotNull(), concat(lit(", "), col("address_3"))
            ).otherwise(lit("")),
            lit(", "),
            col("city"),
            lit(", "),
            when(col("state").isNotNull(), col("state")).otherwise(
                col("state_province")
            ),
            lit(" "),
            col("postal_code"),
            lit(", "),
            when(col("country").isNotNull(), col("country")).otherwise(
                lit("no_country")
            ),
        ).alias("full_address"),
        # Cast coordinates with validation
        when(
            (col("longitude").isNotNull())
            & (col("longitude") >= -180)
            & (col("longitude") <= 180),
            col("longitude").cast(DoubleType()),
        )
        .otherwise(None)
        .alias("longitude"),
        when(
            (col("latitude").isNotNull())
            & (col("latitude") >= -90)
            & (col("latitude") <= 90),
            col("latitude").cast(DoubleType()),
        )
        .otherwise(None)
        .alias("latitude"),
        col("ingested_date").alias("updated_at"),
        to_date(col("ingested_date")).alias("updated_date"),
        # Add SCD2 columns
        col("ingested_date").alias("valid_from"),  # Use ingested_date as valid_from
        lit(None)
        .cast(StringType())
        .alias("valid_to"),  # Current records have no valid_to
        lit(True).alias("is_current"),  # All records are current initially
    )

    # Register the UDF
    sanitize_udf = udf(sanitize_path_component, StringType())

    # Sanitize partition columns
    transformed_df = (
        transformed_df.withColumn("country", sanitize_udf(col("country")))
        .withColumn("state", sanitize_udf(col("state")))
        .withColumn("city", sanitize_udf(col("city")))
    )

    # Define the output path for Delta format
    output_path = "/usr/local/airflow/include/data/prata/openbrewerydb"

    # Log DataFrame schema and count before writing
    logger.info(f"Transformed DataFrame schema: {transformed_df.schema}")
    logger.info(f"Transformed DataFrame count: {transformed_df.count()}")

    try:
        # Ensure the output directory exists with proper permissions
        try:
            os.makedirs(output_path, exist_ok=True)
            # Set permissions to 777 to ensure Spark can write
            os.chmod(output_path, 0o777)
        except Exception as e:
            logger.error(
                f"Failed to create or set permissions for output directory: {str(e)}"
            )
            raise

        # Log the distribution of values in partitioning columns
        logger.info("Distribution of values in partitioning columns:")
        logger.info("Country distribution:")
        transformed_df.groupBy("country").count().show(truncate=False)
        logger.info("State distribution:")
        transformed_df.groupBy("state").count().show(truncate=False)
        logger.info("City distribution:")
        transformed_df.groupBy("city").count().show(truncate=False)

        # Write to Delta format with partitioning
        transformed_df.write.format("delta").partitionBy(
            "country", "state", "city"
        ).mode("overwrite").option("mergeSchema", "true").option(
            "overwriteSchema", "true"
        ).option("path", output_path).save()

        # Verify the write operation
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(spark, output_path)
        logger.info(
            f"Successfully created Delta table with {delta_table.toDF().count()} records"
        )
        logger.info(f"Delta table schema: {delta_table.toDF().schema}")

        # List the contents of the output directory
        logger.info("Contents of output directory:")
        for root, dirs, files in os.walk(output_path):
            for name in files:
                logger.info(os.path.join(root, name))
            for name in dirs:
                logger.info(os.path.join(root, name))

    except Exception as e:
        logger.error(f"Error writing to Delta format: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        logger.error(f"Error traceback: {e.__traceback__}")
        raise

    logger.info(f"Data successfully written to prata layer: {output_path}")
    spark.stop()
    logger.info("Spark session stopped")


def get_latest_bronze_file():
    logger.debug("Searching for latest bronze file")
    # Base path for bronze layer
    base_path = "/usr/local/airflow/include/data/bronze/openbrewerydb/breweries/full"

    # Get the latest date folder
    year_folders = sorted(
        [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))],
        reverse=True,
    )
    if not year_folders:
        logger.error("No year folders found")
        raise Exception("No year folders found")

    latest_year = year_folders[0]
    year_path = os.path.join(base_path, latest_year)
    logger.debug(f"Found latest year: {latest_year}")

    month_folders = sorted(
        [d for d in os.listdir(year_path) if os.path.isdir(os.path.join(year_path, d))],
        reverse=True,
    )
    if not month_folders:
        logger.error("No month folders found")
        raise Exception("No month folders found")

    latest_month = month_folders[0]
    month_path = os.path.join(year_path, latest_month)
    logger.debug(f"Found latest month: {latest_month}")

    day_folders = sorted(
        [
            d
            for d in os.listdir(month_path)
            if os.path.isdir(os.path.join(month_path, d))
        ],
        reverse=True,
    )
    if not day_folders:
        logger.error("No day folders found")
        raise Exception("No day folders found")

    latest_day = day_folders[0]
    day_path = os.path.join(month_path, latest_day)
    logger.debug(f"Found latest day: {latest_day}")

    # Get the latest JSON file
    json_files = [f for f in os.listdir(day_path) if f.endswith(".json")]
    if not json_files:
        logger.error("No JSON files found")
        raise Exception("No JSON files found")

    latest_file = sorted(json_files, reverse=True)[0]
    file_path = os.path.join(day_path, latest_file)
    logger.debug(f"Found latest file: {latest_file}")
    return file_path


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception(f"An error occurred during execution: {str(e)}")
        raise

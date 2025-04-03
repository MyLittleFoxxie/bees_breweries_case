import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
import os


@pytest.fixture
def sample_silver_data(spark):
    """Create sample silver layer data for testing"""
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("country", StringType(), True),
            StructField("state", StringType(), True),
            StructField("city", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("phone", StringType(), True),
            StructField("website_url", StringType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("updated_date", StringType(), True),
            StructField("valid_from", StringType(), True),
            StructField("valid_to", StringType(), True),
            StructField("is_current", StringType(), True),
        ]
    )

    data = [
        (
            "1",
            "Test Brewery 1",
            "micro",
            "United States",
            "Oregon",
            "Portland",
            "97201",
            45.5231,
            -122.6765,
            "+1-503-555-1234",
            "http://test1.com",
            "2024-01-01T00:00:00Z",
            "2024-01-01",
            "2024-01-01",
            None,
            "true",
        ),
        (
            "2",
            "Test Brewery 2",
            "brewpub",
            "United States",
            "Oregon",
            "Portland",
            "97202",
            45.5232,
            -122.6766,
            "+1-503-555-5678",
            "http://test2.com",
            "2024-01-01T00:00:00Z",
            "2024-01-01",
            "2024-01-01",
            None,
            "true",
        ),
    ]

    return spark.createDataFrame(data, schema)

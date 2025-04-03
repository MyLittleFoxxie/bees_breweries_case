import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
)
from datetime import datetime
import os


def test_sanitize_path_component():
    """Test the path sanitization function"""
    from include.scripts.script_silver_breweries_transformation import (
        sanitize_path_component,
    )

    # Test basic cases
    assert sanitize_path_component("Test City") == "Test_City"
    assert sanitize_path_component("New York") == "New_York"

    # Test special characters
    assert sanitize_path_component("São Paulo") == "Sao_Paulo"
    assert sanitize_path_component("München") == "Munchen"

    # Test null/empty cases
    assert sanitize_path_component(None) == "no_value"
    assert sanitize_path_component("") == "no_value"

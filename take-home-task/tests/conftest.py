# From claude to help testing quicker although doesn't seem all that useful.
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("test_transforms")
        .master("local")
        .config("spark.ui.enabled", "false")       # kills the web UI
        .config("spark.sql.shuffle.partitions", "1") # no over-partitioning for small data
        .getOrCreate()
    )
    yield session
    session.stop()
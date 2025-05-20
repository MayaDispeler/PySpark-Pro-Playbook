import os
import sys
import pytest
from pyspark.sql import SparkSession

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

@pytest.fixture(scope="session")
def spark_session():
    """Create a SparkSession for testing."""
    spark = (SparkSession.builder
            .appName("PySpark-Tests")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())
    
    yield spark
    spark.stop()

def test_spark_session_creation(spark_session):
    """Test that SparkSession is created successfully."""
    assert spark_session is not None
    assert spark_session.version is not None
    
def test_basic_dataframe_ops(spark_session):
    """Test basic DataFrame operations."""
    # Create a simple DataFrame
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark_session.createDataFrame(data, ["name", "age"])
    
    # Test DataFrame operations
    assert df.count() == 3
    assert len(df.columns) == 2
    
    # Test filtering
    filtered_df = df.filter(df.age > 25)
    assert filtered_df.count() == 2 
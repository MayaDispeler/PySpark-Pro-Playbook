"""
PySpark Initialization Module
----------------------------
Utilities for initializing PySpark in a standardized way.
"""

import os
import sys

def get_java_home():
    """Get the Java home directory based on environment."""
    # Default Java home for macOS
    default_java_home = "/opt/homebrew/Cellar/openjdk@11/11.0.27/libexec/openjdk.jdk/Contents/Home"
    
    # Use environment variable if set, otherwise use default
    return os.environ.get("JAVA_HOME", default_java_home)

def set_spark_env_vars(java_home=None):
    """Set environment variables required for PySpark."""
    if java_home is None:
        java_home = get_java_home()
    
    os.environ["JAVA_HOME"] = java_home
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    return {
        "JAVA_HOME": java_home,
        "PYSPARK_PYTHON": sys.executable,
        "PYSPARK_DRIVER_PYTHON": sys.executable
    }

def get_spark_session(app_name="PySpark Application", 
                      master="local[*]", 
                      memory="2g",
                      configs=None):
    """Create and configure a SparkSession.
    
    Args:
        app_name (str): Name of the Spark application
        master (str): Spark master URL (local[*], yarn, etc.)
        memory (str): Executor memory
        configs (dict): Additional configurations for Spark
        
    Returns:
        SparkSession: Configured SparkSession
    """
    from pyspark.sql import SparkSession
    
    # Set environment variables
    set_spark_env_vars()
    
    # Create builder
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.executor.memory", memory)
    
    # Add additional configs if provided
    if configs is not None:
        for key, value in configs.items():
            builder = builder.config(key, value)
    
    # Create session
    spark = builder.getOrCreate()
    
    return spark

def test_spark_session():
    """Function to test PySpark installation and setup."""
    # Get SparkSession
    spark = get_spark_session(app_name="PySpark Test")
    sc = spark.sparkContext
    
    # Print version
    print("SparkContext initialized successfully")
    print(f"Spark version: {spark.version}")
    
    # Simple test
    data = [1, 2, 3, 4, 5]
    distData = sc.parallelize(data)
    sum_result = distData.reduce(lambda a, b: a + b)
    print(f"Sum of test data: {sum_result}")
    
    return spark

# Run test if script is executed directly
if __name__ == "__main__":
    test_spark_session()

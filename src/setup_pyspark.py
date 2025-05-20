#!/usr/bin/env python3

"""
PySpark Setup Script - run this before using PySpark
Usage: python3 setup_pyspark.py
"""

import os
import sys

def main():
    # Specify Java home
    java_home = "/opt/homebrew/Cellar/openjdk@11/11.0.27/libexec/openjdk.jdk/Contents/Home"
    
    # Set environment variables
    os.environ["JAVA_HOME"] = java_home
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    # Try importing PySpark
    try:
        from pyspark.sql import SparkSession
        
        # Create SparkSession
        spark = SparkSession.builder \
            .appName("PySpark Test") \
            .master("local[*]") \
            .config("spark.executor.memory", "1g") \
            .getOrCreate()
        
        # Get SparkContext from SparkSession
        sc = spark.sparkContext
        
        # Print version and test
        print(f"✅ PySpark setup successful! Spark version: {spark.version}")
        
        # Simple test
        data = [1, 2, 3, 4, 5]
        distData = sc.parallelize(data)
        sum_result = distData.reduce(lambda a, b: a + b)
        print(f"✅ Test passed: Sum of test data: {sum_result}")
        
        # Stop spark session
        spark.stop()
        
        # Print instructions
        print("\nTo use PySpark in your Jupyter notebook, add the following to the first cell:")
        print("```")
        print("import os")
        print("import sys")
        print(f"os.environ['JAVA_HOME'] = '{java_home}'")
        print("os.environ['PYSPARK_PYTHON'] = sys.executable")
        print("os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable")
        print("from pyspark.sql import SparkSession")
        print("spark = SparkSession.builder \\")
        print("    .appName(\"PySpark Learning\") \\")
        print("    .master(\"local[*]\") \\")
        print("    .config(\"spark.executor.memory\", \"1g\") \\")
        print("    .getOrCreate()")
        print("sc = spark.sparkContext")
        print("```")
        
    except ImportError:
        print("❌ PySpark not found. Installing...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyspark"])
        print("🔄 Installation complete. Please run this script again.")

if __name__ == "__main__":
    main() 
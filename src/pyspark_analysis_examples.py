#!/usr/bin/env python3
"""
Enterprise PySpark Analysis Examples
-----------------------------------
Advanced examples of PySpark operations for enterprise data analysis.

This script demonstrates:
1. Data Loading and Exploration
2. Complex Transformations
3. Window Functions
4. Machine Learning Pipeline
5. Streaming Analytics

Usage:
    python pyspark_analysis_examples.py

Requirements:
    - PySpark
    - Sample datasets (generated with data_generator.py)
"""

import os
import sys
from datetime import datetime, timedelta

# Configure Java
os.environ["JAVA_HOME"] = "/opt/homebrew/Cellar/openjdk@11/11.0.27/libexec/openjdk.jdk/Contents/Home"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType, TimestampType
from pyspark.ml.feature import VectorAssembler, StandardScaler, OneHotEncoder, StringIndexer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.clustering import KMeans

def create_spark_session(app_name="PySpark Advanced Analytics"):
    """Create and configure SparkSession"""
    return (SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "20")
            .config("spark.sql.adaptive.enabled", "true")
            # Enable Hive support if needed
            # .enableHiveSupport()
            .getOrCreate())

def load_datasets(spark, data_dir="./sample_data"):
    """Load the sample datasets"""
    print("Loading datasets...")
    
    # Check if the directory exists
    if not os.path.exists(data_dir):
        print(f"Error: {data_dir} directory not found!")
        print("Please run data_generator.py first to create sample datasets.")
        sys.exit(1)
    
    # Load all datasets
    employees_df = spark.read.parquet(f"{data_dir}/employees.parquet")
    sales_df = spark.read.parquet(f"{data_dir}/sales.parquet")
    customers_df = spark.read.parquet(f"{data_dir}/customers.parquet")
    web_logs_df = spark.read.parquet(f"{data_dir}/web_logs.parquet")
    products_df = spark.read.parquet(f"{data_dir}/products.parquet")
    
    # Register as temp views for SQL
    employees_df.createOrReplaceTempView("employees")
    sales_df.createOrReplaceTempView("sales")
    customers_df.createOrReplaceTempView("customers")
    web_logs_df.createOrReplaceTempView("web_logs")
    products_df.createOrReplaceTempView("products")
    
    return {
        "employees": employees_df,
        "sales": sales_df,
        "customers": customers_df,
        "web_logs": web_logs_df,
        "products": products_df
    }

def data_exploration(spark, dfs):
    """Basic data exploration"""
    print("\n==== Basic Data Exploration ====")
    
    employees_df = dfs["employees"]
    sales_df = dfs["sales"]
    customers_df = dfs["customers"]
    
    # Get basic stats
    print("\nEmployee Dataset:")
    print(f"Number of records: {employees_df.count()}")
    print("Schema:")
    employees_df.printSchema()
    
    # Summary statistics
    print("\nSummary statistics for employee salaries:")
    employees_df.select("salary").describe().show()
    
    # Department distribution
    print("\nEmployee distribution by department:")
    employees_df.groupBy("department").count().orderBy(F.desc("count")).show()
    
    # Sales analysis
    print("\nSales by category:")
    sales_df.groupBy("category").agg(
        F.count("transaction_id").alias("transaction_count"),
        F.round(F.sum("total"), 2).alias("total_revenue"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price")
    ).orderBy(F.desc("total_revenue")).show()
    
    # Customer segments
    print("\nCustomer segments:")
    customers_df.groupBy("segment").agg(
        F.count("customer_id").alias("customer_count"),
        F.round(F.avg("total_spent"), 2).alias("avg_total_spent")
    ).orderBy(F.desc("avg_total_spent")).show()
    
    # SQL example
    print("\nTop 5 selling products (using SQL):")
    spark.sql("""
        SELECT 
            product, 
            COUNT(transaction_id) as transaction_count,
            ROUND(SUM(total), 2) as total_revenue
        FROM sales
        GROUP BY product
        ORDER BY total_revenue DESC
        LIMIT 5
    """).show()

def complex_transformations(spark, dfs):
    """Demonstrate complex transformations"""
    print("\n==== Complex Transformations ====")
    
    employees_df = dfs["employees"]
    sales_df = dfs["sales"]
    customers_df = dfs["customers"]
    products_df = dfs["products"]
    
    # 1. Join datasets and perform calculation
    print("\nJoining sales with products and calculating metrics:")
    sales_with_products = sales_df.join(
        products_df.select("product_id", "name", "price", "inventory_count"),
        sales_df.product == products_df.name, "left"
    )
    
    # Calculate profit margin
    sales_analysis = sales_with_products.withColumn(
        "profit_margin",
        F.round((F.col("total") - (F.col("price") * 0.7 * F.col("quantity"))), 2)
    ).withColumn(
        "margin_percentage",
        F.round(F.col("profit_margin") / F.col("total") * 100, 1)
    )
    
    sales_analysis.select(
        "transaction_id", "product", "quantity", "unit_price", "total", 
        "profit_margin", "margin_percentage"
    ).show(5)
    
    # 2. Complex date operations
    print("\nMonthly sales trend:")
    monthly_sales = sales_df.withColumn(
        "transaction_date", F.to_date(F.col("transaction_date"))
    ).withColumn(
        "year", F.year(F.col("transaction_date"))
    ).withColumn(
        "month", F.month(F.col("transaction_date"))
    ).withColumn(
        "month_name", F.date_format(F.col("transaction_date"), "MMMM")
    ).groupBy("year", "month", "month_name").agg(
        F.count("transaction_id").alias("transaction_count"),
        F.round(F.sum("total"), 2).alias("monthly_revenue")
    ).orderBy("year", "month")
    
    monthly_sales.show(10)
    
    # 3. Explode array and collect
    print("\nEmployee skills distribution:")
    employee_skills = employees_df.select(
        F.col("id"), F.col("first_name"), F.col("last_name"), F.explode(F.col("skills")).alias("skill")
    )
    
    skill_count = employee_skills.groupBy("skill").count().orderBy(F.desc("count"))
    skill_count.show(10)
    
    # 4. Complex aggregation with multiple conditions
    print("\nEmployee salary analysis by department and experience:")
    salary_analysis = employees_df.withColumn(
        "experience_level",
        F.when(F.col("salary") < 70000, "Junior")
         .when(F.col("salary") < 90000, "Mid-level")
         .when(F.col("salary") < 110000, "Senior")
         .otherwise("Executive")
    ).groupBy("department", "experience_level").agg(
        F.count("id").alias("employee_count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary")
    ).orderBy("department", F.desc("avg_salary"))
    
    salary_analysis.show()
    
    # 5. Parse and analyze nested structures
    print("\nCustomer preferences analysis:")
    
    # Extract nested data
    customer_preferences = customers_df.select(
        "customer_id",
        "segment",
        F.explode(F.col("preferences.preferred_categories")).alias("preferred_category")
    )
    
    # Analyze by segment
    category_preference = customer_preferences.groupBy("segment", "preferred_category").count()
    
    # Pivot the results
    pivoted_preferences = category_preference.groupBy("segment").pivot("preferred_category").sum("count").fillna(0)
    pivoted_preferences.show()

def window_function_examples(spark, dfs):
    """Demonstrate window functions"""
    print("\n==== Window Function Examples ====")
    
    sales_df = dfs["sales"]
    employees_df = dfs["employees"]
    
    # 1. Running totals
    print("\nRunning total of sales by date:")
    sales_df = sales_df.withColumn("transaction_date", F.to_date(F.col("transaction_date")))
    
    date_window = Window.orderBy("transaction_date")
    running_total = sales_df.select(
        "transaction_date",
        "total"
    ).groupBy("transaction_date").agg(
        F.sum("total").alias("daily_total")
    ).orderBy("transaction_date").withColumn(
        "running_total", F.round(F.sum("daily_total").over(date_window), 2)
    )
    
    running_total.show(10)
    
    # 2. Ranking
    print("\nRanking employees by salary within department:")
    department_salary_window = Window.partitionBy("department").orderBy(F.desc("salary"))
    
    employee_rankings = employees_df.withColumn(
        "department_rank", F.rank().over(department_salary_window)
    ).withColumn(
        "department_dense_rank", F.dense_rank().over(department_salary_window)
    ).withColumn(
        "percentile", F.percent_rank().over(department_salary_window)
    ).select(
        "id", "first_name", "last_name", "department", "salary", 
        "department_rank", "department_dense_rank", "percentile"
    ).orderBy("department", "department_rank")
    
    employee_rankings.filter(F.col("department_rank") <= 3).show()
    
    # 3. Moving averages - Fixed to use row-based window instead of range-based window
    print("\nMoving average of sales (7-day window):")
    
    # Convert transaction_date to a date type
    sales_dates_df = sales_df.select(
        "transaction_date",
        "total"
    ).groupBy("transaction_date").agg(
        F.sum("total").alias("daily_total")
    ).orderBy("transaction_date")
    
    # Use rows between instead of range between for moving average
    rows_window_7d = Window.orderBy("transaction_date").rowsBetween(-6, 0)
    
    moving_avg = sales_dates_df.withColumn(
        "7_day_moving_avg", F.round(F.avg("daily_total").over(rows_window_7d), 2)
    )
    
    moving_avg.show(10)
    
    # 4. Lag and lead for time series analysis
    print("\nCalculating day-over-day change in sales:")
    ordered_window = Window.orderBy("transaction_date")
    
    day_over_day = sales_dates_df.withColumn(
        "previous_day_total", F.lag("daily_total", 1).over(ordered_window)
    ).withColumn(
        "daily_change", F.round(F.col("daily_total") - F.col("previous_day_total"), 2)
    ).withColumn(
        "daily_change_pct", 
        F.when(F.col("previous_day_total").isNull() | (F.col("previous_day_total") == 0), 0)
         .otherwise(F.round((F.col("daily_total") - F.col("previous_day_total")) / F.col("previous_day_total") * 100, 1))
    )
    
    day_over_day.filter(F.col("previous_day_total").isNotNull()).show(10)
    
    # 5. First and last values
    print("\nFirst and last purchase date by customer:")
    customer_window = Window.partitionBy("customer_id").orderBy("transaction_date")
    customer_window_unbounded = Window.partitionBy("customer_id").orderBy("transaction_date").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    customer_purchase_history = sales_df.select(
        "transaction_id", "customer_id", "transaction_date", "total"
    ).withColumn(
        "first_purchase_date", F.first("transaction_date").over(customer_window_unbounded)
    ).withColumn(
        "last_purchase_date", F.last("transaction_date").over(customer_window_unbounded)
    ).withColumn(
        "days_since_first_purchase", F.datediff(F.col("transaction_date"), F.col("first_purchase_date"))
    ).withColumn(
        "purchase_number", F.row_number().over(customer_window)
    )
    
    # Show summary of customer purchase behavior
    customer_summary = customer_purchase_history.groupBy("customer_id").agg(
        F.min("first_purchase_date").alias("first_purchase"),
        F.max("last_purchase_date").alias("last_purchase"),
        F.count("transaction_id").alias("total_purchases"),
        F.datediff(F.max("last_purchase_date"), F.min("first_purchase_date")).alias("customer_lifespan_days")
    ).orderBy(F.desc("total_purchases"))
    
    customer_summary.show(10)

def machine_learning_examples(spark, dfs):
    """Demonstrate machine learning with PySpark"""
    print("\n==== Machine Learning Examples ====")
    
    employees_df = dfs["employees"]
    sales_df = dfs["sales"]
    customers_df = dfs["customers"]
    
    # 1. Salary Prediction Model (Linear Regression)
    print("\n1. Predicting employee salary based on experience and department")
    
    # Prepare the data
    # Convert categorical to numeric
    print("Preparing data...")
    department_indexer = StringIndexer(inputCol="department", outputCol="department_idx")
    department_encoder = OneHotEncoder(inputCol="department_idx", outputCol="department_vec")
    
    # Assemble features
    feature_cols = ["department_vec"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # Train a linear regression model
    print("Training linear regression model...")
    lr = LinearRegression(featuresCol="features", labelCol="salary")
    
    # Create a pipeline
    pipeline = Pipeline(stages=[
        department_indexer,
        department_encoder,
        assembler,
        lr
    ])
    
    # Split the data
    print("Splitting data and training...")
    train_data, test_data = employees_df.randomSplit([0.8, 0.2], seed=42)
    
    # Fit the model
    model = pipeline.fit(train_data)
    
    # Make predictions
    predictions = model.transform(test_data)
    
    # Evaluate the model
    evaluator = RegressionEvaluator(labelCol="salary", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE): {rmse}")
    
    # Show predictions
    print("\nSample predictions:")
    predictions.select("id", "department", "salary", "prediction").show(5)
    
    # 2. Customer Segmentation (KMeans Clustering)
    print("\n2. Customer Segmentation using KMeans Clustering")
    
    # Prepare customer data
    print("Preparing customer data...")
    customer_features = customers_df.select(
        "customer_id",
        "total_purchases",
        "total_spent",
        "average_order_value"
    )
    
    # Assemble features
    feature_assembler = VectorAssembler(
        inputCols=["total_purchases", "total_spent", "average_order_value"], 
        outputCol="features"
    )
    
    # Scale features
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features",
                          withStd=True, withMean=True)
    
    # Create a pipeline
    clustering_pipeline = Pipeline(stages=[
        feature_assembler,
        scaler
    ])
    
    # Process data
    processed_data = clustering_pipeline.fit(customer_features).transform(customer_features)
    
    # Fit a KMeans model
    print("Training KMeans model...")
    kmeans = KMeans(k=4, seed=42, featuresCol="scaled_features")
    kmeans_model = kmeans.fit(processed_data)
    
    # Get cluster assignments
    clustered_customers = kmeans_model.transform(processed_data)
    
    # Analyze clusters
    print("\nCluster analysis:")
    cluster_summary = clustered_customers.groupBy("prediction").agg(
        F.count("customer_id").alias("customer_count"),
        F.round(F.avg("total_purchases"), 2).alias("avg_purchases"),
        F.round(F.avg("total_spent"), 2).alias("avg_total_spent"),
        F.round(F.avg("average_order_value"), 2).alias("avg_order_value")
    ).orderBy("prediction")
    
    cluster_summary.show()
    
    # 3. Purchase Prediction (Binary Classification)
    print("\n3. Predict if a customer will make a purchase (Binary Classification)")
    
    # Create a label: 1 if customer has made a purchase in last 30 days, 0 otherwise
    print("Preparing purchase prediction data...")
    
    # First identify which customers made recent purchases
    recent_date = datetime.now() - timedelta(days=30)
    recent_date_str = recent_date.strftime("%Y-%m-%d")
    
    recent_customers = sales_df.filter(
        F.col("transaction_date") >= recent_date_str
    ).select("customer_id").distinct()
    
    # Join with customer data
    labeled_customers = customers_df.join(
        recent_customers.withColumn("recent_purchase", F.lit(1)),
        "customer_id",
        "left"
    ).withColumn(
        "recent_purchase", F.coalesce(F.col("recent_purchase"), F.lit(0))
    )
    
    # Select features
    customer_purchase_features = labeled_customers.select(
        "customer_id",
        "segment",
        "total_purchases",
        "total_spent",
        "average_order_value",
        "recent_purchase"
    )
    
    # Convert categorical features
    segment_indexer = StringIndexer(inputCol="segment", outputCol="segment_idx")
    segment_encoder = OneHotEncoder(inputCol="segment_idx", outputCol="segment_vec")
    
    # Assemble features
    purchase_assembler = VectorAssembler(
        inputCols=["segment_vec", "total_purchases", "total_spent", "average_order_value"],
        outputCol="features"
    )
    
    # Train a Random Forest Classifier
    print("Training Random Forest Classifier...")
    rf = RandomForestClassifier(
        featuresCol="features", 
        labelCol="recent_purchase", 
        numTrees=10,
        maxDepth=5,
        seed=42
    )
    
    # Create a pipeline
    purchase_pipeline = Pipeline(stages=[
        segment_indexer,
        segment_encoder,
        purchase_assembler,
        rf
    ])
    
    # Split data
    purchase_train, purchase_test = customer_purchase_features.randomSplit([0.8, 0.2], seed=42)
    
    # Train model
    purchase_model = purchase_pipeline.fit(purchase_train)
    
    # Make predictions
    purchase_predictions = purchase_model.transform(purchase_test)
    
    # Evaluate model
    evaluator = BinaryClassificationEvaluator(
        labelCol="recent_purchase", 
        rawPredictionCol="rawPrediction", 
        metricName="areaUnderROC"
    )
    auc = evaluator.evaluate(purchase_predictions)
    print(f"Area Under ROC: {auc}")
    
    # Show predictions
    print("\nSample predictions:")
    purchase_predictions.select(
        "customer_id", "total_purchases", "total_spent", "average_order_value", 
        "recent_purchase", "prediction", "probability"
    ).show(5)

def stream_processing_example(spark):
    """Simulate stream processing"""
    print("\n==== Stream Processing Example ====")
    
    # Create schema for the streaming data
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("page", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("session_id", StringType(), True),
        StructField("device", StringType(), True)
    ])
    
    # Create a streaming DataFrame
    print("\nSimulating streaming data processing (this will create sample data in 'streaming_data' directory)...")
    streaming_dir = "streaming_data"
    
    # Ensure the directory exists
    if not os.path.exists(streaming_dir):
        os.makedirs(streaming_dir)
    else:
        # Clean existing files to avoid issues with the stream
        for file in os.listdir(streaming_dir):
            os.remove(os.path.join(streaming_dir, file))
    
    # Create some sample streaming data
    from pyspark.sql import Row
    import random
    import time
    import uuid
    
    actions = ["view", "click", "add_to_cart", "remove_from_cart", "checkout", "purchase"]
    pages = ["/", "/products", "/product/1", "/product/2", "/cart", "/checkout"]
    devices = ["desktop", "mobile", "tablet"]
    
    def generate_events(num_events=10):
        events = []
        for i in range(num_events):
            timestamp = datetime.now()
            user_id = random.randint(1, 1000)
            action = random.choice(actions)
            page = random.choice(pages)
            product_id = random.randint(1, 100) if "product" in page else None
            session_id = str(uuid.uuid4())
            device = random.choice(devices)
            
            events.append(Row(
                timestamp=timestamp,
                user_id=user_id,
                action=action,
                page=page,
                product_id=product_id,
                session_id=session_id,
                device=device
            ))
        return events
    
    # Start a stream reader
    stream_df = spark.readStream.schema(schema).json(streaming_dir)
    
    # Define the query to group by action and count occurrences
    action_counts = stream_df.groupBy("action").count()
    
    # Start the query
    stream_query = action_counts.writeStream.outputMode("complete").format("console").start()
    
    # Generate some sample data
    print("Generating streaming data batches...")
    for i in range(5):
        batch = generate_events(random.randint(5, 15))
        batch_df = spark.createDataFrame(batch)
        batch_df.coalesce(1).write.mode("append").json(streaming_dir)
        print(f"Wrote batch {i+1} of events")
        time.sleep(2)  # Wait a bit between batches
    
    # Wait for the streaming to process the data
    time.sleep(5)
    
    # Stop the stream
    stream_query.stop()
    
    print("\nStream processing example complete")

def main():
    """Main function to execute examples"""
    spark = create_spark_session()
    print(f"Using Apache Spark version {spark.version}")
    
    try:
        # Load datasets
        dfs = load_datasets(spark)
        
        # Run examples
        data_exploration(spark, dfs)
        complex_transformations(spark, dfs)
        window_function_examples(spark, dfs)
        machine_learning_examples(spark, dfs)
        stream_processing_example(spark)
        
    finally:
        # Stop the SparkSession
        spark.stop()
        print("\nSpark session stopped. All examples completed.")

if __name__ == "__main__":
    main() 
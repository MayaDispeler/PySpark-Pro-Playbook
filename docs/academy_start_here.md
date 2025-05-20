# Walkthrough: Your First Enterprise PySpark Project

Welcome to the Enterprise PySpark This walkthrough will guide you step-by-step from setup to advanced analytics, even if you have never used Spark before.

---

## What You'll Learn
- How to set up a PySpark environment
- How to generate and explore real-world data
- How to perform transformations, joins, and window functions
- How to build machine learning models
- How to process streaming data
- How to test your Spark code

---

## Prerequisites
- Python 3.8+
- Java 8 or 11
- Basic Python knowledge (no Spark experience needed)

---

## Step 1: Setting Up the Environment

Open your terminal and run:

```bash
make setup
```

**Expected Output:**
```
✅ PySpark setup successful! Spark version: 3.x.x
✅ Test passed: Sum of test data: 15
```

---

## Step 2: Generating Sample Data

Generate realistic datasets for your analysis:

```bash
make generate-data
```

**Expected Output:**
```
Generating 1000 employee records...
Employee data saved to sample_data/employees.parquet, ...
Generating 10000 sales records...
Sales data saved to sample_data/sales.parquet, ...
...
```

---

## Step 3: Exploring Your First DataFrame

Open a Python shell or notebook and run:

```python
from src.pyspark_init import get_spark_session
spark = get_spark_session(app_name="Academy Demo")
df = spark.read.parquet("sample_data/employees.parquet")
df.show(5)
df.printSchema()
```

**Expected Output:**
```
+---+----------+---------+--------------------+...
| id|first_name|last_name|email              |...
+---+----------+---------+--------------------+...
|  1|  John    |  Smith  | john.smith@...     |...
|  2|  Jane    |  Doe    | jane.doe@...       |...
...
```

---

## Step 4: Transformations and Joins

```python
from pyspark.sql import functions as F
sales = spark.read.parquet("sample_data/sales.parquet")
products = spark.read.parquet("sample_data/products.parquet")
joined = sales.join(products, sales.product == products.name, "left")
monthly = sales.groupBy(F.year("transaction_date").alias("year"), F.month("transaction_date").alias("month")).agg(F.sum("total").alias("revenue"))
monthly.show(5)
```

---

## Step 5: Window Functions

```python
from pyspark.sql.window import Window
window = Window.orderBy("transaction_date").rowsBetween(-6, 0)
daily = sales.groupBy("transaction_date").agg(F.sum("total").alias("daily_total")).orderBy("transaction_date")
daily = daily.withColumn("7_day_moving_avg", F.round(F.avg("daily_total").over(window), 2))
daily.show(10)
```

---

## Step 6: Machine Learning

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
assembler = VectorAssembler(inputCols=["quantity", "unit_price"], outputCol="features")
train = assembler.transform(sales)
lr = LinearRegression(featuresCol="features", labelCol="total")
model = lr.fit(train)
predictions = model.transform(train)
predictions.select("total", "prediction").show(5)
```

---

## Step 7: Streaming Analytics

```bash
make run-examples
```

Look for the streaming section in the output. You'll see simulated streaming data being processed in real time.

---

## Step 8: Running Tests

```bash
make test
```

**Expected Output:**
```
============================= test session starts =============================
collected 2 items

tests/test_spark_session.py ..                                         [100%]

============================== 2 passed in 3.21s ==============================
```

---

## Step 9: Where to Go Next
- Try modifying the data generator to create your own datasets
- Explore more advanced analytics in `src/pyspark_analysis_examples.py`
- Read the [Getting Started Guide](getting-started.md) and [Best Practices](best-practices.md)
- Join the community for help and ideas

---

## Tips & Best Practices
- Always use SparkSession, not SparkContext directly
- Use Parquet for efficient data storage
- Partition your data for scalability
- Use version control (git) for your code
- Write tests for your Spark jobs

---

## Visual: Data Flow

```mermaid
flowchart LR
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style G fill:#bbf,stroke:#333,stroke-width:2px
    A([🚀 Data Generation]):::start --> B([🔍 Data Exploration])
    B --> C([🔄 Transformations])
    C --> D([📊 Window Functions])
    D --> E([🤖 Machine Learning])
    E --> F([🌊 Streaming])
    F --> G([🧪 Testing]):::end

    classDef start fill:#f9f,stroke:#333,stroke-width:2px;
    classDef end fill:#bbf,stroke:#333,stroke-width:2px;
```

---

Congratulations! You've completed your first enterprise PySpark project. Keep experimenting and building! 
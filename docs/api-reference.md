# API Reference: Enterprise PySpark Cheatsheet

---

## src/pyspark_init.py

### get_java_home
```python
get_java_home() -> str
```
- **Description:** Returns the JAVA_HOME directory path, using the environment variable if set, otherwise a default.
- **Parameters:** None
- **Returns:** `str` — Path to Java home directory.
- **Raises:** None

---

### set_spark_env_vars
```python
set_spark_env_vars(java_home: str = None) -> dict
```
- **Description:** Sets required environment variables for PySpark (JAVA_HOME, PYSPARK_PYTHON, PYSPARK_DRIVER_PYTHON).
- **Parameters:**
  - `java_home` (str, optional): Path to Java home. If not provided, uses `get_java_home()`.
- **Returns:** `dict` — The environment variables set.
- **Raises:** None

---

### get_spark_session
```python
get_spark_session(
    app_name: str = "PySpark Application",
    master: str = "local[*]",
    memory: str = "2g",
    configs: dict = None
) -> pyspark.sql.SparkSession
```
- **Description:** Creates and configures a SparkSession.
- **Parameters:**
  - `app_name` (str): Name of the Spark application.
  - `master` (str): Spark master URL (e.g., "local[*]", "yarn").
  - `memory` (str): Executor memory (e.g., "2g").
  - `configs` (dict, optional): Additional Spark configuration key-value pairs.
- **Returns:** `SparkSession` — Configured SparkSession object.
- **Raises:** None
- **Example:**
  ```python
  spark = get_spark_session(app_name="TestApp", memory="4g")
  ```

---

### test_spark_session
```python
test_spark_session() -> pyspark.sql.SparkSession
```
- **Description:** Runs a test Spark job, prints Spark version and sum of test data.
- **Parameters:** None
- **Returns:** `SparkSession`
- **Raises:** None

---

## src/data_generator.py

### ensure_dir
```python
ensure_dir(directory: str)
```
- **Description:** Creates the directory if it does not exist.
- **Parameters:** `directory` (str): Path to directory.
- **Returns:** None

---

### generate_employee_data
```python
generate_employee_data(output_dir: str, num_records: int = 1000)
```
- **Description:** Generates a synthetic employee dataset and saves as Parquet, CSV, and JSON.
- **Parameters:**
  - `output_dir` (str): Output directory.
  - `num_records` (int): Number of records to generate.
- **Returns:** None

---

### generate_sales_data
```python
generate_sales_data(output_dir: str, num_records: int = 10000)
```
- **Description:** Generates a synthetic sales transactions dataset.
- **Parameters:** Same as above.
- **Returns:** None

---

### generate_customer_data
```python
generate_customer_data(output_dir: str, num_records: int = 5000)
```
- **Description:** Generates a synthetic customer dataset.
- **Parameters:** Same as above.
- **Returns:** None

---

### generate_web_logs
```python
generate_web_logs(output_dir: str, num_records: int = 50000)
```
- **Description:** Generates a synthetic web logs dataset.
- **Parameters:** Same as above.
- **Returns:** None

---

### generate_product_catalog
```python
generate_product_catalog(output_dir: str)
```
- **Description:** Generates a synthetic product catalog.
- **Parameters:** `output_dir` (str)
- **Returns:** None

---

### main (data_generator.py)
```python
main()
```
- **Description:** Command-line entry point. Generates all datasets in the specified output directory.
- **Parameters:** None (uses argparse)
- **Returns:** None

---

## src/pyspark_analysis_examples.py

### create_spark_session
```python
create_spark_session(app_name: str = "PySpark Advanced Analytics") -> SparkSession
```
- **Description:** Creates a SparkSession with enterprise configs.
- **Parameters:** `app_name` (str)
- **Returns:** `SparkSession`

---

### load_datasets
```python
load_datasets(spark: SparkSession, data_dir: str = "./sample_data") -> dict
```
- **Description:** Loads all sample datasets as DataFrames and registers them as temp views.
- **Parameters:**
  - `spark` (SparkSession)
  - `data_dir` (str)
- **Returns:** `dict` of DataFrames

---

### data_exploration
```python
data_exploration(spark: SparkSession, dfs: dict)
```
- **Description:** Prints basic stats, schema, groupBy, and SQL examples.
- **Parameters:** `spark`, `dfs` (dict of DataFrames)
- **Returns:** None

---

### complex_transformations
```python
complex_transformations(spark: SparkSession, dfs: dict)
```
- **Description:** Demonstrates joins, aggregations, and nested data analysis.
- **Parameters:** `spark`, `dfs`
- **Returns:** None

---

### window_function_examples
```python
window_function_examples(spark: SparkSession, dfs: dict)
```
- **Description:** Demonstrates running totals, ranking, moving averages, and time-series analysis.
- **Parameters:** `spark`, `dfs`
- **Returns:** None

---

### machine_learning_examples
```python
machine_learning_examples(spark: SparkSession, dfs: dict)
```
- **Description:** Demonstrates regression, clustering, and classification with PySpark MLlib.
- **Parameters:** `spark`, `dfs`
- **Returns:** None

---

### stream_processing_example
```python
stream_processing_example(spark: SparkSession)
```
- **Description:** Simulates streaming analytics with sample data.
- **Parameters:** `spark`
- **Returns:** None

---

### main (pyspark_analysis_examples.py)
```python
main()
```
- **Description:** Runs all examples in sequence.
- **Parameters:** None
- **Returns:** None

---

## src/setup_pyspark.py

### main
```python
main()
```
- **Description:** Sets up environment variables, tests PySpark installation, prints Spark version, and provides notebook setup instructions.
- **Parameters:** None
- **Returns:** None

---

## tests/test_spark_session.py

### test_spark_session_creation
```python
test_spark_session_creation(spark_session)
```
- **Description:** Tests that a SparkSession is created successfully.
- **Parameters:** `spark_session` (pytest fixture)
- **Returns:** None

---

### test_basic_dataframe_ops
```python
test_basic_dataframe_ops(spark_session)
```
- **Description:** Tests basic DataFrame operations (count, columns, filter).
- **Parameters:** `spark_session`
- **Returns:** None

---

For more details, see the source code in each script. 
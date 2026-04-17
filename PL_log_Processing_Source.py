# Databricks notebook source
# MAGIC %md
# MAGIC ####Reading the Json Data with timestamp from the api poling landing in bronze layer.

# COMMAND ----------

# MAGIC %md
# MAGIC #### *Approch 1*

# COMMAND ----------

# import requests
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import current_timestamp, to_date, col

# COMMAND ----------

# posts_url = "https://jsonplaceholder.typicode.com/posts" #getting the data via api here 
# posts_data = requests.get(posts_url).json()

# posts_df = spark.createDataFrame(posts_data)

# # Add ingestion columns
# posts_df = posts_df.withColumn("ingestion_time", current_timestamp())
# posts_df = posts_df.withColumn("ingestion_date", to_date(col("ingestion_time")))

# COMMAND ----------

# comments_url = "https://jsonplaceholder.typicode.com/comments" 
# comments_data = requests.get(comments_url).json()

# comments_df = spark.createDataFrame(comments_data)

# comments_df = comments_df.withColumn("ingestion_time", current_timestamp())
# comments_df = comments_df.withColumn("ingestion_date", to_date(col("ingestion_time")))

# COMMAND ----------

# # Partition by date for better organization and query performance
# # Write
# posts_df.write.format("delta") \
#     .mode("append") \
#     .partitionBy("ingestion_date") \
#     .saveAsTable("bizmetric_databricks_learning.default.Log_Posts_Bronze")

# COMMAND ----------

# # Partition by date for better organization and query performance, Append since we dont want to create the table again an again hence avoiding the use of overwrite. 
# comments_df.write.format("delta") \
#     .mode("append") \
#     .partitionBy("ingestion_date") \
#     .saveAsTable("bizmetric_databricks_learning.default.Log_Comments_Bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #### *Approch 2*

# COMMAND ----------

# Imports

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date, col
from pyspark.sql.types import StructType
from datetime import datetime

# COMMAND ----------

# MAGIC %run /Workspace/Users/prathameshzr9527@gmail.com/Log_Processing_System/NB_Create_Audit_log

# COMMAND ----------

# Create Widgets

dbutils.widgets.text("url", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("catalog", "logs")
dbutils.widgets.text("schema", "bronze")

# COMMAND ----------

# Read Widget Values


url = dbutils.widgets.get("url")
table_name = dbutils.widgets.get("table_name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# print("URL:", url)
# print("Table:", table_name)

# COMMAND ----------

# Generic Source → Bronze Ingestion Framework
class BronzeIngestion:

    def __init__(self, spark: SparkSession, catalog: str, schema: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    # Step 1: Fetch data from API
    def fetch_api_data(self, url: str):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching data from {url}: {e}")
            return []

    # Step 2: Create DataFrame
    def create_dataframe(self, data, schema: StructType = None):
        if schema:
            return self.spark.createDataFrame(data, schema=schema)
        else:
            return self.spark.createDataFrame(data)

    # Step 3: Add ingestion metadata
    def add_metadata(self, df):
        df = df.withColumn("ingestion_time", current_timestamp())
        df = df.withColumn("ingestion_date", to_date(col("ingestion_time")))
        return df
    # Step 4: Write to Bronze (Unity Catalog)
    def write_to_bronze(self, df, table_name: str, mode: str = "append"):
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"

        df.write.format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .partitionBy("ingestion_date") \
            .saveAsTable(full_table_name)

        print(f"Data written to {full_table_name}")

    # Step 5: Full pipeline execution

    def run(self, url: str, table_name: str, schema: StructType = None):

        start_time = datetime.now()

        try:
            # Step 1: Fetch
            data = self.fetch_api_data(url)

            if not data:
                raise ValueError("No data fetched from API")

            # Step 2: Create DataFrame
            df = self.create_dataframe(data, schema)

            # Step 3: Add metadata
            df = self.add_metadata(df)

            # Step 4: Record count
            record_count = df.count()

            # Step 5: Write to Bronze
            self.write_to_bronze(df, table_name)

            end_time = datetime.now()

            # Step 6: Audit SUCCESS
            log_audit(
                layer="bronze",
                table_name=table_name,
                status="SUCCESS",
                start_time=start_time,
                end_time=end_time
            )

        except Exception as e:

            end_time = datetime.now()

            # Audit FAILED
            log_audit(
                layer="bronze",
                table_name=table_name,
                status="FAILED",
                start_time=start_time,
                end_time=end_time,
                error_message=str(e)
            )

            raise

# COMMAND ----------

# Initialize Ingestion


ingestion = BronzeIngestion(
    spark,
    catalog=catalog,
    schema=schema
)

# COMMAND ----------

ingestion.run(
    url=url,
    table_name=table_name
)
# Databricks notebook source
# MAGIC %md
# MAGIC ####Notebook: Silver Get Clean Validate and Split

# COMMAND ----------

# from pyspark.sql.functions import col, row_number, when
# from pyspark.sql.window import Window

# COMMAND ----------

# posts_df = spark.read.table("bizmetric_databricks_learning.default.log_posts_bronze")
# comments_df = spark.read.table("bizmetric_databricks_learning.default.log_comments_bronze")

# COMMAND ----------

# # Step 1: Select & structure columns
# posts_df = posts_df.select(
#     col("id"),
#     col("userId"),
#     col("title"),
#     col("body"),
#     col("ingestion_time"),
#     col("ingestion_date")
# )

# # Step: Deduplicate check (latest record)
# window_spec = Window.partitionBy("id").orderBy(col("ingestion_time").desc())

# posts_df = posts_df.withColumn("rn", row_number().over(window_spec)) \
#                    .filter(col("rn") == 1) \
#                    .drop("rn")

# # Step: Validation rules
# valid_posts = posts_df.filter(
#     col("id").isNotNull() &
#     col("userId").isNotNull()
# )

# invalid_posts = posts_df.filter(
#     col("id").isNull() |
#     col("userId").isNull()
# )

# # Step: Add error reason
# invalid_posts = invalid_posts.withColumn(
#     "error_reason",
#     when(col("id").isNull(), "Missing ID")
#     .when(col("userId").isNull(), "Missing userId")
# )

# COMMAND ----------

# # Step 5: Write outputs for posts_logs
# valid_posts.write.format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("bizmetric_databricks_learning.silver.posts_clean")

# invalid_posts.write.format("delta") \
#     .mode("append") \
#     .saveAsTable("bizmetric_databricks_learning.silver.posts_quarantine")

# COMMAND ----------

# # Comments logs Step 1: Select & structure columns
# comments_df = comments_df.select(
#     col("id"),
#     col("postId"),
#     col("name"),
#     col("email"),
#     col("body"),
#     col("ingestion_time"),
#     col("ingestion_date")
# )

# # Table: Deduplicate (latest record)
# window_spec = Window.partitionBy("id").orderBy(col("ingestion_time").desc())

# comments_df = comments_df.withColumn("rn", row_number().over(window_spec)) \
#                          .filter(col("rn") == 1) \
#                          .drop("rn")

# # Data: Validation rules
# valid_comments = comments_df.filter(
#     col("id").isNotNull() &
#     col("postId").isNotNull() &
#     col("email").isNotNull()
# )

# invalid_comments = comments_df.filter(
#     col("id").isNull() |
#     col("postId").isNull() |
#     col("email").isNull()
# )

# # Error: Add error reason
# invalid_comments = invalid_comments.withColumn(
#     "error_reason",
#     when(col("id").isNull(), "Missing ID")
#     .when(col("postId").isNull(), "Missing postId")
#     .when(col("email").isNull(), "Missing email")
# )

# COMMAND ----------

# # Output: Write outputs for Comment_logs
# valid_comments.write.format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("bizmetric_databricks_learning.silver.comments_clean")

# invalid_comments.write.format("delta") \
#     .mode("append") \
#     .saveAsTable("bizmetric_databricks_learning.silver.comments_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC #### *Approch 2 Using Classes*

# COMMAND ----------

from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

# MAGIC %run /Workspace/Users/prathameshzr9527@gmail.com/Log_Processing_System/NB_Create_Audit_log

# COMMAND ----------

# Widgets

dbutils.widgets.text("table_name", "")
dbutils.widgets.text("key_column", "id")

dbutils.widgets.text("catalog", "logs")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("silver_schema", "silver")

# COMMAND ----------

# Reading Parameters

table_name = dbutils.widgets.get("table_name")
key_col = dbutils.widgets.get("key_column")
catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

# COMMAND ----------

# Dynamic Silver Processor
class DynamicSilverProcessor:

    def __init__(self, spark, catalog, bronze_schema, silver_schema):
        self.spark = spark
        self.catalog = catalog
        self.bronze_schema = bronze_schema
        self.silver_schema = silver_schema

    # --------------------------------------
    # Read Bronze Table
    # --------------------------------------
    def read_bronze(self, table_name):
        table = f"{self.catalog}.{self.bronze_schema}.{table_name}"
        return self.spark.read.table(table)

    # --------------------------------------
    # Auto column selection
    # --------------------------------------
    def select_columns(self, df):
        return df  # keep all columns dynamically

    # --------------------------------------
    # Deduplication
    # --------------------------------------
    def deduplicate(self, df, key_col):
        window_spec = Window.partitionBy(key_col).orderBy(col("ingestion_time").desc())

        df = df.withColumn("rn", row_number().over(window_spec))

        latest_df = df.filter(col("rn") == 1).drop("rn")
        duplicate_df = df.filter(col("rn") > 1).drop("rn")

        return latest_df, duplicate_df

    # --------------------------------------
    # Auto validation (exclude ingestion cols)
    # --------------------------------------
    def get_validation_columns(self, df, key_col):
        exclude_cols = ["ingestion_time", "ingestion_date"]
        return [c for c in df.columns if c not in exclude_cols]

    def validate(self, df, key_col):
        required_cols = self.get_validation_columns(df, key_col)

        condition = None
        for c in required_cols:
            if condition is None:
                condition = col(c).isNotNull()
            else:
                condition = condition & col(c).isNotNull()

        valid_df = df.filter(condition)
        invalid_df = df.filter(~condition)

        return valid_df, invalid_df, required_cols

    # --------------------------------------
    # Error reason
    # --------------------------------------
    def add_error_reason(self, df, required_cols):
        for c in required_cols:
            df = df.withColumn(
                "error_reason",
                when(col(c).isNull(), f"{c} is NULL").otherwise(None)
            )
        return df

    # --------------------------------------
    # Write tables
    # --------------------------------------
    def write(self, df, table_name, suffix, mode):
        full_table = f"{self.catalog}.{self.silver_schema}.{table_name}_{suffix}"

        df.write.format("delta") \
            .mode(mode) \
            .saveAsTable(full_table)

    # --------------------------------------
    # Run full pipeline
    # --------------------------------------

    def run(self, table_name, key_col):

        start_time = datetime.now()

        try:
            # Step 1: Read
            df = self.read_bronze(table_name)

            # Step 2: Deduplicate
            latest_df, duplicate_df = self.deduplicate(df, key_col)

            # Step 3: Validate
            valid_df, invalid_df, _ = self.validate(latest_df, key_col)

            # Step 4: Count
            record_count = valid_df.count()

            # Step 5: Write
            self.write(valid_df, table_name, "clean", "overwrite")
            self.write(invalid_df, table_name, "quarantine", "append")
            self.write(duplicate_df, table_name, "duplicates", "append")

            end_time = datetime.now()

            # Step 6: Log success
            log_audit("silver", table_name, "SUCCESS", start_time, end_time)

        except Exception as e:
            end_time = datetime.now()

            # Log failure
            log_audit("silver", table_name, "FAILED", start_time, end_time, str(e))

            raise e

# COMMAND ----------

processor = DynamicSilverProcessor(
    spark,
    catalog,
    bronze_schema,
    silver_schema
)

# COMMAND ----------

processor.run(
    table_name=table_name,
    key_col=key_col
)
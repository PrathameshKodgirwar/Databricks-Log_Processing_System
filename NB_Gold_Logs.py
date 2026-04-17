# Databricks notebook source
from pyspark.sql.functions import count
from datetime import datetime

# COMMAND ----------

# MAGIC %run /Workspace/Users/prathameshzr9527@gmail.com/Log_Processing_System/NB_Create_Audit_log

# COMMAND ----------

# Widgets

dbutils.widgets.text("catalog", "logs")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("posts_table", "posts")
dbutils.widgets.text("comments_table", "comments")

# COMMAND ----------

# Read Params

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
posts_table = dbutils.widgets.get("posts_table")
comments_table = dbutils.widgets.get("comments_table")

print("Posts Table:", posts_table) #for reference
print("Comments Table:", comments_table) #for reference

# COMMAND ----------

# Gold Aggregation Class

class GoldAggregator:

    def __init__(self, spark, catalog, silver_schema, gold_schema):
        self.spark = spark
        self.catalog = catalog
        self.silver_schema = silver_schema
        self.gold_schema = gold_schema

    # Read tables
    def read_tables(self, posts_table, comments_table):
        posts = self.spark.read.table(f"{self.catalog}.{self.silver_schema}.{posts_table}")
        comments = self.spark.read.table(f"{self.catalog}.{self.silver_schema}.{comments_table}")
        return posts, comments

    # Join
    def join_data(self, posts, comments):
        return comments.join(posts, comments.postId == posts.id, "inner")

    # User activity
    def user_activity(self, posts):
        return posts.groupBy("userId").count() \
            .withColumnRenamed("count", "total_posts")

    # Comments per post
    def comments_per_post(self, comments):
        return comments.groupBy("postId").count() \
            .withColumnRenamed("count", "total_comments")

    # Engagement
    def engagement(self, joined_df):
        return joined_df.groupBy("userId") \
            .agg(count("postId").alias("comments_received"))

    # Write table
    def write(self, df, table_name):
        df.write.mode("overwrite") \
            .saveAsTable(f"{self.catalog}.{self.gold_schema}.{table_name}")

    # Run full pipeline

    def run(self, posts_table, comments_table):

        start_time = datetime.now()

        try:
            # Data: Read
            posts, comments = self.read_tables(posts_table, comments_table)

            # Table: Join
            joined_df = self.join_data(posts, comments)

            # Table: Aggregations
            user_activity_df = self.user_activity(posts)
            comments_df = self.comments_per_post(comments)
            engagement_df = self.engagement(joined_df)

            # Table: Record count (choose main metric)
            record_count = user_activity_df.count()

            # Table: Write outputs
            self.write(user_activity_df, "user_activity")
            self.write(comments_df, "comments_per_post")
            self.write(engagement_df, "user_engagement")

            end_time = datetime.now()

            # Audit: Audit success
            log_audit(
                layer="gold",
                table_name="gold_aggregation",
                status="SUCCESS",
                start_time=start_time,
                end_time=end_time
            )

            return user_activity_df, comments_df, engagement_df

        except Exception as e:

            end_time = datetime.now()

            # Audit: Audit failure
            log_audit(
                layer="gold",
                table_name="gold_aggregation",
                status="FAILED",
                start_time=start_time,
                end_time=end_time,
                error_message=str(e)
            )

            raise

# COMMAND ----------

# Initialize Aggregator

gold = GoldAggregator(
    spark,
    catalog,
    silver_schema,
    gold_schema
)

# COMMAND ----------

# Run Gold Layer

user_activity_df, comments_df, engagement_df = gold.run(
    posts_table,
    comments_table
)

# COMMAND ----------

# Visualizations

display(user_activity_df)
display(comments_df)
display(engagement_df)
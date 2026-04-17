# Databricks notebook source
# MAGIC %md
# MAGIC This is use to creating and view the audit log table. 

# COMMAND ----------

import uuid
from datetime import datetime

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS Logs.config.audit_Logs (
    audit_id STRING,
    layer STRING,
    table_name STRING,
    status STRING,
    record_count INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    error_message STRING
)
USING DELTA
""")

# COMMAND ----------



def log_audit(layer, table_name, status, start_time, end_time, error_message=""):

    audit_data = [{
        "audit_id": str(uuid.uuid4()),
        "layer": layer,
        "table_name": table_name,
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
        "error_message": error_message
    }]

    df = spark.createDataFrame(audit_data)

    df.write.format("delta") \
        .mode("append") \
        .saveAsTable("logs.config.audit_Logs")
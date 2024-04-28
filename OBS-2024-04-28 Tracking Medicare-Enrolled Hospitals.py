# Databricks notebook source
from pyspark.sql.functions import col, first, max as _max, min as _min

# COMMAND ----------

df = (spark.read.table("mimi_ws_1.datacmsgov.pc_hospital")
        .groupBy("npi")
        .agg(
            _max(col("_input_file_date")).alias("last_observed_date"),
            _min(col("_input_file_date")).alias("first_observed_date"),
            first(col("incorporation_date")).alias("incorporation_date"),
            first(col("organization_name")).alias("organization_name")
        )
)

# COMMAND ----------

df.filter(col("last_observed_date") < "2024-04-01").display()

# COMMAND ----------



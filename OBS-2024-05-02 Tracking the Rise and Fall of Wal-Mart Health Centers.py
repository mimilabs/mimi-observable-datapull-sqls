# Databricks notebook source
from pyspark.sql.functions import col
import re
import pandas as pd
from collections import Counter
import json

# COMMAND ----------

# We got this number from https://npi-db.org
npi = "1750308979" # Wal-Mart Stores East Lp

# COMMAND ----------

h3_of_interest = (spark.read.table("mimi_ws_1.nppes.address_h3")
    .filter(col("npi")==npi)
    .collect())[0]["h3_m"]

# COMMAND ----------

pdf = (spark.read.table("mimi_ws_1.nppes.address_h3")
            .filter(col("h3_m")==h3_of_interest)
            .toPandas())

# COMMAND ----------

df_wh = (spark.read.table("mimi_ws_1.nppes.npidata")
    .filter(col("_input_file_date")=="2024-04-07")
    .filter(col("npi").isin(pdf["npi"].tolist()))
    .filter(col("healthcare_provider_taxonomies").contains("208D00000X") | 
            col("healthcare_provider_taxonomies").contains("207Q00000X"))
    )

# COMMAND ----------

df_wh.display()

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC This example is written in Python/Pyspark.
# MAGIC
# MAGIC This script gathers all Wal-Mart-like records from NPPES and track the growth of the Wal-Mart Health Centers.
# MAGIC
# MAGIC The output data is used in this Observable plot: https://observablehq.com/d/2fab357cc8b91c9c
# MAGIC Take a look at the Observable script and its description to learn how the data was used and what the extracted data means.
# MAGIC
# MAGIC Use the Databricks Assistant features to explore the code!
# MAGIC
# MAGIC Questions: 
# MAGIC - What more do you want to see in the data/chart? 
# MAGIC - How would you improve the query/visualization? 
# MAGIC - What are the limitations of the query/visualization?
# MAGIC - In what situations would you need this type of information?

# COMMAND ----------

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



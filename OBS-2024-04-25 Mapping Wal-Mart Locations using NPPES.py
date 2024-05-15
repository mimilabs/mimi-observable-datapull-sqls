# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This example is written in Python/Pyspark.
# MAGIC
# MAGIC This script gathers all Wal-Mart-like records from NPPES and use Census Geocode/H3 libraries to map their locations on the U.S. map. 
# MAGIC
# MAGIC The output data is used in this Observable plot: https://observablehq.com/d/31ca127d61a7a8f9
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

!pip install h3

# COMMAND ----------

from pyspark.sql.functions import col
import re
import pandas as pd
from collections import Counter
import json
import h3

# COMMAND ----------

# We checked this number from https://npi-db.org
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

pdf["namekey"] = pd.Series([re.sub('\W+', '', x)[:2] for x in pdf["name"]])

# COMMAND ----------

h3.h3_to_parent

# COMMAND ----------

h3_cnt = Counter()
for _, row in pdf.loc[pdf["namekey"].isin(["SA", "WH", "WA"]),:].iterrows():
    if row["h3_b"] is None:
        # For this, we used Census Geocoder, which comes up as 90% match rate
        # We have about 10% of the addresses that are not geo-coded.
        # For the purpose of this script, we ignore those for now.
        continue
    h3_cnt[h3.h3_to_parent(row["h3_b"], 4)] += 1

# COMMAND ----------

with open("/Volumes/mimi_ws_1/sandbox/src/obs-2024-04-25-walmart-h3maps.json", "w") as fp:
    json.dump(h3_cnt, fp, indent=2)

# COMMAND ----------



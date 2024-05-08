# Databricks notebook source
!pip install h3

# COMMAND ----------

from pyspark.sql.functions import col
import re
import pandas as pd
from collections import Counter
import json
import csv
import h3
from collections import defaultdict
from statistics import mean

# COMMAND ----------

npi_lst = []
npi2rate = {}
with open("/Volumes/mimi_ws_1/sandbox/src/unh_acupuncture_rate_97810.csv", "r") as fp:
    reader = csv.reader(fp)
    header = next(reader)
    for row in reader:
        npi = row[0]
        npi_lst.append(npi)
        npi2rate[npi] = float(row[1])

# COMMAND ----------

h3_of_interest = (spark.read.table("mimi_ws_1.nppes.address_h3")
    .filter(col("npi").isin(npi_lst)))

# COMMAND ----------

pdf = h3_of_interest.select("h3_b", "npi", "name").toPandas()

# COMMAND ----------

h3_cnt_ = defaultdict(list)
for _, row in pdf.iterrows():
    if row["h3_b"] is None:
        # For this, we used Census Geocoder, which comes up as 90% match rate
        # We have about 10% of the addresses that are not geo-coded.
        # For the purpose of this script, we ignore those for now.
        continue
    npi = row["npi"]
    h3_cnt_[h3.h3_to_parent(row["h3_b"], 4)] += [npi2rate[npi]]

# COMMAND ----------

h3_cnt = {}
for k, v in h3_cnt_.items():
    h3_cnt[k] = mean(v)

# COMMAND ----------

with open("/Volumes/mimi_ws_1/sandbox/src/obs-2024-05-07-unh-acupucture-rate-h3maps.json", "w") as fp:
    json.dump(h3_cnt, fp, indent=2)

# COMMAND ----------



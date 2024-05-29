# Databricks notebook source
from pyspark.sql.functions import col, explode, median as _median

# COMMAND ----------

df = (spark.read.table('mimi_ws_1.payermrf.in_network_rates')
          .filter(col('billing_code')=='98941')
          .filter(col('negotiation_arrangement')=='ffs')
          .select('billing_code', 'billing_code_modifier',
                  'negotiated_rate', 'provider_reference', 'source'))
df_prov = (spark.read.table('mimi_ws_1.payermrf.provider_references')
           .select(col('provider_group_id').alias('provider_reference'),
                   'npi_lst', 'source'))
df = (df.join(df_prov, on=['provider_reference', 'source'], how='left')
        .select(explode('npi_lst').alias('npi'), 'negotiated_rate', 'source'))

# COMMAND ----------

df.groupBy("source").agg(_median(col("negotiated_rate")).alias("median_rate")).display()

# COMMAND ----------

df_h3 = (spark.read.table('mimi_ws_1.nppes.address_h3')
            .select('npi','h3_b'))
df = df.join(df_h3, on='npi', how='left')

# COMMAND ----------

pdf = df.filter(col('h3_b').isNotNull()).toPandas()

# COMMAND ----------

!pip install h3

# COMMAND ----------

import pandas as pd
from collections import Counter
import json
import csv
import h3
from collections import defaultdict
from statistics import mean, median

# COMMAND ----------

h3_cnt_ = defaultdict(list)
h3_cnt = {}

for _, row in pdf.iterrows():
    npi = row["npi"]
    h3_cnt_[h3.h3_to_parent(row["h3_b"], 4)] += [row['negotiated_rate']]

for k, v in h3_cnt_.items():
    h3_cnt[k] = median(v)

# COMMAND ----------

with open("/Volumes/mimi_ws_1/sandbox/src/obs-2024-05-28-unh-chiropractor-rate-h3maps.json", "w") as fp:
    json.dump(h3_cnt, fp, indent=2)

# COMMAND ----------



# Databricks notebook source
from pyspark.sql.functions import (
    col,
    first,
    collect_list,
    collect_set,
    concat_ws,
    array_sort,
    lit
)

# COMMAND ----------


pac_id = '0143656355' # Adventist Health System Sunbelt Healthcare Corporation

df_all = None

for fac_type in ['hospital', 'homehealth', 'fqhc', 'hospice', 'ruralhealthclinic', 'snf'][:1]:
        df_fac = (spark.read.table(f'mimi_ws_1.datacmsgov.pc_{fac_type}')
                        .filter(col('_input_file_date')=='2024-04-01')
                        .select('enrollment_id', 
                                'city', 
                                'state', 
                                'doing_business_as_name')
                        .distinct())
        enrollment_ids = [x['enrollment_id'] for x in 
                         (spark.read.table(f'mimi_ws_1.datacmsgov.pc_{fac_type}_owner')
                              .filter(col('_input_file_date')=='2024-04-01')
                              .filter(col('role_text__owner').contains('5% OR GREATER'))
                                .filter(col('associate_id__owner')==pac_id)
                                .select('enrollment_id')
                                .distinct()
                                .collect())]
        df_fac_owner = (spark.read.table(f'mimi_ws_1.datacmsgov.pc_{fac_type}_owner')
                        .select('organization_name', 
                                'associate_id',
                                'enrollment_id',
                                'role_text__owner', 
                                concat_ws(' ',col('organization_name__owner'),
                                             col('first_name__owner'),
                                             col('last_name__owner')).alias('owner_name'),
                                'associate_id__owner')
                        .filter(col('_input_file_date')=='2024-04-01')
                        .filter(col('enrollment_id').isin(enrollment_ids))
                        .filter((col('role_text__owner').contains('5% OR GREATER')))
                )
        df = (df_fac_owner.join(df_fac, 
                                on='enrollment_id', 
                                how='left')
                .withColumn('facility_type', lit(fac_type)))
        if df_all is None:
                df_all = df
        else:
                df_all = df_all.union(df)

# COMMAND ----------

pdf = df_all.toPandas()

# COMMAND ----------

nodes = {}
node_id = 0
for _, row in pdf.iterrows():
    if row['associate_id__owner'] == pac_id:
        nodes[row['owner_name']] = {"category": "owner_of_interest", "name": row['owner_name']}
        break

group_map = {'hospital': 1, 'homehealth': 2, 'fqhc': 3, 'hospice': 4, 'ruralhealthclinic': 5, 'snf': 6}
for _, row in pdf.iterrows():
    node_name = f"{row['doing_business_as_name']} in {row['city']}, {row['state']}"
    if node_name not in nodes:
        nodes[node_name] = {"category": row['facility_type'],
                            "name": node_name}

for _, row in pdf.iterrows():
    if row['owner_name'] not in nodes:
        nodes[row['owner_name']] = {"category": "other_owner", "name": row['owner_name']}

# COMMAND ----------

links = []
for _, row in pdf.iterrows():
    source = row['owner_name']
    target = f"{row['doing_business_as_name']} in {row['city']}, {row['state']}"
    links.append({"source": source, "target": target, "value": 1})

# COMMAND ----------

import json

with open("/Volumes/mimi_ws_1/sandbox/src/pe_ownership_sankey.json", "w") as fp:
    json.dump({"nodes": [x for x in nodes.values()], "links": links}, fp, indent=2)

# COMMAND ----------

pdf

# COMMAND ----------



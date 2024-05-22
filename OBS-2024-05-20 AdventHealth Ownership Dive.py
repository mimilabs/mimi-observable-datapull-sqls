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


pac_id = '5597676429' # Adventist Health System Sunbelt Healthcare Corporation

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
                              .filter((col('role_text__owner').contains('5% OR GREATER') | 
                                        (col('role_text__owner') == 'OPERATIONAL/MANAGERIAL CONTROL')))
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
                        .filter((col('role_text__owner').contains('5% OR GREATER') | 
                                (col('role_text__owner') == 'OPERATIONAL/MANAGERIAL CONTROL')))
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
        nodes[row['owner_name']] = {"group_name": "owner_of_interest", "group": 0, "id": row['owner_name']}
        break

group_map = {'hospital': 1, 'homehealth': 2, 'fqhc': 3, 'hospice': 4, 'ruralhealthclinic': 5, 'snf': 6}
for _, row in pdf.iterrows():
    node_name = f"[{row['facility_type'].upper()}] {row['doing_business_as_name']} in {row['city']}, {row['state']}"
    if node_name not in nodes:
        nodes[node_name] = {"group_name": row['facility_type'],
                            "group": group_map[row['facility_type']],
                            "id": node_name
                            }

for _, row in pdf.iterrows():
    if row['owner_name'] not in nodes:
        nodes[row['owner_name']] = {"group_name": "other_owner", "group": 7, "id": row['owner_name']}

# COMMAND ----------

links = []
for _, row in pdf.iterrows():
    source = row['owner_name']
    target = f"[{row['facility_type'].upper()}] {row['doing_business_as_name']} in {row['city']}, {row['state']}"
    source_id = nodes[source]["id"]
    target_id = nodes[target]["id"]
    value = 1
    if nodes[source]["group"] != 0:
        value = 2
    links.append({"source": source_id, "target": target_id, "value": row["role_text__owner"]})

# COMMAND ----------

import json

with open("/Volumes/mimi_ws_1/sandbox/src/adventhealth_ownership_florida.json", "w") as fp:
    json.dump({"nodes": [x for x in nodes.values()], "links": links}, fp, indent=2)

# COMMAND ----------

df_all.display()

# COMMAND ----------

links

# COMMAND ----------



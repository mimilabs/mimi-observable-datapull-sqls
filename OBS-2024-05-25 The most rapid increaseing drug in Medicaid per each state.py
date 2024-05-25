# Databricks notebook source
from pyspark.sql.functions import col, sum, max as _max, initcap, when, lit
from pyspark.sql import Window

# COMMAND ----------

df_statename = (spark.read.table('mimi_ws_1.nber.ssa2fips_state_and_county')
                    .withColumn('state_name', 
                                when(col('state')=='DC',
                                 lit('District of Columnbia')   
                                ).otherwise(initcap(col('state_name'))))
                    .select('state', 'state_name')
                    .filter(col('state_name').isNotNull())
                    .filter(col('state').isNotNull())
                    .dropDuplicates())

# COMMAND ----------


df_y22 = (spark.read.table('mimi_ws_1.datamedicaidgov.drugutilization')
        .filter(col('year')==2022)
        .filter(col('suppression_used')==False)
        .filter(col('state')!='XX')
        .groupBy(['product_name', 'state'])
        .agg(sum('medicaid_amount_reimbursed').alias('tot_medicaid_amt_y22'))
        .filter(col('tot_medicaid_amt_y22') > 100000))
df_y23 = (spark.read.table('mimi_ws_1.datamedicaidgov.drugutilization')
        .filter(col('year')==2023)
        .filter(col('suppression_used')==False)
        .filter(col('state')!='XX')
        .groupBy(['product_name', 'state'])
        .agg(sum('medicaid_amount_reimbursed').alias('tot_medicaid_amt_y23')))

df = df_y22.join(df_y23, on=['state', 'product_name'], how='left')
w = Window.partitionBy('state')
(df.withColumn('change', col('tot_medicaid_amt_y23')/col('tot_medicaid_amt_y22'))
    .withColumn('change_max', _max(col('change')).over(w))
    .filter(col('change')==col('change_max'))
    .filter(col('change_max') > 1)
    .join(df_statename, on='state', how='left')).display()

# COMMAND ----------



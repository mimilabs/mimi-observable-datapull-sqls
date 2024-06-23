# Databricks notebook source
from pyspark.sql.functions import col, avg, sum

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, CONCAT(measure_code, '; ', indicator_description) AS measure_desc FROM mimi_ws_1.partcd.hedis_national_rates 
# MAGIC WHERE measure_code in ('TRC')

# COMMAND ----------


df_general = (spark.read.table('mimi_ws_1.partcd.hedis_general')
              .select('contract_number', 'plan_type', 
                      'enrollment_yearend', 'hedis_year'))
(spark.read.table('mimi_ws_1.partcd.hedis_measures')
        .filter(col('rate').isNotNull())
        .filter(col('measure_code')=='CBP')
        .join(df_general, on=['contract_number', 'hedis_year'], how='left')
        .withColumn('num', col('rate') * col('enrollment_yearend'))
        .groupBy('hedis_year', 'indicator_key', 'plan_type')
        .agg(sum(col('num')).alias('tot_num'),
             sum(col('enrollment_yearend')).alias('tot_denom')
        )
        .withColumn('rate', col('tot_num')/col('tot_denom'))
        
        ).display()

# COMMAND ----------



# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

df_zillow_raw = spark.read.table('mimi_ws_1.zillow.homevalue_zip')
latest_date = df_zillow_raw.select(f.max('date')).collect()[0][0]
df_zillow = df_zillow_raw.filter(df_zillow_raw.date == latest_date)

# COMMAND ----------

df_adi_tract_raw = spark.read.table('mimi_ws_1.neighborhoodatlas.adi_censustract')
latest_date = df_adi_tract_raw.select(f.max('mimi_src_file_date')).collect()[0][0]
df_adi_tract = (df_adi_tract_raw.filter(f.col('mimi_src_file_date') == latest_date)
                    .select(f.col('fips_censustract').alias('tract'), 'adi_natrank_avg'))
df_tract_to_zip = spark.read.table('mimi_ws_1.huduser.tract_to_zip_mto').select('tract', 'zip')

# COMMAND ----------

df = df_adi_tract.join(df_tract_to_zip, 'tract', 'left').join(df_zillow, 'zip', 'left')

# COMMAND ----------

df.filter(f.col('value').isNotNull()).groupBy('zip').agg(
    f.avg('adi_natrank_avg').alias('adi_natrank_avg_over_zip'),
    f.first('value').alias('zhvi'),
    f.first('metro').alias('metro'),
    f.first('state').alias('state')
).display()

# COMMAND ----------



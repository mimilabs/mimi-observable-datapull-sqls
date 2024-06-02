# Databricks notebook source
from pyspark.sql.functions import col, year, substring, concat

# COMMAND ----------

df_svi = (spark.read.table('mimi_ws_1.cdc.svi_censustract_multiyears')
            .filter(year(col('year'))==2022)
            .filter(col('svi') > 0)
            .select(col('fips').alias('fips_tract'), 'svi'))
df_adi = (spark.read.table('mimi_ws_1.neighborhoodatlas.adi_censusblock ')
            .filter(year(col('_input_file_date'))==2021)
            .select(col('fips').alias('fips_blockgroup'),
                    substring(col('fips'), 1, 11).alias('fips_tract'),
                    'adi_natrank'))
df_cenpop = (spark.read.table('mimi_ws_1.census.centerofpop_bg')
                .withColumn('fips_blockgroup', concat(col('statefp'),
                                                      col('countyfp'),
                                                      col('tractce'),
                                                      col('blkgrpce')))
                .select('fips_blockgroup', 'latitude', 'longitude'))
df = (df_adi.join(df_svi, on='fips_tract', how='left')
        .filter(col('svi') > 0.90)
        .filter(col('adi_natrank') < 10)
        .join(df_cenpop, on='fips_blockgroup', how='left'))

# COMMAND ----------

df.display()

# COMMAND ----------



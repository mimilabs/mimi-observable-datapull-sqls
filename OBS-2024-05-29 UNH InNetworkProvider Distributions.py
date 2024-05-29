# Databricks notebook source
!pip install openpyxl

# COMMAND ----------

from pyspark.sql.functions import col, explode, count, max, substring
import pandas as pd

# COMMAND ----------

pdf = pd.read_excel("/Volumes/mimi_ws_1/huduser/src/ZIP_COUNTY_032024.xlsx",
                    dtype={"ZIP": str, "COUNTY": str})
pdf = (pdf.sort_values(by="RES_RATIO")
    .drop_duplicates(subset=["ZIP"], keep="last")
    .loc[:,["ZIP", "COUNTY"]])
pdf.columns = ["zip5", "fips"]
df_zip2fips = spark.createDataFrame(pdf)

# COMMAND ----------

df_nppes = (spark.read.table('mimi_ws_1.nppes.npidata')
                .filter(col('_input_file_date') > '2024-05-01')
                .select("npi", 
                        substring(col("provider_business_practice_location_address_postal_code"),
                                  1, 5)
                        .alias("zip5")))
df_taxonomy = (spark.read.table('mimi_ws_1.nppes.taxonomy_se')
                .filter(col('taxonomy_dt_e') > '2024-05-01')
                .filter(col('taxonomy_code').isNotNull())
                .filter(col('is_main')=='Y')
                .select('npi', 'taxonomy_desc')
                .dropDuplicates(['npi']))
df_nppes = (df_nppes.join(df_taxonomy, on="npi", how="left")
                .join(df_zip2fips, on="zip5", how="left")
                .filter(col("fips").isNotNull()))

# COMMAND ----------

df_unh = (spark.read.table('mimi_ws_1.payermrf.provider_references')
        .filter(col("tin_type")=='ein')
        .select(explode("npi_lst").alias("npi"))
        .dropDuplicates())

# COMMAND ----------

df = df_unh.join(df_nppes, on="npi", how="left")

# COMMAND ----------

# Family Nurse Practitioner
# Family Medicine Physician
# Internal Medicine Physician
# df.groupBy('taxonomy_desc').count().display()

# COMMAND ----------

df_fnp = (df.filter(col("taxonomy_desc")=="Family Nurse Practitioner")
            .groupBy('fips')
            .agg(count('*').alias('cnt_fnp')))
df_fmp = (df.filter(col("taxonomy_desc")=="Family Medicine Physician")
            .groupBy('fips')
            .agg(count('*').alias('cnt_fmp')))

# COMMAND ----------

(df_fnp.join(df_fmp, on="fips", how="left")
    .filter(col("cnt_fmp") > 10)
    .filter(col("cnt_fnp") > 10)
    .withColumn("ratio", col("cnt_fnp")/col("cnt_fmp"))).display()

# COMMAND ----------



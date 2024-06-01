# Databricks notebook source
# MAGIC %sql
# MAGIC -- NY FIPS statecode is 36
# MAGIC WITH svi AS (
# MAGIC   SELECT fips AS fips_tract, svi 
# MAGIC   FROM mimi_ws_1.cdc.svi_censustract_multiyears 
# MAGIC   WHERE YEAR(year)=2022 AND svi > 0)
# MAGIC SELECT *, COUNT(*) AS cnt FROM (
# MAGIC SELECT a.adi_natrank, ROUND(b.svi*100) AS svi FROM (
# MAGIC SELECT 
# MAGIC   fips AS fips_blockgroup, 
# MAGIC   SUBSTRING(fips, 1, 11) AS fips_tract,
# MAGIC   adi_natrank
# MAGIC FROM mimi_ws_1.neighborhoodatlas.adi_censusblock 
# MAGIC WHERE YEAR(_input_file_date)=2021) AS a
# MAGIC LEFT JOIN svi AS b ON a.fips_tract=b.fips_tract)
# MAGIC GROUP BY adi_natrank, svi

# COMMAND ----------



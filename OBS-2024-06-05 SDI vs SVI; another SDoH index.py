# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC WITH svi AS (
# MAGIC   SELECT fips AS fips_tract, svi 
# MAGIC   FROM mimi_ws_1.cdc.svi_censustract_multiyears 
# MAGIC   WHERE YEAR(year)=2022 AND svi >= 0)
# MAGIC   
# MAGIC SELECT *, COUNT(*) AS cnt 
# MAGIC FROM (
# MAGIC   SELECT a.sdi_score, ROUND(b.svi*100) AS svi 
# MAGIC   FROM (
# MAGIC     SELECT 
# MAGIC       censustract_fips AS fips_tract, 
# MAGIC       sdi_score
# MAGIC     FROM mimi_ws_1.grahamcenter.sdi_censustract 
# MAGIC     WHERE YEAR(_input_file_date)=2019) AS a
# MAGIC   LEFT JOIN svi AS b ON a.fips_tract=b.fips_tract)
# MAGIC GROUP BY sdi_score, svi

# COMMAND ----------



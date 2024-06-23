# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT CONCAT(statefp, countyfp) as fips_county, stname, couname FROM mimi_ws_1.census.centerofpop_co;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM (SELECT *, substr(fips_county, 1, 2) AS fips_state 
# MAGIC     FROM mimi_ws_1.neighborhoodatlas.adi_county 
# MAGIC     WHERE YEAR(mimi_src_file_date) = 2022) AS a
# MAGIC LEFT JOIN (SELECT CONCAT(statefp, countyfp) as fips_county, stname, couname FROM mimi_ws_1.census.centerofpop_co) AS b 
# MAGIC ON a.fips_county = b.fips_county

# COMMAND ----------

 A 

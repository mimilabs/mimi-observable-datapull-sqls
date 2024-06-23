# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT indicator, AVG(data_value) FROM mimi_ws_1.cdc.vsrr_drugoverdose GROUP BY indicator;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mimi_ws_1.cdc.vsrr_drugoverdose WHERE indicator = 'Number of Drug Overdose Deaths' AND state='US';

# COMMAND ----------



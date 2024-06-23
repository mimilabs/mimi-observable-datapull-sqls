# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT MAX(_input_file_date) FROM mimi_ws_1.datacmsgov.pendingilt;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(_input_file_date) FROM mimi_ws_1.datacmsgov.pendingilt;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *, datediff(dt_max, dt_min) AS days_till_approval
# MAGIC FROM (SELECT npi, 
# MAGIC   MAX(_input_file_date) AS dt_max,
# MAGIC   MIN(_input_file_date) AS dt_min
# MAGIC FROM mimi_ws_1.datacmsgov.pendingilt
# MAGIC GROUP BY npi) 
# MAGIC WHERE dt_min < '2023-09-01' AND dt_min > '2023-06-01';

# COMMAND ----------



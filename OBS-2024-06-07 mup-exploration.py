# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM mimi_ws_1.datacmsgov.mupdme WHERE rfrg_npi = '1003000126' AND hcpcs_cd = 'E0431'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _input_file_date, AVG(bene_cc_bh_schizo_othpsy_v1_pct) FROM mimi_ws_1.datacmsgov.mupdme_prvdr GROUP BY _input_file_date

# COMMAND ----------



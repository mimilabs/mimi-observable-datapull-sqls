# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT week_ending, 
# MAGIC     SUM(staff_weekly_confirmed_covid19) AS staff_weekly_confirmed_covid19, 
# MAGIC     SUM(residents_weekly_confirmed_covid19) as residents_weekly_confirmed_covid19,
# MAGIC     SUM(staff_weekly_confirmed_covid19)/SUM(residents_weekly_confirmed_covid19) as staff_to_res_ratio
# MAGIC FROM mimi_ws_1.datacmsgov.covid19nursinghomes 
# MAGIC GROUP BY week_ending

# COMMAND ----------



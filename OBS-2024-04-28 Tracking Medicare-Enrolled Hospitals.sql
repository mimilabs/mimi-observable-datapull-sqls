-- Databricks notebook source
SELECT _input_file_date, state, COUNT(ccn) FROM mimi_ws_1.datacmsgov.pc_hospital GROUP BY _input_file_date, state;

-- COMMAND ----------


SELECT _input_file_date, state, COUNT(DISTINCT ccn_remapped) as cnt_ccn FROM mimi_ws_1.datacmsgov.pc_hospital GROUP BY _input_file_date, state;

-- COMMAND ----------



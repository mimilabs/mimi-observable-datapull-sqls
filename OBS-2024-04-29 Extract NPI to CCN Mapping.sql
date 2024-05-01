-- Databricks notebook source
SELECT facility_type, 
        other_id_state, 
        AVG(ccn_cnt) as avg_ccn_per_npi,
        MEDIAN(ccn_cnt) as median_ccn_per_npi,
        MAX(ccn_cnt) as max_ccn_per_npi,
        COUNT(*) as entry_cnt
FROM (SELECT npi, facility_type, other_id_state, COUNT(*) as ccn_cnt 
      FROM mimi_ws_1.nppes.otherid_ccn_se GROUP BY npi, facility_type, other_id_state)
GROUP BY facility_type, 
        other_id_state
HAVING entry_cnt > 10;

-- COMMAND ----------



-- COMMAND ----------

SELECT npi, 
  other_id as ccn, 
  other_id_dt_s as first_observed_date,  
  other_id_dt_e as last_observed_date,
  facility_type  
FROM mimi_ws_1.nppes.otherid_ccn_se;

-- COMMAND ----------



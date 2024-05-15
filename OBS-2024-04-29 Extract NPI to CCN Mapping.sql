-- Databricks notebook source
/*
This SQL script combines the NPPES and Provider Characteristics datasets to create a CCN-to-NPI mapping file. 
The output data is used in this Observable plot: https://observablehq.com/d/75477d5dbeb7fe5f
Take a look at the Observable script and its description to learn how the data was used and what the extracted data means.

The output mapping file is available at https://github.com/mimilabs/mimi-datafiles

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- In what situations would you need this type of information?
*/

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

SELECT npi, 
  other_id as ccn, 
  other_id_dt_s as first_observed_date,  
  other_id_dt_e as last_observed_date,
  facility_type  
FROM mimi_ws_1.nppes.otherid_ccn_se;

-- COMMAND ----------



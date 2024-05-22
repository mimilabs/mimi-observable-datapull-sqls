-- Databricks notebook source
/*
This script uses the Medicare Geographic Variation data to calculate the FFS vs MA ratio of each county in the US.
*/
-- Select the county code, FFS ratio, and MA ratio
SELECT bene_geo_cd, 
  benes_ffs_cnt/benes_wth_ptaptb_cnt AS ffs_ratio,
  benes_ma_cnt/benes_wth_ptaptb_cnt AS ma_ratio
FROM mimi_ws_1.datacmsgov.geovariation 
-- Filter the data for the year 2022, county level, and all age levels
WHERE year=2022 
  AND bene_geo_lvl = 'County' 
  AND bene_age_lvl='All' 
  -- Exclude null values in the FFS ratio and MA ratio
  AND benes_ffs_cnt/benes_wth_ptaptb_cnt IS NOT NULL;

-- COMMAND ----------



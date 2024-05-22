-- Databricks notebook source
/*
Exploratory visuzliation to find out which "disease label" to track.
After examining the top 20 viruses, I found that Pertussis (Whooping Cough) is on the rise.
In the next cell, I go dive deep into that label.
*/
WITH topK AS (SELECT label, SUM(current_week) as tot_cases
    FROM mimi_ws_1.cdc.nndss 
    WHERE reporting_area = 'TOTAL' 
    GROUP BY label
    ORDER BY tot_cases DESC LIMIT 20)

SELECT reporting_area, label, current_week, report_date
FROM mimi_ws_1.cdc.nndss 
WHERE reporting_area = 'TOTAL' AND label IN (SELECT label FROM topK);

-- COMMAND ----------

SELECT location1, 
  report_date, 
  current_week
FROM mimi_ws_1.cdc.nndss 
WHERE label = 'Pertussis' AND location1 IS NOT NULL AND current_week IS NOT NULL;

-- COMMAND ----------



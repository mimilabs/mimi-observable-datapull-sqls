-- Databricks notebook source
-- NWSS COVID
/*
This SQL script uses the NWSS (national sewer data) from CDC to track/monitor COVID-19 in California. 
The output data is used in this Observable plot: https://observablehq.com/d/4d810abcceb0c810

Take a look at these Observable script and its description to learn how the data was used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script. Also, please read the column/table descriptions of the tables used in the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- In what situations would you need this type of information?
*/

SELECT county_names, date_end, AVG(percentile)
FROM (
  SELECT a.wwtp_id, a.county_names, a.date_end, a.percentile, b.latitude, b.longitude
  FROM mimi_ws_1.cdc.nwss_covid AS a
  LEFT JOIN
  (SELECT key_plot_id, latitude, longitude 
    FROM mimi_ws_1.cdc.nwss_mpox) AS b 
  ON a.key_plot_id=b.key_plot_id
  WHERE a.reporting_jurisdiction = 'California' AND
    b.latitude IS NOT NULL AND
    b.longitude IS NOT NULL AND
    a.percentile <= 100) 
GROUP BY county_names, date_end;

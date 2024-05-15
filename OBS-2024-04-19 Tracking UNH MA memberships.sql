-- Databricks notebook source
/*
This SQL script tracks the member enrollment numbers for the UnitedHealthcare Medicare Advantage plans.
The output data is used in this Observable plot: https://observablehq.com/d/407ca52bfba5fe2d
Take a look at the Observable script and its description to see how the data is used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- In what situations, would you need this type of information?
*/
SELECT _input_file_date AS date, plan_type, SUM(enrollment) as enrollment FROM (
    SELECT * FROM mimi_ws_1.partcd.cpsc_combined WHERE parent_organization ILIKE "UnitedHealth%")
GROUP BY _input_file_date, plan_type;

-- COMMAND ----------



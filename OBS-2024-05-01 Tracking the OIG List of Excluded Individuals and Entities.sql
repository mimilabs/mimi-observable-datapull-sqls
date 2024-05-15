-- Databricks notebook source
/*
This SQL script uses the List of Excluded Individuals/Entities file from the Office of Inspector General (OIG) to visualize the exclusion trends.
The output data is used in this Observable plot: https://observablehq.com/d/3e4c53abe2fef70f
Take a look at the Observable script and its description to learn how the data was used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- In what situations would you need this type of information?
*/

SELECT excl_description, excldate, COUNT(*) as cnt 
FROM mimi_ws_1.hhsoig.leie 
GROUP BY excl_description, excldate;

-- COMMAND ----------



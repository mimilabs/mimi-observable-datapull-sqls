-- Databricks notebook source
/*
This SQL script produces a list of Medicare Advantage plans with their parent organizations.
The output data is used in this Observable plot: https://observablehq.com/d/dd4b4209eb270f24
Take a look at the Observable script and its description to see how the data is used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script.

Questions: 
- What do you want to see more in the data/chart? 
  e.g. I want to see a different time frame as well: OBS-2024-04-21 Making a tree diagram for the Medicare Advantage plans in 2021 - 2024
- How would you improve the query/visualization? 
- In what situations, would you need this type of information?
*/
SELECT DISTINCT CONCAT_WS('^', 
  parent_organization, 
  organization_marketing_name, 
  plan_type, 
  contract_id, 
  CONCAT(plan_name,
    ' (SNP: ',
    snp_plan,
    ') ',
    ' since ',
    contract_effective_date
  )) AS path, 
  parent_organization
FROM mimi_ws_1.partcd.cpsc_contract 
WHERE _input_file_date = '2024-04-01';

-- COMMAND ----------



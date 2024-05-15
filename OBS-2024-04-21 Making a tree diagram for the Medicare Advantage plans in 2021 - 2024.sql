-- Databricks notebook source
/*
This SQL script tracks the member enrollment numbers for the UnitedHealthcare Medicare Advantage plans.
I wrote this script to enhance the previous work: "OBS-2024-04-19 Making a tree diagram for the Medicare Advantage plans in 2024.sql"
The output data is used in this Observable plot: https://observablehq.com/d/f17a253d68266b92
Take a look at the Observable script and its description to learn how the data was used and what the extracted data means.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- In what situations would you need this type of information?
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
  parent_organization,
  _input_file_date
FROM cpsc_contract 
WHERE _input_file_date in ('2024-04-01', '2023-04-01', '2022-04-01', '2021-04-01');

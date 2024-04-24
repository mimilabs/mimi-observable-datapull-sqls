-- Databricks notebook source
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

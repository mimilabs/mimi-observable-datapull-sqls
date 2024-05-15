-- Databricks notebook source
/*
This SQL script uses the Provider Characteristics data provided on the Data.CMS.gov site to track the Medicare-enrolled hospitals over time. 
The output data is used in this Observable plot: https://observablehq.com/d/6ee9c0aad49d70ca
Take a look at the Observable script and its description to learn how the data was used and what the extracted data means.

NOTE: So, a long story short, I posted this observation on my LinkedIn (see: https://www.linkedin.com/feed/update/urn:li:activity:7190511043272417280/), and here are the comments I received:

- David Roberts, Analytics Engineering Manager @ Astrana Health | Biomedical Informatics
Yubin Park, PhD spitballing... Perhaps Pecos was not up to date bc data validation was not required to get paid?
https://www.hallrender.com/2023/07/28/medicare-exact-match-update-provider-compliance-required-effective-8-1-2023/
In my experience, the PECOS data can be very messy as it relates to CCN's posting on claims, especially with CHOW
I think this is it! This seems to be the most promising explanation for this behavior.

Another lesson here: there are many back stories behind the data. We cannot just interpret the numbers as numbers!

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- In what situations would you need this type of information?
*/

SELECT _input_file_date, state, COUNT(ccn) 
FROM mimi_ws_1.datacmsgov.pc_hospital 
GROUP BY _input_file_date, state;

-- COMMAND ----------


SELECT _input_file_date, state, COUNT(DISTINCT ccn_remapped) as cnt_ccn 
FROM mimi_ws_1.datacmsgov.pc_hospital 
GROUP BY _input_file_date, state;

-- COMMAND ----------



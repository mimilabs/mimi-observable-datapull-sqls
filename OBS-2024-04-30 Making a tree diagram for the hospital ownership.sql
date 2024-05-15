-- Databricks notebook source
/*
This SQL script uses the Hospital Ownership data from Data.CMS.gov to visualize the hospital ownership structure. 
The output data is used in this Observable plot: https://observablehq.com/d/375225beead5705a
Take a look at the Observable script and its description to learn how the data was used and what the extracted data means.

NOTE: The Centers for Medicare & Medicaid Services started publishing hospital ownership data for the last couple of years [2]. The data is self-reported. Some folks may not see the data has limited usage, but this is a significant step forward for business transparency, and to be honest, there is a lot to uncover.

One downside of the CMS data site is that the table view of the data was too much to read. The ownerships are better represented in a tree structure, so I leveraged Observable to enhance the readability.

NOTE: Use /explain on the Databricks Assistant view (Toggle Assistant, top-right on the cell) to know more about the script.

Questions: 
- What more do you want to see in the data/chart? 
- How would you improve the query/visualization? 
- In what situations would you need this type of information?
*/

SELECT DISTINCT CONCAT_WS('^', 
  CONCAT(
    COALESCE(
      ANY_VALUE(doing_business_as_name), 
      organization_name
    ), 
    ' (', 
    associate_id, 
    ')'
  ),
  role_text__owner, 
  CONCAT(
    COALESCE(organization_name__owner,
              CONCAT(COALESCE(first_name__owner, ''), 
                      ' ', 
                      COALESCE(last_name__owner, ''))),
    ' (',
    associate_id__owner,
    ') since ',
    MIN(association_date__owner)
  )
  ) AS path, 
  CONCAT(
    COALESCE(
      ANY_VALUE(doing_business_as_name), 
      organization_name
    ), 
    ' (', 
    associate_id, 
    ')'
  ) AS organization,
  ANY_VALUE(state) AS state
FROM (
  SELECT DISTINCT organization_name, 
    a.associate_id, 
    state,
    role_text__owner, 
    organization_name__owner, 
    first_name__owner, 
    last_name__owner,
    associate_id__owner,
    association_date__owner,
    doing_business_as_name
  FROM mimi_ws_1.datacmsgov.pc_hospital_owner AS a
  LEFT JOIN (SELECT DISTINCT associate_id, state, doing_business_as_name FROM mimi_ws_1.datacmsgov.pc_hospital) AS b
  ON a.associate_id = b.associate_id
  WHERE a._input_file_date = '2024-04-01')
GROUP BY organization_name, 
    associate_id, 
    role_text__owner, 
    organization_name__owner, 
    first_name__owner, 
    last_name__owner,
    associate_id__owner;

-- COMMAND ----------



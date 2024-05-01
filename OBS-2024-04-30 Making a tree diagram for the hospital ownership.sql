-- Databricks notebook source
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



{{ config(materialized="table") }}


WITH parsed_data AS (
  SELECT
    DATE(EXTRACTION_DATE) as date,
    EXTRACTION_DATE,
    PARSE_JSON(SOURCE_JSON) as json_data
  from {{ source("PROD_LANDING", "raw_veFXS_supply") }}
),
ranked_data AS (
  SELECT
    date,
    EXTRACTION_DATE,
    json_data,
    ROW_NUMBER() OVER (PARTITION BY date ORDER BY EXTRACTION_DATE DESC) as rn
  FROM parsed_data
), current_json AS (
  SELECT * 
  FROM ranked_data
  WHERE rn = 1
),
flattened_data AS (
  SELECT
    f.value:txn_date::STRING as date,
    f.value:delta::FLOAT as daily_supply,
    f.value:veFXS::FLOAT as total_supply,
  FROM current_json,
  LATERAL FLATTEN(input => json_data) f
)
SELECT
  -- This will properly handle the UTC timezone indicator
  TRY_TO_TIMESTAMP(REPLACE(date, ' UTC', ''), 'YYYY-MM-DD HH24:MI:SS.FF3') as date,
  daily_supply,
  total_supply
FROM flattened_data
ORDER BY date
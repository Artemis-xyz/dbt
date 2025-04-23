{{ config(materialized="table") }}

WITH parsed_data AS (
  SELECT
    DATE(EXTRACTION_DATE) as date,
    EXTRACTION_DATE,
    PARSE_JSON(SOURCE_JSON) as json_data
  from {{ source("PROD_LANDING", "raw_jupiter_aggregator_volume") }}
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
    value:date::STRING as date_str,
    value:overall::FLOAT as overall,
    value:single::FLOAT as single,
  FROM current_json,
  LATERAL FLATTEN(input => json_data) f
)
SELECT
  date_str as date,
  overall,
  single
FROM flattened_data
ORDER BY date
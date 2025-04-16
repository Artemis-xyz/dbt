{{
    config(
        materialized="table",
        alias="fact_meteora_api_metrics",
    )
}}

WITH parsed_data AS (
  SELECT
    DATE(EXTRACTION_DATE) as date,
    EXTRACTION_DATE,
    PARSE_JSON(SOURCE_JSON) as json_data
  FROM landing_database.prod_landing.raw_meteora_metrics
),
ranked_data AS (
  SELECT
    date,
    EXTRACTION_DATE,
    json_data,
    ROW_NUMBER() OVER (PARTITION BY date ORDER BY EXTRACTION_DATE DESC) as rn
  FROM parsed_data
)
SELECT
  date,
  json_data:daily_fee::FLOAT as daily_fee,
  json_data:daily_trade_volume::FLOAT as daily_trade_volume,
  json_data:total_fee::FLOAT as total_fee,
  json_data:total_trade_volume::FLOAT as total_trade_volume,
  json_data:total_tvl::FLOAT as total_tvl
FROM ranked_data
WHERE rn = 1
ORDER BY date


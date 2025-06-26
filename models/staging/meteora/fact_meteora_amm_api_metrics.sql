{{
    config(
        materialized="table",
        alias="fact_meteora_amm_api_metrics",
    )
}}

WITH parsed_data AS (
  SELECT
    DATE(EXTRACTION_DATE) as date,
    EXTRACTION_DATE,
    PARSE_JSON(SOURCE_JSON) as json_data
  FROM {{ source('PROD_LANDING', 'raw_meteora_amm_metrics') }}
),
ranked_data AS (
  SELECT
    date,
    EXTRACTION_DATE,
    json_data,
    ROW_NUMBER() OVER (PARTITION BY date ORDER BY EXTRACTION_DATE DESC) as rn
  FROM parsed_data
),
latest_data AS (
  SELECT
    date,
    json_data
  FROM ranked_data
  WHERE rn = 1
)

SELECT
  date,
  -- Extract values directly from the JSON
  json_data:dynamic_amm_tvl::DECIMAL(38,8) as amm_tvl,
  json_data:dynamic_amm_daily_volume::DECIMAL(38,8) as amm_daily_volume,
  json_data:dynamic_amm_total_volume::DECIMAL(38,8) as amm_total_volume,
  json_data:dynamic_amm_daily_fee::DECIMAL(38,8) as amm_daily_fee,
  json_data:dynamic_amm_total_fee::DECIMAL(38,8) as amm_total_fee,
  
  -- Extract from nested objects
  json_data:dynamic_amm.tvl::DECIMAL(38,8) as dynamic_amm_tvl,
  json_data:dynamic_amm.daily_volume::DECIMAL(38,8) as dynamic_amm_daily_volume,
  json_data:dynamic_amm.total_volume::DECIMAL(38,8) as dynamic_amm_total_volume,
  json_data:dynamic_amm.daily_fee::DECIMAL(38,8) as dynamic_amm_daily_fee,
  json_data:dynamic_amm.total_fee::DECIMAL(38,8) as dynamic_amm_total_fee,
  
  -- Extract LST metrics
  json_data:lst.tvl::DECIMAL(38,8) as lst_tvl,
  json_data:lst.daily_volume::DECIMAL(38,8) as lst_daily_volume,
  json_data:lst.total_volume::DECIMAL(38,8) as lst_total_volume,
  json_data:lst.daily_fee::DECIMAL(38,8) as lst_daily_fee,
  json_data:lst.total_fee::DECIMAL(38,8) as lst_total_fee,
  
  -- Extract farms metrics
  json_data:farms.tvl::DECIMAL(38,8) as farms_tvl,
  json_data:farms.daily_volume::DECIMAL(38,8) as farms_daily_volume,
  json_data:farms.total_volume::DECIMAL(38,8) as farms_total_volume,
  json_data:farms.daily_fee::DECIMAL(38,8) as farms_daily_fee,
  json_data:farms.total_fee::DECIMAL(38,8) as farms_total_fee,
  
  -- Extract multitokens metrics
  json_data:multitokens.tvl::DECIMAL(38,8) as multitokens_tvl,
  json_data:multitokens.daily_volume::DECIMAL(38,8) as multitokens_daily_volume,
  json_data:multitokens.total_volume::DECIMAL(38,8) as multitokens_total_volume,
  json_data:multitokens.daily_fee::DECIMAL(38,8) as multitokens_daily_fee,
  json_data:multitokens.total_fee::DECIMAL(38,8) as multitokens_total_fee
FROM latest_data
ORDER BY date
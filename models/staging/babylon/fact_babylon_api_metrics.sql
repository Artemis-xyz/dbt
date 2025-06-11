{{
    config(
        materialized="table",
        alias="fact_babylon_api_metrics",
    )
}}

WITH parsed_data AS (
  SELECT
    DATE_TRUNC('day', EXTRACTION_DATE) as date,
    EXTRACTION_DATE,
    PARSE_JSON(SOURCE_JSON):data as json_data
  FROM {{ source('PROD_LANDING', 'raw_babylon_metrics') }}
),
ranked_data AS (
  SELECT
    date,
    EXTRACTION_DATE,
    json_data,
    ROW_NUMBER() OVER (PARTITION BY date ORDER BY EXTRACTION_DATE DESC) as rn
  FROM parsed_data
)
, latest_data as (
  SELECT
    date,
    json_data:active_delegations::FLOAT as active_delegations,
    json_data:total_active_delegations::FLOAT as total_active_delegations,
    json_data:active_finality_providers::FLOAT as active_finality_providers,
    json_data:total_active_finality_providers::FLOAT as total_active_finality_providers,
    json_data:active_tvl::FLOAT as active_tvl_satoshis,
    json_data:active_tvl::FLOAT / 1e8 as active_tvl_btc,
    json_data:total_active_tvl::FLOAT as total_active_tvl_satoshis,
    json_data:total_active_tvl::FLOAT / 1e8 as total_active_tvl_btc,
  FROM ranked_data
  WHERE rn = 1
  ORDER BY date
)
SELECT
  date,
  active_delegations,
  total_active_delegations,
  active_finality_providers,
  total_active_finality_providers,
  active_tvl_satoshis,
  active_tvl_btc,
  total_active_tvl_satoshis,
  total_active_tvl_btc,
  b.price as price,
  total_active_tvl_btc * b.price as total_active_tvl_usd
FROM latest_data
LEFT JOIN {{ source('BITCOIN_FLIPSIDE_PRICE', 'ez_prices_hourly') }} b
  on latest_data.date = b.hour
  and b.symbol = 'BTC'
where date > '2025-04-30' --API calls start May 1 2025

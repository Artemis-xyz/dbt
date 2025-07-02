{{
    config(
        materialized='table',
        snowflake_warehouse='GMX'
    )
}}

-- First, calculate how many hours to generate
WITH hours_needed AS (
  SELECT DATEDIFF(hour, '2022-01-01 05:00:00.000', '2023-06-09 05:00:00.000') AS hour_count
),
-- Use a recursive CTE to generate the sequence
date_series AS (
  SELECT 
    '2022-01-01 05:00:00.000'::TIMESTAMP AS hour,
    1 AS seq
  FROM hours_needed
  
  UNION ALL
  
  SELECT
    DATEADD(hour, 1, hour),
    seq + 1
  FROM date_series, hours_needed
  WHERE seq < hour_count
),
usdc_historic_prices as (
    -- Select the data with our constant values
    SELECT
      hour,
      LOWER('0xff970a61a04b1ca14834a43f5de4533ebddb5cc8') AS token_address,
      'USDC' AS token_symbol,
      'USD Coin (Arb1)' AS name,
      6 AS decimal,
      1 AS price,
      'arbitrum' AS blockchain,
      FALSE AS is_native,
      FALSE AS is_imputed,
      FALSE AS is_deprecated,
      FALSE AS is_verified,
      NULL AS ez_prices_hourly_id,
      CURRENT_TIMESTAMP() AS inserted_timestamp,
      CURRENT_TIMESTAMP() AS modified_timestamp,
    FROM 
      date_series
    ORDER BY 
      hour
),
updated_pricing as (
    SELECT * FROM ARBITRUM_FLIPSIDE.PRICE.EZ_PRICES_HOURLY
    UNION ALL
    SELECT * FROM usdc_historic_prices
)
select * from updated_pricing
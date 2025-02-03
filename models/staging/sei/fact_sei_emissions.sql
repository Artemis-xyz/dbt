{{
    config(
        materialized='table',
        snowflake_warehouse='SEI',
    )
}}

SELECT
  DATE_TRUNC(DAY, BLOCK_TIMESTAMP) as date,
  SUM(AMOUNT) / 1e6 as rewards_amount 
FROM
    {{ source('SEI_FLIPSIDE', 'fact_staking_rewards') }}
GROUP BY
  1
ORDER BY
  1 DESC
{{
    config(
        materialized="table",
        snowflake_warehouse="AXELAR"
    )
}}

-- Axelar Staking Mints
SELECT
  DATE_TRUNC('DAY', block_timestamp) as date,
  SUM(amount/1e6) as mints
FROM
  axelar_flipside.gov.fact_staking_rewards
WHERE action = 'withdraw_rewards'
GROUP BY 1
ORDER BY 1 DESC
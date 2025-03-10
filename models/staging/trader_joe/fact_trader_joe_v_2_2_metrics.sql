{{
    config(
        materialized='table',
        snowflake_warehouse='TRADER_JOE'
    )
}}

SELECT 
    date(block_timestamp) AS date,
    chain, 
    version,
    count(distinct user_address) as unique_traders,
    count(*) as daily_txns,
    sum(volume_usd) as total_volume,
    sum(fee_usd) as total_fees, 
    sum(protocol_fees_usd) as protocol_fees
FROM {{ref('fact_trader_joe_v_2_2_dex_swaps')}}
GROUP BY 1, 2, 3
ORDER BY 1 DESC 

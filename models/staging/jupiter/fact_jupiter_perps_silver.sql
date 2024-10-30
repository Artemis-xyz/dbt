{{
    config(
        materialized="table",
        unique_key="date",
        snowflake_warehouse="JUPITER",
    )
}}


SELECT
    block_timestamp::date as date
    , sum(size_usd) as volume
    , sum(fee_usd) as fees
    , count(distinct owner) as traders
    , count(distinct tx_id) as txns
FROM {{ ref('fact_jupiter_perps_txs') }} 
GROUP BY 1 
ORDER BY 1 DESC
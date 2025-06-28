{{ config(materialized="incremental", snowflake_warehouse="HYPERLIQUID", unique_key=["date"]) }}

with 
fundamental_data as (
    {{ get_goldsky_chain_fundamental_metrics("hyperliquid") }}
),
price as (
    {{ get_coingecko_price_with_latest('hyperliquid') }}
)

SELECT 
    fd.date
    , fd.daa
    , fd.txns
    , fd.hyperevm_burns_native
    , hyperevm_burns_native * price as hyperevm_burns
    , 'hyperliquid' as chain
FROM fundamental_data fd
LEFT JOIN price p
ON fd.date = p.date

{{ config(materialized="incremental", snowflake_warehouse="STORY", unique_key=["date"]) }}


with fundamental_data as (
    {{ get_goldsky_chain_fundamental_metrics("story") }}
),
price as (
    {{ get_coingecko_price_with_latest('story-2') }}
)

SELECT 
    fd.date,
    fd.daa,
    fd.txns,
    fd.fees_native,
    fees_native * price as fees
FROM fundamental_data fd
LEFT JOIN price p
ON fd.date = p.date

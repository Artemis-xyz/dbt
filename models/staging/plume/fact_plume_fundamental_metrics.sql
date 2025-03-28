{{ config(materialized="incremental", snowflake_warehouse="PLUME", unique_key=["date"]) }}


with fundamental_data as (
    {{ get_goldsky_chain_fundamental_metrics("plume") }}
),
price as (
    {{ get_coingecko_price_with_latest('plume') }}
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

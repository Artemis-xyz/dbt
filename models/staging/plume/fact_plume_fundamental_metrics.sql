{{ config(materialized="incremental", snowflake_warehouse="PLUME", unique_key=["date"]) }}


with fundamental_data as (
    {{ get_goldsky_chain_fundamental_metrics("plume") }}
)
, rwa_tvl as (
    select 
        date
        , sum(rwa_supply_usd) as rwa_tvl 
    from {{ ref('fact_plume_rwa_supply_by_date_and_chain') }} 
    group by date 
), price as (
    {{ get_coingecko_price_with_latest('plume') }}
)
, stablecoin_tvl as (
    select 
        date
        , sum(stablecoin_supply_usd) as stablecoin_tvl 
    from {{ ref('fact_plume_stablecoin_supply_by_date_and_chain') }} 
    group by date 
)
SELECT 
    fd.date
    , fd.daa
    , fd.txns
    , fd.fees_native
    , fees_native * price as fees
    , rwa_tvl.rwa_tvl
    , stablecoin_tvl.stablecoin_tvl as stablecoin_total_supply
FROM fundamental_data fd
LEFT JOIN price p
ON fd.date = p.date
LEFT JOIN rwa_tvl
ON fd.date = rwa_tvl.date
LEFT JOIN stablecoin_tvl
ON fd.date = stablecoin_tvl.date

{{
    config(
        materialized="table",
        snowflake_warehouse="IMMUTABLE_X",
        database="immutable_x",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    nft_metrics as ({{ get_nft_metrics("immutable_x") }}),
    price_data as ({{ get_coingecko_metrics("immutable-x") }})
select
    date
    , 'immutable_x' as chain
    , nft_trading_volume
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    -- Chain Metrics
    , nft_trading_volume AS chain_nft_trading_volume
from nft_metrics
left join price_data using (date)

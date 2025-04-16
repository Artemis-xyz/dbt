-- buyback started on March 25th. 25% of all trading fees
{{
    config(
        materialized="table",
        snowflake_warehouse="DYDX",
        database="dydx",
        schema="core",
        alias="ez_metrics",
    )
}}

WITH 
    date_spine AS (
        SELECT * 
        FROM {{ ref('dim_date_spine') }}
        WHERE date BETWEEN '2020-09-20' AND TO_DATE(SYSDATE())
    ),
    trading_volume_data_v4 as (
        select date, trading_volume
        from {{ ref("fact_dydx_v4_trading_volume") }}
    ),
    fees_data_v4 as (
        select date, maker_fees, taker_fees, fees
        from {{ ref("fact_dydx_v4_fees") }}
    ),
    chain_data_v4 as (
        select date, txn_fees, 
        from {{ ref("fact_dydx_v4_txn_and_trading_fees") }}
    ),
    unique_traders_data_v4 as (
        select date, unique_traders
        from {{ ref("fact_dydx_v4_unique_traders") }}
    ),
    trading_volume_data as (
        select date, trading_volume
        from {{ ref("fact_dydx_trading_volume") }}
        where market_pair is null
    ),
    unique_traders_data as (
        select date, unique_traders
        from {{ ref("fact_dydx_unique_traders") }}
        where unique_traders < 1e6
    )
    , price_data as (
        {{ get_coingecko_metrics("dydx-chain") }}
    )
select
    date_spine.date as date
    , 'dydx' as app
    , 'DeFi' as category
    -- Not accounting for v4 to support backwards compitability. When we shift in the adapter we will delete v4.
    , trading_volume_data.trading_volume as trading_volume
    , unique_traders_data.unique_traders as unique_traders
    , fees + txn_fees as fees
    
    -- standardize metrics

    -- Market Metrics
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume

    -- Cash Flow Metrics
    , fees as perp_fees
    , txn_fees as chain_fees
    , trading_volume_data.trading_volume + trading_volume_data_v4.trading_volume as perp_volume
    , unique_traders_data.unique_traders + unique_traders_data_v4.unique_traders as perp_dau
    , txn_fees + fees as gross_protocol_revenue
    , case when date_spine.date >= '2025-03-25' then gross_protocol_revenue * 0.25 else 0 end as buybacks

    -- Other Metrics
    , price_data.token_turnover_circulating
    , price_data.token_turnover_fdv
from date_spine
left join trading_volume_data using(date)
left join unique_traders_data using(date)
left join trading_volume_data_v4 using(date)s
left join fees_data_v4 using(date)
left join chain_data_v4 using(date)
left join unique_traders_data_v4 using(date)
left join price_data using(date)
where date_spine.date < to_date(sysdate())

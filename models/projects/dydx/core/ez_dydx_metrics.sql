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
        where unique_traders < 1e5
    ), 
    dydx_supply_data AS (
        SELECT
            date
            , premine_unlocks_native
            , circulating_supply_native
        FROM {{ ref('fact_dydx_supply_data') }}
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
    , fees as trading_fees
    , txn_fees as txn_fees
    
    -- standardize metrics

    -- Market Metrics
    , price_data.price
    , price_data.market_cap
    , price_data.fdmc
    , price_data.token_volume

    -- Cash Flow Metrics
    , fees as perp_fees
    , txn_fees as chain_fees
    , coalesce(trading_volume_data.trading_volume, 0) + coalesce(trading_volume_data_v4.trading_volume, 0) as perp_volume
    , coalesce(unique_traders_data.unique_traders, 0) + coalesce(unique_traders_data_v4.unique_traders, 0) as perp_dau
    , coalesce(txn_fees, 0) + coalesce(fees, 0) as ecosystem_revenue
    , case when date_spine.date >= '2022-03-25' then ecosystem_revenue * 0.25 else 0 end as buybacks

    -- Supply Metrics
    , dydx_supply_data.circulating_supply_native - lag(dydx_supply_data.circulating_supply_native) over (order by date_spine.date) as net_supply_change_native
    , dydx_supply_data.premine_unlocks_native as premine_unlocks_native
    , dydx_supply_data.circulating_supply_native as circulating_supply_native

from date_spine
left join trading_volume_data on date_spine.date = trading_volume_data.date
left join unique_traders_data on date_spine.date = unique_traders_data.date
left join trading_volume_data_v4 on date_spine.date = trading_volume_data_v4.date
left join fees_data_v4 on date_spine.date = fees_data_v4.date
left join chain_data_v4 on date_spine.date = chain_data_v4.date
left join unique_traders_data_v4 on date_spine.date = unique_traders_data_v4.date
left join dydx_supply_data on date_spine.date = dydx_supply_data.date
left join price_data on date_spine.date = price_data.date
where date_spine.date < to_date(sysdate())

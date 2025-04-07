{{
    config(
        materialized="table",
        snowflake_warehouse="DYDX",
        database="dydx",
        schema="core",
        alias="ez_metrics_by_version",
    )
}}

with
    trading_volume_data_v4 as (
        select date, trading_volume
        from {{ ref("fact_dydx_v4_trading_volume") }}
    ),
    fees_data_v4 as (
        select date, fees
        from {{ ref("fact_dydx_v4_fees") }}
    ),
    chain_data_v4 as (
        select date, txn_fees
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
    )

select
    unique_traders_data.date as date
    , 'dydx' as app
    , 'DeFi' as category
    , 3 as version
    , trading_volume_data.trading_volume
    , unique_traders_data.unique_traders
    -- standardize metrics
    , trading_volume_data.trading_volume as perp_volume
    , unique_traders_data.unique_traders as perp_dau
from unique_traders_data
left join trading_volume_data on unique_traders_data.date = trading_volume_data.date
union all
select
    unique_traders_data_v4.date as date
    , 'dydx_v4' as app
    , 'DeFi' as category
    , 4 as version
    , trading_volume
    , unique_traders
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
from unique_traders_data_v4
left join trading_volume_data_v4 on unique_traders_data_v4.date = trading_volume_data_v4.date
left join fees_data_v4 on unique_traders_data_v4.date = fees_data_v4.date
left join chain_data_v4 on unique_traders_data_v4.date = chain_data_v4.date
where unique_traders_data_v4.date < to_date(sysdate())
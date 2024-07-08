{{
    config(
        materialized="table",
        snowflake_warehouse="DYDX",
        database="dydx",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
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
    unique_traders_data.date as date,
    'dydx' as app,
    'DeFi' as category,
    'starkware' as chain,
    trading_volume_data.trading_volume,
    unique_traders_data.unique_traders
from unique_traders_data
left join trading_volume_data on unique_traders_data.date = trading_volume_data.date
where unique_traders_data.date < to_date(sysdate())

{{
    config(
        materialized="table",
        snowflake_warehouse="HYPERLIQUID",
        database="hyperliquid",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_data as (
        select date, trading_volume, chain
        from {{ ref("fact_hyperliquid_trading_volume") }}
    ),
    unique_traders_data as (
        select date, unique_traders, chain
        from {{ ref("fact_hyperliquid_unique_traders") }}
    )
select
    date,
    'hyperliquid' as app,
    'DeFi' as category,
    chain,
    trading_volume,
    unique_traders
from unique_traders_data
left join trading_volume_data using(date, chain)
where date < to_date(sysdate())
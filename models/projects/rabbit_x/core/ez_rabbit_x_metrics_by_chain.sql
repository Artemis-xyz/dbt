{{
    config(
        materialized="table",
        snowflake_warehouse="RABBIT_X",
        database="rabbit_x",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}


with
    trading_volume_data as (
        select date, trading_volume, 'starknet' as chain
        from {{ ref("fact_rabbitx_trading_volume") }}
        where market_pair is null
    ),
    unique_traders_data as (
        select date, unique_traders, 'starknet' as chain
        from {{ ref("fact_rabbitx_unique_traders") }}
    )
select 
    date
    , 'rabbit-x' as app
    , 'DeFi' as category
    , chain
    , trading_volume
    , unique_traders
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
from trading_volume_data
left join unique_traders_data using(date, chain)
where date < to_date(sysdate())

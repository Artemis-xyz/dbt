{{
    config(
        materialized="table",
        snowflake_warehouse="GAINS_NETWORK",
        database="gains_network",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with 
    gains_data as (
        with agg as (
            select date, sum(trading_volume) as trading_volume, sum(unique_traders) as unique_traders, chain
            from {{ ref("fact_gains_trading_volume_unique_traders") }}
            where chain is not null
            group by date, chain
            UNION ALL
            SELECT date, sum(trading_volume) as trading_volume, sum(unique_traders) as unique_traders, chain
            from {{ ref("fact_gains_data_v8_v9") }}
            where chain is not null
            group by date, chain
        )
        select date, sum(trading_volume) as trading_volume, sum(unique_traders) as unique_traders, chain
        from agg
        group by date, chain
    )

select
    date
    , 'gains-network' as app
    , 'DeFi' as category
    , chain
    , trading_volume
    , unique_traders
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
from gains_data
where date < to_date(sysdate())

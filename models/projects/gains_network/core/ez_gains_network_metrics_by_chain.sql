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
        select date, trading_volume, unique_traders, chain
        from {{ ref("fact_gains_trading_volume_unique_traders") }}
        where chain is not null
    )

select
    date,
    'gains-network' as app,
    'DeFi' as category,
    chain,
    trading_volume,
    unique_traders
from gains_data
where date < to_date(sysdate())
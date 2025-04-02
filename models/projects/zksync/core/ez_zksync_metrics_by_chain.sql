{{
    config(
        materialized="table",
        snowflake_warehouse="ZKSYNC",
        database="zksync",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    bridge_volume_metrics as (
        select date, chain, inflow, outflow
        from {{ ref("fact_zksync_era_bridge_bridge_volume") }}
        where chain is not null
    ),
    price_data as (
        {{ get_coingecko_metrics('zksync') }}
    )
select
    b.date
    , 'zksync' as app
    , 'Bridge' as category
    , b.chain
    -- Old metrics needed for compatibility

    -- Standardized Metrics
    -- Bridge Metrics
    , inflow
    , outflow

    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    , token_volume
from bridge_volume_metrics as b
left join price_data on b.date = price_data.date
where b.date < to_date(sysdate())

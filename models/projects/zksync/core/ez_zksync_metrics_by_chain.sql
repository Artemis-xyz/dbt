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
    -- Old metrics needed for compatibility
    , 'zksync' as app
    , 'Bridge' as category
    , b.chain
    -- Standardized Metrics
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_volume
    -- Bridge Metrics
    , inflow
    , outflow
    , token_turnover_circulating
    , token_turnover_fdv
from bridge_volume_metrics as b
left join price_data on b.date = price_data.date
where b.date < to_date(sysdate())

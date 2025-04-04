{{
    config(
        materialized="table",
        snowflake_warehouse="BEDROCK",
        database="bedrock",
        schema="core",
        alias="ez_metrics",
    )
}}

with restaked_eth_metrics as (
    select
        date,
        chain,
        num_restaked_eth,
        amount_restaked_usd,
        num_restaked_eth_net_change,
        amount_restaked_usd_net_change
    from {{ ref('fact_bedrock_restaked_eth_count_with_usd_and_change') }}
)
, market_metrics as (
    {{get_coingecko_metrics('bedrock')}}
)
, date_spine as (
    select
        ds.date
    from {{ ref('dim_date_spine') }} ds
    where ds.date between (select min(date) from restaked_eth_metrics) and to_date(sysdate())
)
select
    date_spine.date,
    'bedrock' as app,
    'DeFi' as category,

    -- Old metrics needed for compatibility
    restaked_eth_metrics.num_restaked_eth,
    restaked_eth_metrics.amount_restaked_usd,
    restaked_eth_metrics.num_restaked_eth_net_change,
    restaked_eth_metrics.amount_restaked_usd_net_change

    -- Standardized Metrics
    , restaked_eth_metrics.num_restaked_eth as tvl_native
    , restaked_eth_metrics.amount_restaked_usd as tvl
    , restaked_eth_metrics.num_restaked_eth_net_change as tvl_native_net_change
    , restaked_eth_metrics.amount_restaked_usd_net_change as tvl_net_change

    -- Market Metrics
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv
from date_spine
left join restaked_eth_metrics using(date)
left join market_metrics using(date)
where date_spine.date < to_date(sysdate())
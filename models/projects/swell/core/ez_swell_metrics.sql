{{
    config(
        materialized="table",
        snowflake_warehouse="SWELL",
        database="swell",
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
    from {{ ref('fact_rsweth_restaked_eth_count_with_usd_and_change') }}
),
staked_eth_metrics as (
    select
        date,
        chain,
        num_staked_eth,
        amount_staked_usd,
        num_staked_eth_net_change,
        amount_staked_usd_net_change
    from {{ ref('fact_sweth_staked_eth_count_with_usd_and_change') }}
),
market_metrics as (
    {{get_coingecko_metrics('swell-network')}}
),
date_spine as (
    select
        ds.date
    from {{ ref('dim_date_spine') }} ds
    where ds.date between (select min(date) from restaked_eth_metrics) and to_date(sysdate())
)
select
    date_spine.date,
    'swell' as app,
    'DeFi' as category,

    --Old metrics needed for compatibility
    restaked_eth_metrics.num_restaked_eth,
    restaked_eth_metrics.amount_restaked_usd,
    restaked_eth_metrics.num_restaked_eth_net_change,
    restaked_eth_metrics.amount_restaked_usd_net_change,
    staked_eth_metrics.num_staked_eth,
    staked_eth_metrics.amount_staked_usd,
    staked_eth_metrics.num_staked_eth_net_change,
    staked_eth_metrics.amount_staked_usd_net_change

    --Standardized Metrics

    --Market Metrics 
    , market_metrics.price as price
    , market_metrics.token_volume as token_volume
    , market_metrics.market_cap as market_cap
    , market_metrics.fdmc as fdmc

    -- LRT Usage Metrics
    , restaked_eth_metrics.num_restaked_eth as lrt_tvl_native
    , restaked_eth_metrics.amount_restaked_usd as lrt_tvl
    , restaked_eth_metrics.num_restaked_eth_net_change as lrt_tvl_native_net_change
    , restaked_eth_metrics.amount_restaked_usd_net_change as lrt_tvl_net_change
    
    -- LST Usage Metrics
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.num_staked_eth_net_change as lst_tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change
    
    -- TVL Metrics
    , lst_tvl_native + lrt_tvl_native as tvl_native
    , lst_tvl + lrt_tvl as tvl
    , lst_tvl_native_net_change + lrt_tvl_native_net_change as tvl_native_net_change
    , lst_tvl_net_change + lrt_tvl_net_change as tvl_net_change

    -- Market Metrics
    , market_metrics.token_turnover_circulating as token_turnover_circulating
    , market_metrics.token_turnover_fdv as token_turnover_fdv


from date_spine
left join restaked_eth_metrics on date_spine.date = restaked_eth_metrics.date
left join staked_eth_metrics on date_spine.date = staked_eth_metrics.date and restaked_eth_metrics.chain = staked_eth_metrics.chain
left join market_metrics on date_spine.date = market_metrics.date
where date_spine.date < to_date(sysdate())

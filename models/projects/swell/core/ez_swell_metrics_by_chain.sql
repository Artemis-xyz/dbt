{{
    config(
        materialized="table",
        snowflake_warehouse="SWELL",
        database="swell",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    restaked_eth_metrics as (
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
    date_spine as (
        select
            ds.date,
        from {{ ref('dim_date_spine') }} ds
        where ds.date between (select min(date) from restaked_eth_metrics) and to_date(sysdate())
    )
select
    date_spine.date,
    restaked_eth_metrics.chain,
    'swell' as artemis_id,

    --Standardized Metrics

    -- LRT Usage Metrics
    , restaked_eth_metrics.num_restaked_eth as lrt_tvl_native
    , restaked_eth_metrics.amount_restaked_usd as lrt_tvl
    
    -- LST Usage Metrics
    , staked_eth_metrics.num_staked_eth as lst_tvl_native
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    
    -- TVL Metrics
    , coalesce(lst_tvl_native, 0) + coalesce(lrt_tvl_native, 0) as tvl_native
    , coalesce(lst_tvl, 0) + coalesce(lrt_tvl, 0) as tvl

from date_spine
left join restaked_eth_metrics using (date)
left join staked_eth_metrics using (date)
where date_spine.date < to_date(sysdate())

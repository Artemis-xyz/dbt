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
    )
select
    restaked_eth_metrics.date,
    'swell' as protocol,
    'DeFi' as category,
    restaked_eth_metrics.chain,
    restaked_eth_metrics.num_restaked_eth,
    restaked_eth_metrics.amount_restaked_usd,
    restaked_eth_metrics.num_restaked_eth_net_change,
    restaked_eth_metrics.amount_restaked_usd_net_change,
    staked_eth_metrics.num_staked_eth,
    staked_eth_metrics.amount_staked_usd,
    staked_eth_metrics.num_staked_eth_net_change,
    staked_eth_metrics.amount_staked_usd_net_change
from restaked_eth_metrics
left join staked_eth_metrics on restaked_eth_metrics.date = staked_eth_metrics.date and restaked_eth_metrics.chain = staked_eth_metrics.chain
where restaked_eth_metrics.date < to_date(sysdate())

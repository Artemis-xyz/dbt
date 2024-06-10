{{
    config(
        materialized="table",
        snowflake_warehouse="BSC_SM",
        database="bsc",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    staked_eth_metrics as (
        select
            date,
            chain,
            num_staked_eth,
            amount_staked_usd,
            num_staked_eth_net_change,
            amount_staked_usd_net_change
        from {{ ref('fact_binance_staked_eth_count_with_usd_and_change') }}
    )
select
    staked_eth_metrics.date,
    'binance' as app,
    'DeFi' as category,
    staked_eth_metrics.chain,
    staked_eth_metrics.num_staked_eth,
    staked_eth_metrics.amount_staked_usd,
    staked_eth_metrics.num_staked_eth_net_change,
    staked_eth_metrics.amount_staked_usd_net_change
from staked_eth_metrics
where staked_eth_metrics.date < to_date(sysdate())

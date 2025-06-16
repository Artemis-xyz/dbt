{{
    config(
        materialized="table",
        snowflake_warehouse="COINBASE",
        database="coinbase",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    staked_eth_metrics as (
        select
            date,
            sum(num_staked_eth) as num_staked_eth,
            sum(amount_staked_usd) as amount_staked_usd,
            sum(num_staked_eth_net_change) as num_staked_eth_net_change,
            sum(amount_staked_usd_net_change) as amount_staked_usd_net_change
        from {{ ref('fact_coinbase_staked_eth_count_with_usd_and_change') }}
        GROUP BY 1
    )
select
    staked_eth_metrics.date,
    'coinbase' as app,
    'DeFi' as category,
    staked_eth_metrics.num_staked_eth,
    staked_eth_metrics.amount_staked_usd,
    staked_eth_metrics.num_staked_eth_net_change,
    staked_eth_metrics.amount_staked_usd_net_change

    -- Standardized Metrics
    , staked_eth_metrics.num_staked_eth as tvl_native
    , staked_eth_metrics.amount_staked_usd as tvl
    , staked_eth_metrics.num_staked_eth_net_change as tvl_native_net_change
    , staked_eth_metrics.amount_staked_usd_net_change as tvl_net_change
from staked_eth_metrics
where staked_eth_metrics.date < to_date(sysdate())

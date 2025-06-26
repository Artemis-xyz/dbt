
{{
    config(
        materialized="table",
        snowflake_warehouse="FRAX",
        database="frax",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    staked_eth_metrics as (
        select
            date,
            'ethereum' as chain,
            num_staked_eth,
            amount_staked_usd,
            num_staked_eth_net_change,
            amount_staked_usd_net_change
        from {{ ref('fact_frax_staked_eth_count_with_USD_and_change') }}
    ),
    trading_volume_by_chain as (
        select
            date,
            chain,
            sum(trading_volume) as trading_volume,
            sum(trading_fees) as trading_fees,
            sum(unique_traders) as unique_traders,
            sum(gas_cost_native) as gas_cost_native,
            sum(gas_cost_usd) as gas_cost_usd
        from {{ ref("fact_fraxswap_ethereum_trading_vol_fees_traders_by_pool") }}
        group by date, chain
    ),
    tvl_by_chain as (
        select
            date,
            chain,
            sum(tvl) as tvl
        from {{ ref("fact_fraxswap_ethereum_tvl_by_pool") }}
        group by date, chain
    )
select
    coalesce(tvl_by_chain.date, staked_eth_metrics.date) as date,
    'frax' as app,
    'DeFi' as category,
    tvl_by_chain.chain,
    staked_eth_metrics.num_staked_eth,
    staked_eth_metrics.amount_staked_usd,
    staked_eth_metrics.num_staked_eth_net_change,
    staked_eth_metrics.amount_staked_usd_net_change,
    trading_volume_by_chain.trading_volume,
    trading_volume_by_chain.trading_fees,
    trading_volume_by_chain.unique_traders,
    trading_volume_by_chain.gas_cost_usd

    -- Standardized Metrics
    
    -- Usage/Sector Metrics
    , trading_volume_by_chain.unique_traders as spot_dau
    , trading_volume_by_chain.trading_volume as spot_volume
    , tvl_by_chain.tvl as spot_tvl
    , staked_eth_metrics.amount_staked_usd as lst_tvl
    , staked_eth_metrics.amount_staked_usd_net_change as lst_tvl_net_change
    , tvl_by_chain.tvl + staked_eth_metrics.amount_staked_usd as tvl
    

    -- Money Metrics
    , trading_volume_by_chain.trading_fees as spot_fees
    , trading_volume_by_chain.trading_fees as fees
    , trading_volume_by_chain.gas_cost_native
    , trading_volume_by_chain.gas_cost_usd as gas_cost
from tvl_by_chain 
left join trading_volume_by_chain using(date, chain)
left join staked_eth_metrics using(date, chain)
where staked_eth_metrics.date < to_date(sysdate())
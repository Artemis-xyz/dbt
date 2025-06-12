{{
    config(
        materialized="table",
        snowflake_warehouse="QUICKSWAP",
        database="quickswap",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_by_chain as (
        select
            date,
            chain,
            sum(trading_volume) as trading_volume,
            sum(trading_fees) as trading_fees,
            sum(unique_traders) as unique_traders,
            sum(gas_cost_native) as gas_cost_native,
            sum(gas_cost_usd) as gas_cost_usd
        from {{ ref("fact_quickswap_polygon_trading_vol_fees_traders_by_pool") }}
        group by date, chain
    ),
    tvl_by_chain as (
        select
            date,
            chain,
            sum(tvl) as tvl
        from {{ ref("fact_quickswap_polygon_tvl_by_pool") }}
        group by date, chain
    )
select
    tvl_by_chain.date
    , 'quickswap' as app
    , 'DeFi' as category
    , tvl_by_chain.chain
    , trading_volume_by_chain.trading_volume
    , trading_volume_by_chain.trading_fees
    , trading_volume_by_chain.unique_traders
    , trading_volume_by_chain.gas_cost_usd

    -- Standardized Metrics
    -- Usage/Sector Metrics
    , trading_volume_by_chain.unique_traders as spot_dau
    , trading_volume_by_chain.trading_volume as spot_volume
    , tvl_by_chain.tvl
    
    -- Money Metrics
    , trading_volume_by_chain.trading_fees as spot_fees
    , trading_volume_by_chain.trading_fees as fees
    -- We only track v2 where all fees go to LPs
    , trading_volume_by_chain.trading_fees as service_fee_allocation
    , trading_volume_by_chain.gas_cost_native
    , trading_volume_by_chain.gas_cost_usd as gas_cost
from tvl_by_chain
left join trading_volume_by_chain using(date, chain)
where tvl_by_chain.date < to_date(sysdate())
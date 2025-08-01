{{
    config(
        materialized="table",
        snowflake_warehouse="FRAX",
        database="frax",
        schema="core",
        alias="ez_metrics_by_pool",
    )
}}

with
    trading_volume_pool as (
        select *
        from {{ ref("fact_fraxswap_ethereum_trading_vol_fees_traders_by_pool") }}
    ),
    tvl_by_pool as (
       select *
       from {{ ref("fact_fraxswap_ethereum_tvl_by_pool") }}
    )
select
    tvl_by_pool.date
    , 'frax' as artemis_id
    , tvl_by_pool.chain
    , tvl_by_pool.version
    , tvl_by_pool.pool
    , tvl_by_pool.token_0
    , tvl_by_pool.token_0_symbol
    , tvl_by_pool.token_1
    , tvl_by_pool.token_1_symbol

    -- Standardized Metrics
    
    -- Usage/Sector Metrics
    , trading_volume_pool.unique_traders as spot_dau
    , trading_volume_pool.trading_volume as spot_volume
    , tvl_by_pool.tvl as spot_tvl
    , tvl_by_pool.tvl

    -- Money Metrics
    , trading_volume_pool.trading_fees as spot_fees
    , trading_volume_pool.trading_fees as fees

    -- Other Metrics
    , trading_volume_pool.gas_cost_native
    , trading_volume_pool.gas_cost_usd as gas_cost
from tvl_by_pool
left join trading_volume_pool using(date, chain, version, pool)
where tvl_by_pool.date < to_date(sysdate())

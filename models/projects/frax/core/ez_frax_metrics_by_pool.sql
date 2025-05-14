{{
    config(
        materialized="table",
        snowflake_warehouse="FRAX",
        table_format="iceberg",
        database="frax",
        schema="core",
        external_volume="ICEBERG_EXTERNAL_VOLUME_INTERNAL",
        alias="ez_metrics_by_pool",
        base_location_root="frax"
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
    tvl_by_pool.date::TIMESTAMP_NTZ(6) AS date,
    'frax' as app,
    'DeFi' as category,
    tvl_by_pool.chain,
    tvl_by_pool.version,
    tvl_by_pool.pool,
    tvl_by_pool.token_0,
    tvl_by_pool.token_0_symbol,
    tvl_by_pool.token_1,
    tvl_by_pool.token_1_symbol,
    tvl_by_pool.tvl,
    trading_volume_pool.trading_volume,
    trading_volume_pool.trading_fees,
    trading_volume_pool.unique_traders,
    trading_volume_pool.gas_cost_usd

    -- Standardized Metrics
    
    -- Usage/Sector Metrics
    , trading_volume_pool.unique_traders as spot_dau
    , trading_volume_pool.trading_volume as spot_volume
    , tvl_by_pool.tvl as spot_tvl

    -- Money Metrics
    , trading_volume_pool.trading_fees as spot_fees
    , trading_volume_pool.trading_fees as gross_protocol_revenue

    -- Other Metrics
    , trading_volume_pool.gas_cost_native
    , trading_volume_pool.gas_cost_usd as gas_cost
from tvl_by_pool
left join trading_volume_pool using(date, chain, version, pool)
where tvl_by_pool.date < to_date(sysdate())

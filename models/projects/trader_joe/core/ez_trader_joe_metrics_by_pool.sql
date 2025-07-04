{{
    config(
        materialized="table",
        snowflake_warehouse="TRADER_JOE",
        database="trader_joe",
        schema="core",
        alias="ez_metrics_by_pool",
    )
}}

with
    trading_volume_pool as (
       {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_trader_joe_arbitrum_trading_vol_fees_traders_by_pool"),
                    ref("fact_trader_joe_avalanche_trading_vol_fees_traders_by_pool"),
                ],
            )
        }}
    ),
    tvl_by_pool as (
       {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_trader_joe_arbitrum_tvl_by_pool"),
                    ref("fact_trader_joe_avalanche_tvl_by_pool"),
                ],
            )
        }}
    )
select
    tvl_by_pool.date,
    'trader_joe' as app,
    'DeFi' as category,
    tvl_by_pool.chain,
    tvl_by_pool.version,
    tvl_by_pool.pool,
    tvl_by_pool.token_0,
    tvl_by_pool.token_0_symbol,
    tvl_by_pool.token_1,
    tvl_by_pool.token_1_symbol,
    trading_volume_pool.trading_volume,
    trading_volume_pool.trading_fees,
    trading_volume_pool.unique_traders,
    trading_volume_pool.gas_cost_usd

    -- Standardized Metrics
    , trading_volume_pool.unique_traders as spot_dau
    , trading_volume_pool.trading_fees as spot_fees
    , trading_volume_pool.trading_volume as spot_volume
    , tvl_by_pool.tvl as tvl
    , trading_volume_pool.trading_fees as fees
    , trading_volume_pool.gas_cost_native
    , trading_volume_pool.gas_cost_usd as gas_cost

from tvl_by_pool
left join trading_volume_pool using(date, chain, version, pool)
where tvl_by_pool.date < to_date(sysdate())
{{
    config(
        materialized="table",
        snowflake_warehouse="CURVE",
        database="curve",
        schema="core",
        alias="ez_metrics_by_pool",
    )
}}

with
    trading_volume_pool as (
       {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_curve_arbitrum_daily_trading_metrics"),
                    ref("fact_curve_avalanche_daily_trading_metrics"),
                    ref("fact_curve_ethereum_daily_trading_metrics"),
                    ref("fact_curve_optimism_daily_trading_metrics"),
                    ref("fact_curve_polygon_daily_trading_metrics"),
                ],
            )
        }}
    ),
    tvl_by_pool as (
       {{
            dbt_utils.union_relations(
                relations=[
                    ref("fact_curve_arbitrum_tvl_by_pool"),
                    ref("fact_curve_avalanche_tvl_by_pool"),
                    ref("fact_curve_ethereum_tvl_by_pool"),
                    ref("fact_curve_optimism_tvl_by_pool"),
                    ref("fact_curve_polygon_tvl_by_pool"),
                ],
            )
        }}
    )

select
    tvl_by_pool.date
    , 'curve' as app
    , 'DeFi' as category
    , tvl_by_pool.chain
    , tvl_by_pool.pool
    , trading_volume_pool.trading_volume
    , trading_volume_pool.trading_fees
    , trading_volume_pool.unique_traders
    , trading_volume_pool.gas_cost_usd

    -- Standardized Metrics
    , trading_volume_pool.trading_volume as spot_volume
    , trading_volume_pool.unique_traders as spot_dau
    , tvl_by_pool.tvl

    -- Money Metrics
    , trading_volume_pool.trading_fees as spot_fees
    , trading_volume_pool.trading_fees as ecosystem_revenue
    , trading_volume_pool.trading_fees * 0.5 as staking_cash_flow
    , trading_volume_pool.trading_fees * 0.5 as service_cash_flow
    , trading_volume_pool.gas_cost_native
    , trading_volume_pool.gas_cost_usd as gas_cost
from tvl_by_pool
left join trading_volume_pool using(date, chain, pool)
where tvl_by_pool.date < to_date(sysdate())
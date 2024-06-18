{{
    config(
        materialized="table",
        snowflake_warehouse="CURVE_SM",
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
    tvl_by_pool.date,
    'curve' as app,
    'DeFi' as category,
    tvl_by_pool.chain,
    tvl_by_pool.version,
    tvl_by_pool.pool,
    tvl_by_pool.tvl,
    trading_volume_pool.trading_volume,
    trading_volume_pool.trading_fees,
    trading_volume_pool.unique_traders,
    trading_volume_pool.gas_cost_native,
    trading_volume_pool.gas_cost_usd
from tvl_by_pool
left join trading_volume_pool using(date, chain, version, pool)
where tvl_by_pool.date < to_date(sysdate())
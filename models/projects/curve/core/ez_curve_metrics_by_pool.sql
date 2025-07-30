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
    , 'curve' as artemis_id
    , tvl_by_pool.pool

    --Usage Data
    , trading_volume_pool.unique_traders as spot_dau
    , trading_volume_pool.unique_traders as dau
    , tvl_by_pool.tvl
    , trading_volume_pool.trading_volume as spot_volume
    , trading_volume_pool.gas_cost_native
    , trading_volume_pool.gas_cost_usd as gas_cost

    --Fee Data
    , trading_volume_pool.trading_fees as spot_fees
    , trading_volume_pool.trading_fees as fees

    --Fee Allocation
    , trading_volume_pool.trading_fees * 0.5 as staking_fee_allocation
    , trading_volume_pool.trading_fees * 0.5 as lp_fee_allocation

from tvl_by_pool
left join trading_volume_pool using(date, chain, pool)
where tvl_by_pool.date < to_date(sysdate())
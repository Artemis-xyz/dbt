{{
    config(
        materialized="table",
        snowflake_warehouse="CURVE_SM",
        database="curve",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with
    trading_volume_by_pool as (
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
    trading_volume_by_chain as (
        select
            trading_volume_by_pool.date,
            trading_volume_by_pool.chain,
            sum(trading_volume_by_pool.trading_volume) as trading_volume,
            sum(trading_volume_by_pool.trading_fees) as trading_fees,
            sum(trading_volume_by_pool.trading_revenue) as unique_traders,
            sum(trading_volume_by_pool.unique_traders) as unique_traders,
            sum(trading_volume_by_pool.gas_cost_native) as gas_cost_native,
            sum(trading_volume_by_pool.gas_cost_usd) as gas_cost_usd
        from trading_volume_by_pool
        group by trading_volume_by_pool.date, trading_volume_by_pool.chain
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
    ),
    tvl_by_chain as (
        select
            tvl_by_pool.date,
            tvl_by_pool.chain,
            sum(tvl_by_pool.tvl) as tvl
        from tvl_by_pool
        group by tvl_by_pool.date, tvl_by_pool.chain
    )
select
    tvl_by_chain.date,
    'curve' as app,
    'DeFi' as category,
    tvl_by_chain.chain,
    tvl_by_chain.tvl,
    trading_volume_by_chain.trading_volume,
    trading_volume_by_chain.trading_fees,
    trading_volume_by_chain.unique_traders,
    trading_volume_by_chain.gas_cost_native,
    trading_volume_by_chain.gas_cost_usd
from tvl_by_chain
left join trading_volume_by_chain using(date, chain)
where tvl_by_chain.date < to_date(sysdate())
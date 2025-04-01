{{
    config(
        materialized="table",
        snowflake_warehouse="CURVE",
        database="curve",
        schema="core",
        alias="ez_metrics",
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
    trading_volume as (
        select
            trading_volume_by_pool.date,
            sum(trading_volume_by_pool.trading_volume) as trading_volume,
            sum(trading_volume_by_pool.trading_fees) as trading_fees,
            sum(trading_volume_by_pool.trading_revenue) as trading_revenue,
            sum(trading_volume_by_pool.unique_traders) as unique_traders,
            sum(trading_volume_by_pool.gas_cost_native) as gas_cost_native,
            sum(trading_volume_by_pool.gas_cost_usd) as gas_cost_usd
        from trading_volume_by_pool
        group by trading_volume_by_pool.date
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
    tvl as (
        select
            tvl_by_pool.date,
            sum(tvl_by_pool.tvl) as tvl
        from tvl_by_pool
        group by tvl_by_pool.date
    )
    , market_metrics as (
        {{ get_coingecko_metrics('curve-dao-token')}}
    )
select
    tvl.date
    , 'curve' as app
    , 'DeFi' as category
    , tvl.tvl
    , trading_volume.trading_volume as spot_volume
    , trading_volume.unique_traders as spot_dau
    , trading_volume.trading_fees as spot_fees
    , trading_volume.trading_fees as gross_protocol_revenue
    , trading_volume.trading_fees * 0.5 as fee_sharing_token_cash_flow
    , trading_volume.trading_fees * 0.5 as service_cash_flow
    , trading_volume.gas_cost_native
    , trading_volume.gas_cost_usd as gas_cost
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv
    , market_metrics.token_volume
from tvl
left join trading_volume using(date)
left join market_metrics using(date)
where tvl.date < to_date(sysdate())
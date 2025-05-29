{{
    config(
        materialized="table",
        snowflake_warehouse="CURVE",
        database="curve",
        schema="core",
        alias="ez_metrics_by_chain",
    )
}}

with trading_volume_by_pool as (
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
)
, trading_volume_by_chain as (
    select
        trading_volume_by_pool.date as date,
        trading_volume_by_pool.chain as chain,
        sum(trading_volume_by_pool.trading_volume) as trading_volume,
        sum(trading_volume_by_pool.trading_fees) as trading_fees,
        sum(trading_volume_by_pool.trading_revenue) as trading_revenue,
        sum(trading_volume_by_pool.unique_traders) as unique_traders,
        sum(trading_volume_by_pool.gas_cost_native) as gas_cost_native,
        sum(trading_volume_by_pool.gas_cost_usd) as gas_cost_usd
    from trading_volume_by_pool
    group by trading_volume_by_pool.date, trading_volume_by_pool.chain
)
, tvl_by_pool as (
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
, tvl_by_chain as (
    select
        tvl_by_pool.date as date,
        tvl_by_pool.chain as chain,
        sum(tvl_by_pool.tvl) as tvl
    from tvl_by_pool
    group by tvl_by_pool.date, tvl_by_pool.chain
)
, token_incentives as (
    select
        date as date,
        'ethereum' as chain,
        sum(minted_usd) as token_incentives
    from {{ ref('fact_curve_token_incentives') }}
    group by 1
)

select
    tvl_by_chain.date
    , tvl_by_chain.chain
    , 'curve' as app
    , 'DeFi' as category
    

    --Old metrics needed for compatibility
    , trading_volume_by_chain.trading_volume
    , trading_volume_by_chain.trading_fees
    , trading_volume_by_chain.unique_traders

    -- Standardized Metrics

    -- Usage Metrics
    , trading_volume_by_chain.trading_volume as spot_volume
    , trading_volume_by_chain.unique_traders as spot_dau
    , tvl_by_chain.tvl

    -- Cashflow Metrics
    , trading_volume_by_chain.trading_fees as spot_fees
    , trading_volume_by_chain.trading_fees as ecosystem_revenue
    , trading_volume_by_chain.trading_fees * 0.5 as fee_sharing_token_cash_flow
    , trading_volume_by_chain.trading_fees * 0.5 as service_cash_flow
    , trading_volume_by_chain.gas_cost_native
    , trading_volume_by_chain.gas_cost_usd as gas_cost
    , coalesce(token_incentives.token_incentives, 0) as token_incentives

from tvl_by_chain
left join trading_volume_by_chain on tvl_by_chain.date = trading_volume_by_chain.date and tvl_by_chain.chain = trading_volume_by_chain.chain
left join token_incentives on tvl_by_chain.date = token_incentives.date and tvl_by_chain.chain = token_incentives.chain
where tvl_by_chain.date < to_date(sysdate())
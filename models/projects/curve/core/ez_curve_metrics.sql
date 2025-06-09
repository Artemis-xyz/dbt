{{
    config(
        materialized="table",
        snowflake_warehouse="CURVE",
        database="curve",
        schema="core",
        alias="ez_metrics",
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
, trading_volume as (
    select
        trading_volume_by_pool.date,
        sum(trading_volume_by_pool.trading_volume) as trading_volume,
        sum(trading_volume_by_pool.trading_fees) as trading_fees,
        sum(trading_volume_by_pool.trading_revenue) as trading_revenue,
        sum(trading_volume_by_pool.gas_cost_native) as gas_cost_native,
        sum(trading_volume_by_pool.gas_cost_usd) as gas_cost_usd
    from trading_volume_by_pool
    group by trading_volume_by_pool.date
)
, ez_dex_swaps as (
    SELECT
        block_timestamp::date as date,
        count(distinct sender) as unique_traders,
        count(*) as spot_txns
    FROM
        {{ ref('ez_curve_dex_swaps') }}
    group by 1
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
, tvl as (
    select
        tvl_by_pool.date,
        sum(tvl_by_pool.tvl) as tvl
    from tvl_by_pool
    group by tvl_by_pool.date
)
, token_incentives as (
    select
        date,
        sum(minted_amount) as token_incentives_native,
        sum(minted_usd) as token_incentives
    from {{ ref('fact_curve_token_incentives') }}
    group by 1
)
, date_spine as (
    select
        date
    from {{ ref('dim_date_spine') }}
    where date between (select min(date) from tvl) and to_date(sysdate())
)
, market_metrics as (
    {{ get_coingecko_metrics('curve-dao-token')}}
)

select
    date_spine.date
    , 'curve' as app
    , 'DeFi' as category

    -- Standardized Metrics

    -- Market Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Metrics
    , ez_dex_swaps.unique_traders as spot_dau
    , ez_dex_swaps.spot_txns
    , trading_volume.trading_volume as spot_volume
    , tvl.tvl

    -- Cashflow Metrics
    , trading_volume.trading_fees as spot_fees
    , trading_volume.trading_fees as ecosystem_revenue
    , trading_volume.trading_fees * 0.5 as fee_sharing_token_cash_flow
    , trading_volume.trading_fees * 0.5 as service_cash_flow
    , token_incentives.token_incentives_native
    , trading_volume.gas_cost_native
    , trading_volume.gas_cost_usd as gas_cost

    -- Financial Statement Metrics
    , trading_volume.trading_fees as fees
    , trading_volume.trading_fees * 0.5 as revenue
    , token_incentives.token_incentives as token_incentives
    , revenue - token_incentives as earnings
    
    -- Other Metrics
    , market_metrics.token_turnover_circulating
    , market_metrics.token_turnover_fdv

from date_spine
left join market_metrics using(date)
left join ez_dex_swaps using(date)
left join trading_volume using(date)
left join tvl using(date)
left join token_incentives using(date)
where date_spine.date < to_date(sysdate())
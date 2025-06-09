{{
    config(
        materialized="table",
        snowflake_warehouse="QUICKSWAP",
        database="quickswap",
        schema="core",
        alias="ez_metrics",
    )
}}

with
    dex_swaps as (
        select
            block_timestamp::date as date,
            count(distinct sender) as unique_traders,
            count(*) as number_of_swaps,
            sum(trading_volume) as trading_volume,
            sum(trading_fees) as trading_fees,
            sum(gas_cost_native) as gas_cost_native
        from {{ ref("ez_quickswap_dex_swaps") }}
        group by 1
    )
    , tvl as (
        select
            date,
            sum(tvl) as tvl
        from {{ ref("fact_quickswap_polygon_tvl_by_pool") }}
        group by date
    )
    , market_metrics as (
        {{ get_coingecko_metrics('quickswap') }}
    )
SELECT
    dex_swaps.date
    , 'quickswap' as app
    , 'DeFi' as category
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume
    , dex_swaps.unique_traders as spot_dau
    , dex_swaps.number_of_swaps as spot_txns
    , dex_swaps.trading_volume as spot_volume
    , tvl.tvl
    , dex_swaps.trading_fees as spot_fees
    , dex_swaps.trading_fees as gross_fees
    , 0 as revenue
    , 0 - token_incentives.token_incentives as earnings
    -- We only track v2 where all fees go to LPs
    , dex_swaps.trading_fees as service_cash_flow
from dex_swaps
left join tvl using(date)
left join market_metrics using(date)
where dex_swaps.date < to_date(sysdate())
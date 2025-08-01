{{
    config(
        materialized="incremental",
        snowflake_warehouse="ANALYTICS_XL",
        database="quickswap",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

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
    , token_incentives as (
        select
            day as date,
            sum(TOTAL_DAILY_TOKEN_INCENTIVE) as token_incentives
        from {{ ref("fact_quickswap_polygon_token_incentives") }}
        group by 1
    )
SELECT
    dex_swaps.date
    , 'quickswap' as artemis_id

    -- Standardized Metrics
    -- Market Data Metrics
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_volume

    -- Usage Metrics
    , dex_swaps.unique_traders as spot_dau
    , dex_swaps.number_of_swaps as spot_txns
    , dex_swaps.trading_volume as spot_volume
    , tvl.tvl

    -- Fee Metrics
    , dex_swaps.trading_fees as spot_fees
    , dex_swaps.trading_fees as fees
    , dex_swaps.trading_fees as lp_fee_allocation

    -- Token Incentives
    , coalesce(token_incentives.token_incentives, 0) as token_incentives

    -- Timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from dex_swaps
left join tvl using(date)
left join market_metrics using(date)
left join token_incentives using(date)
where true
{{ ez_metrics_incremental('dex_swaps.date', backfill_date) }}
and dex_swaps.date < to_date(sysdate())
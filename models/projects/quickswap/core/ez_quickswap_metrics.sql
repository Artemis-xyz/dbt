{{
    config(
        materialized="incremental",
        snowflake_warehouse="QUICKSWAP",
        database="quickswap",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
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
        {{ ez_metrics_incremental('block_timestamp::date', backfill_date) }}
        group by 1
    )
    , tvl as (
        select
            date,
            sum(tvl) as tvl
        from {{ ref("fact_quickswap_polygon_tvl_by_pool") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
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
        {{ ez_metrics_incremental('day', backfill_date) }}
        group by 1
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
    , dex_swaps.trading_fees as fees
    
    -- We only track v2 where all fees go to LPs
    , dex_swaps.trading_fees as service_fee_allocation
    , coalesce(token_incentives.token_incentives, 0) as token_incentives

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from dex_swaps
left join tvl using(date)
left join market_metrics using(date)
left join token_incentives using(date)
{{ ez_metrics_incremental('dex_swaps.date', backfill_date) }}
and dex_swaps.date < to_date(sysdate())
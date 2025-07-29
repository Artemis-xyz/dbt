{{
    config(
        materialized = 'incremental',
        snowflake_warehouse = 'BLUR',
        database = 'blur',
        schema = 'core',
        alias = 'ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    blur_fees as (
        select *
        from {{ ref("fact_blur_fees") }}
    )
    , blur_daus as (
        select *
        from {{ ref("fact_blur_daus") }}
    )
    , blur_daily_txns as (
        select *
        from {{ ref("fact_blur_daily_txns") }}
    )
    , blur_daily_supply as (
        select *
        from {{ ref("fact_blur_daily_supply") }}
    )
    , market_data as (
        {{ get_coingecko_metrics("blur") }}
    )

select
    blur_daus.date
    , 'blur' AS artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_data.price
    , market_data.market_cap
    , market_data.fdmc
    , market_data.token_volume 

    -- Usage Data
    , blur_daus.dau AS nft_dau
    , blur_daus.dau
    , blur_daily_txns.daily_txns AS nft_txns
    , blur_daily_txns.daily_txns AS txns

    -- Fee Data
    , blur_fees.fees AS fees
    , blur_fees.fees AS nft_fees
    , blur_fees.fees AS other_fee_allocation

    -- Supply Data
    , COALESCE(blur_daily_supply.premine_unlocks_native, 0) AS premine_unlocks_native
    , blur_daily_supply.circulating_supply_native

    -- Turnover Metrics
    , market_data.token_turnover_circulating
    , market_data.token_turnover_fdv

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from blur_daus
left join blur_daily_txns using (date)
left join blur_fees using (date)
left join blur_daily_supply using (date)
left join market_data using (date)
where true
{{ ez_metrics_incremental('blur_daus.date', backfill_date) }}
and blur_daus.date < to_date(sysdate())
order by blur_daus.date desc

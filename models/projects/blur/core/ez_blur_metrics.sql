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
        merge_exclude_columns=['created_on'],
        full_refresh=false
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

with
    blur_fees as (
        select *
        from {{ ref("fact_blur_fees") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    , blur_daus as (
        select *
        from {{ ref("fact_blur_daus") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    , blur_daily_txns as (
        select *
        from {{ ref("fact_blur_daily_txns") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    , blur_daily_supply as (
        select *
        from {{ ref("fact_blur_daily_supply") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
    )
    , market_data as (
        {{ get_coingecko_metrics("blur") }}
    )

select
    blur_daus.date
    , coalesce(blur_daus.dau, 0) as dau
    , coalesce(blur_daily_txns.daily_txns, 0) as txns
    , coalesce(blur_fees.fees, 0) as fees
    -- Token Metrics
    , coalesce(market_data.price, 0) as price
    , coalesce(market_data.market_cap, 0) as market_cap
    , coalesce(market_data.fdmc, 0) as fdmc
    , coalesce(market_data.token_volume, 0) as token_volume 
    -- NFT Metrics
    , coalesce(blur_daus.dau, 0) as nft_dau
    , coalesce(blur_daily_txns.daily_txns, 0) as nft_txns
    , coalesce(blur_fees.fees, 0) as nft_fees
    -- Cash Flow Metrics
    , coalesce(blur_fees.fees, 0) as ecosystem_revenue
    , coalesce(blur_fees.fees, 0) as service_fee_allocation
    -- Supply Metrics
    , coalesce(blur_daily_supply.premine_unlocks_native, 0) as premine_unlocks_native
    , coalesce(blur_daily_supply.circulating_supply_native, 0) as circulating_supply_native
    , coalesce(blur_daily_supply.net_supply_change_native, 0) as net_supply_change_native
    -- Turnover Metrics
    , coalesce(market_data.token_turnover_circulating, 0) as token_turnover_circulating
    , coalesce(market_data.token_turnover_fdv, 0) as token_turnover_fdv
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from blur_daus
left join blur_daily_txns using (date)
left join blur_fees using (date)
left join blur_daily_supply using (date)
left join market_data using (date)
{{ ez_metrics_incremental('blur_daus.date', backfill_date) }}
order by date desc

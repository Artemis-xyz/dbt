{{
    config(
        materialized="incremental",
        snowflake_warehouse="KTX_FINANCE",
        database="ktx_finance",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with
    trading_volume_data as (
        select date, sum(trading_volume) as trading_volume
        from {{ ref("fact_ktx_finance_trading_volume") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        group by date
    )
    , unique_traders_data as (
        select date, sum(unique_traders) as unique_traders
        from {{ ref("fact_ktx_finance_unique_traders") }}
        {{ ez_metrics_incremental('date', backfill_date) }}
        group by date
    )
    , price as ({{ get_coingecko_metrics("ktx-finance") }})
select
    date
    , 'ktx_finance' as app
    , 'DeFi' as category
    -- standardize metrics
    , trading_volume as perp_volume
    , unique_traders as perp_dau
    -- Market Data
    , price
    , market_cap
    , fdmc
    , token_turnover_circulating
    , token_turnover_fdv
    , token_volume
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from trading_volume_data
left join unique_traders_data using(date)
left join price using(date)
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())

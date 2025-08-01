{{
    config(
        materialized="incremental",
        snowflake_warehouse="RONIN",
        database="ronin",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with 
    ronin_dex_volumes as (
        select date, daily_volume as dex_volumes, daily_volume_adjusted as adjusted_dex_volumes
        from {{ ref("fact_ronin_daily_dex_volumes") }}
    ),
    price_data as ({{ get_coingecko_metrics("ronin") }})
select
    ronin_dex_volumes.date
    , dex_volumes
    , adjusted_dex_volumes
    -- Standardized Metrics
    -- Market Data Metrics
    , price
    , market_cap
    , fdmc
    , token_volume
    , token_turnover_circulating
    -- Chain Usage Metrics
    , dex_volumes AS chain_spot_volume

    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from ronin_dex_volumes   
left join price_data on ronin_dex_volumes.date = price_data.date
where true
{{ ez_metrics_incremental('ronin_dex_volumes.date', backfill_date) }}
and ronin_dex_volumes.date < to_date(sysdate())

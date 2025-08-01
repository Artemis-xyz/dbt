{{
    config(
        materialized="incremental",
        snowflake_warehouse="LIFINITY",
        database="lifinity",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with lifinity_dex_volumes as (
    select date, daily_volume as dex_volumes
    from {{ ref("fact_lifinity_dex_volumes") }}
)
, lifinity_market_data as (
    {{ get_coingecko_metrics('lifinity') }}
)

select
    date
    , dex_volumes
    -- Standardized Metrics
    , dex_volumes as spot_volume
    -- Market Metrics
    , lmd.price
    , lmd.market_cap
    , lmd.fdmc
    , lmd.token_turnover_circulating
    , lmd.token_turnover_fdv
    , lmd.token_volume
    -- timestamp columns
    , sysdate() as created_on
    , sysdate() as modified_on
from lifinity_dex_volumes   
left join lifinity_market_data lmd using (date)
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())

{{
    config(
        materialized="incremental",
        snowflake_warehouse="NOVA",
        database="nova",
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

with nova_dex_volumes as (
    select date, daily_volume as dex_volumes
    from {{ ref("fact_nova_daily_dex_volumes") }}
    {{ ez_metrics_incremental('date', backfill_date) }}
)
, nova_market_data as (
    {{ get_coingecko_metrics('novadex') }}
)
select
    date
    , dex_volumes
    -- Standardized Metrics
    , dex_volumes as spot_volume
    -- Market Metrics
    , nmd.price
    , nmd.market_cap
    , nmd.fdmc
    , nmd.token_turnover_circulating
    , nmd.token_turnover_fdv
    , nmd.token_volume
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from nova_dex_volumes   
left join nova_market_data nmd using (date)
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())

{{
    config(
        materialized="incremental",
        snowflake_warehouse="DEBRIDGE",
        database="debridge",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with bridge_volume_fees as (
    select 
        date
        , coalesce(bridge_volume, 0) as bridge_volume
        , coalesce(ecosystem_revenue, 0) as ecosystem_revenue
        , coalesce(bridge_txns, 0) as bridge_txns
        , coalesce(bridge_txns, 0) as txns
        , coalesce(bridge_dau, 0) as bridge_dau
        , coalesce(ecosystem_revenue, 0) as fees
    from {{ ref("fact_debridge_fundamental_metrics") }}
)

, market_metrics as ({{ get_coingecko_metrics("debridge") }})

select
    bridge_volume_fees.date
    , 'debridge' as artemis_id

    -- Standardized Metrics

    -- Market Data
    , market_metrics.price
    , market_metrics.market_cap
    , market_metrics.fdmc
    , market_metrics.token_turnover_circulating

    -- Usage Data
    , bridge_volume_fees.bridge_dau as bridge_dau
    , bridge_volume_fees.bridge_dau as dau
    , bridge_volume_fees.bridge_txns as bridge_txns
    , bridge_volume_fees.bridge_txns as txns
    , bridge_volume

    -- Fee Data
    , bridge_volume_fees.fees as bridge_fees
    , bridge_volume_fees.fees as fees
    
    -- Token Turnover/Other Data
    , market_metrics.token_turnover_fdv as token_turnover_fdv
    , market_metrics.token_volume as token_volume

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on

from bridge_volume_fees
left join market_metrics using (date)
where true
{{ ez_metrics_incremental('bridge_volume_fees.date', backfill_date) }}
and bridge_volume_fees.date < to_date(sysdate())

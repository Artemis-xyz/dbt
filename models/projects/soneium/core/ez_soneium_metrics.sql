{{
    config(
        materialized="incremental",
        snowflake_warehouse="SONEIUM",
        database="soneium",
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

select
    date
    , txns
    , daa as dau
    , fees_native
    , fees
    -- Standardized metrics
    -- Chain Usage Metrics
    , dau as chain_dau
    , txns as chain_txns
    -- Cashflow metrics
    , fees_native AS ecosystem_revenue_native
    , fees AS ecosystem_revenue
    -- timestamp columns    
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from {{ ref("fact_soneium_fundamental_metrics") }}
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())

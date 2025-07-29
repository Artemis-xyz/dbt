{{
    config(
        materialized="incremental",
        snowflake_warehouse="FREQUENCY",
        database="frequency",
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

with
    fundamental_data as (
        select
            date, 
            txns,
            daa, 
            fees_native
        from {{ ref("fact_frequency_fundamental_metrics") }}
    )
select
    date
    , 'frequency' as artemis_id

    -- Standardized Metrics
    -- Usage Metrics
    , txns as chain_txns
    , txns
    , daa as chain_dau
    , daa as dau
    -- Cash Flow Metrics
    , coalesce(fees_native, 0) as fees_native
    , fees_native as ecosystem_revenue_native
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from fundamental_data
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and fundamental_data.date < to_date(sysdate())

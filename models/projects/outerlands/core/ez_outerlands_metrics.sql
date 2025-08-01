{{
    config(
        materialized="incremental",
        database="outerlands",
        snowflake_warehouse="OUTERLANDS",
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

SELECT date, cumulative_index_value as price,
    -- timestamp columns
    sysdate() as created_on,
    sysdate() as modified_on
FROM {{ ref('fact_outerlands_fundamental_index_performance') }}
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
{{
    config(
        materialized="incremental",
        snowflake_warehouse="CYPHER",
        database="CYPHER",
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

select
    date::date as date,
    sum(transfer_volume) as transfer_volume,
    -- timestamp columns
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from {{ ref("fact_cypher_transfers") }}
where true
{{ ez_metrics_incremental('date::date', backfill_date) }}
and date::date < to_date(sysdate())
group by 1
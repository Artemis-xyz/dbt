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
        merge_exclude_columns=["created_on"],
        full_refresh=false
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

select
    date::date as date,
    sum(transfer_volume) as transfer_volume,
    -- timestamp columns
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from {{ ref("fact_cypher_transfers") }}
{{ ez_metrics_incremental('date::date', backfill_date) }}
group by 1
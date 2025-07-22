{{
    config(
        materialized="incremental",
        snowflake_warehouse="COINDESK_20",
        database="COINDESK_20",
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
    date
    , price
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from {{ ref("fact_coindesk20_price") }}
{{ ez_metrics_incremental('date', backfill_date) }}

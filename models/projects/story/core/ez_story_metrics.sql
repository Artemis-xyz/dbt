{{
    config(
        materialized="incremental",
        snowflake_warehouse="STORY",
        database="story",
        schema="core",
        alias="ez_metrics",
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
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

    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from {{ ref("fact_story_fundamental_metrics") }}
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())

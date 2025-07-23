{{
    config(
        materialized='incremental',
        snowflake_warehouse='ECONIA',
        database='ECONIA',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with econia_tvl as (
    {{ get_defillama_protocol_tvl('econia') }}
)

select
    econia_tvl.date
    -- Standardized Metrics
    , econia_tvl.tvl
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from econia_tvl
{{ ez_metrics_incremental('econia_tvl.date', backfill_date) }}
    and econia_tvl.date < to_date(sysdate())
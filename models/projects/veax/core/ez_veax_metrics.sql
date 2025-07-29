{{
    config(
        materialized='incremental',
        snowflake_warehouse='VEAX',
        database='VEAX',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_update_columns=var('backfill_columns', []),
        merge_exclude_columns=['created_on'] if not var('backfill_columns', []) else none,
        full_refresh=false,
        tags=['ez_metrics']
    )
}}

{% set backfill_date = var("backfill_date", None) %}

with veax_tvl as (
    {{ get_defillama_protocol_tvl('veax') }}
)

select
    veax_tvl.date
    , 'Defillama' as source

    -- Standardized Metrics
    , veax_tvl.tvl    
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from veax_tvl
where true
{{ ez_metrics_incremental('veax_tvl.date', backfill_date) }}
and veax_tvl.date < to_date(sysdate())
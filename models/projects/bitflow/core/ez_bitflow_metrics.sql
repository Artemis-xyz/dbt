{{
    config(
        materialized='incremental',
        snowflake_warehouse='BITFLOW',
        database='BITFLOW',
        schema='core',
        alias='ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_exclude_columns=['created_on'],
        full_refresh=false
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

with bitflow_tvl as (
    {{ get_defillama_protocol_tvl('bitflow') }}
)

select
    bitflow_tvl.date
    , 'Defillama' as source
    -- Standardized Metrics
    , bitflow_tvl.tvl
    -- timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
from bitflow_tvl
{{ ez_metrics_incremental('bitflow_tvl.date', backfill_date) }}
    and bitflow_tvl.date < to_date(sysdate())

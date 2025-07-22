{{
    config(
        materialized = 'incremental',
        database = 'blackrock',
        schema = 'core',
        snowflake_warehouse = 'BLACKROCK',
        alias = 'ez_metrics',
        incremental_strategy='merge',
        unique_key='date',
        on_schema_change='append_new_columns',
        merge_exclude_columns=['created_on'],
        full_refresh=false
    )
}}

-- NOTE: When running a backfill, add merge_update_columns=[<columns>] to the config and set the backfill date below

{% set backfill_date = None %}

SELECT
    date,
    sum(tokenized_mcap_change) as tokenized_mcap_change,
    sum(tokenized_mcap) as tokenized_mcap,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM {{ ref('ez_blackrock_metrics_by_chain') }}
{{ ez_metrics_incremental('date', backfill_date) }}
GROUP BY 1
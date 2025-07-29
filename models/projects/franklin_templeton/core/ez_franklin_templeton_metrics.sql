{{
    config(
        materialized = 'incremental',
        database = 'franklin_templeton',
        schema = 'core',
        snowflake_warehouse = 'FRANKLIN_TEMPLETON',
        alias = 'ez_metrics',
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] if not var("backfill_columns", []) else none,
        full_refresh=var("full_refresh", false),
        tags=["ez_metrics"],
    )
}}

{% set backfill_date = var("backfill_date", None) %}

SELECT
    date
    , 'franklin_templeton' as artemis_id

    -- Standardized Metrics
    , sum(tokenized_mcap_change) as tokenized_mcap_change
    , sum(tokenized_mcap) as tokenized_mcap

    -- Timestamp columns
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as created_on
    , TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()) as modified_on
FROM {{ ref('ez_franklin_templeton_metrics_by_chain') }}
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
GROUP BY 1
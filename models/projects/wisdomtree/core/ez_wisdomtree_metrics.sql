{{
    config(
        materialized = 'incremental',
        database = 'wisdomtree',
        schema = 'core',
        snowflake_warehouse = 'WISDOMTREE',
        alias = 'ez_metrics',
        incremental_strategy="merge",
        unique_key="date",
        on_schema_change="append_new_columns",
        merge_update_columns=var("backfill_columns", []),
        merge_exclude_columns=["created_on"] | reject('in', var("backfill_columns", [])) | list,
        full_refresh=false,
        tags=["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", None) %}

SELECT
    date,
    sum(tokenized_mcap_change) as tokenized_mcap_change,
    sum(tokenized_mcap) as tokenized_mcap
FROM {{ ref('ez_wisdomtree_metrics_by_chain') }}
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
GROUP BY 1
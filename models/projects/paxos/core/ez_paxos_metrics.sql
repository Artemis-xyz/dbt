{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ["date"],
        merge_update_columns = "ALL",
        merge_exclude_columns = ["created_on"],
        on_schema_change = "sync_all_columns",
        full_refresh = false,
        database = 'paxos',
        schema = 'core',
        snowflake_warehouse = 'PAXOS',
        alias = 'ez_metrics',
        tags = ["ez_metrics"]
    )
}}

{% set backfill_date = var("backfill_date", none) %}

SELECT
    date,
    sum(tokenized_mcap_change) as tokenized_mcap_change,
    sum(tokenized_mcap) as tokenized_mcap,
    to_timestamp_ntz(current_timestamp()) as created_on,
    to_timestamp_ntz(current_timestamp()) as modified_on
FROM {{ ref('ez_paxos_metrics_by_chain') }}
where true
{{ ez_metrics_incremental('date', backfill_date) }}
and date < to_date(sysdate())
GROUP BY 1
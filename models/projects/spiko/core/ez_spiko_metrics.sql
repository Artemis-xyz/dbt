{{
    config(
        materialized = 'table',
        database = 'spiko',
        schema = 'core',
        snowflake_warehouse = 'SPIKO',
        alias = 'ez_metrics'
    )
}}

SELECT
    date,
    sum(tokenized_mcap_change) as tokenized_mcap_change,
    sum(tokenized_mcap) as tokenized_mcap
FROM {{ ref('ez_spiko_metrics_by_chain') }}
GROUP BY 1
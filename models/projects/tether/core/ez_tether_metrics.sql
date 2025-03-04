{{
    config(
        materialized = 'table',
        database = 'tether',
        schema = 'core',
        snowflake_warehouse = 'TETHER',
        alias = 'ez_metrics'
    )
}}

SELECT
    date,
    sum(tokenized_mcap_change) as tokenized_mcap_change,
    sum(tokenized_mcap) as tokenized_mcap
FROM {{ ref('ez_tether_metrics_by_chain') }}
GROUP BY 1
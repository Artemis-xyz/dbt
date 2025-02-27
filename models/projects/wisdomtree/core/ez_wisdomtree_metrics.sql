{{
    config(
        materialized = 'table',
        database = 'wisdomtree',
        schema = 'core',
        snowflake_warehouse = 'WISDOMTREE',
        alias = 'ez_metrics'
    )
}}

SELECT
    date,
    sum(tokenized_mcap_change) as tokenized_mcap_change,
    sum(tokenized_mcap) as tokenized_mcap
FROM {{ ref('ez_wisdomtree_metrics_by_chain') }}
GROUP BY 1
{{
    config(
        materialized = 'table',
        database = 'franklin_templeton',
        schema = 'core',
        snowflake_warehouse = 'FRANKLIN_TEMPLETON',
        alias = 'ez_metrics'
    )
}}

SELECT
    date,
    sum(tokenized_mcap_change) as tokenized_mcap_change,
    sum(tokenized_mcap) as tokenized_mcap
FROM {{ ref('ez_franklin_templeton_metrics_by_chain') }}
GROUP BY 1
{{
    config(
        materialized = 'table',
        database = 'blackrock',
        schema = 'core',
        snowflake_warehouse = 'BLACKROCK',
        alias = 'ez_metrics'
    )
}}

SELECT
    date,
    sum(tokenized_mcap_change) as tokenized_mcap_change,
    sum(tokenized_mcap) as tokenized_mcap
FROM {{ ref('ez_blackrock_metrics_by_chain') }}
GROUP BY 1
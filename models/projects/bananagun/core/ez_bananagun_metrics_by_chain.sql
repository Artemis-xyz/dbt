{{
    config(
        materialized='table',
        snowflake_warehouse='BANANAGUN',
        database='BANANAGUN',
        schema='core',
        alias='ez_metrics_by_chain'
    )
}}

SELECT *
FROM {{ ref('fact_bananagun_all_metrics') }}


ORDER BY trade_date DESC
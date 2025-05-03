{{
    config(
        materialized='table',
        unique_key='date',
        snowflake_warehouse='PYTH',
    )
}}

SELECT
    date,
    tvl as dfl_tvs
FROM {{ source('MANUAL_STATIC_TABLES', 'pyth_tvs') }}
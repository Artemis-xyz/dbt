{{
    config(
        materialized='table',
        snowflake_warehouse='AERODROME',
        database='aerodrome',
        schema='raw',
        alias='fact_aerodrome_tvl'
    )
}}

with combined_tvl as (
    SELECT * FROM {{ ref('fact_aerodrome_v1_tvl') }}
    UNION ALL
    SELECT * FROM {{ ref('fact_aerodrome_v2_tvl') }}
)
SELECT * FROM combined_tvl
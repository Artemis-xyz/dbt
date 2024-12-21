{{
    config(
        materialized='table',
        snowflake_warehouse='LIQUITY',
        database='LIQUITY',
        schema='raw',
        alias='fact_liquity_outstanding_supply'
    )
}}

SELECT * FROM {{ ref('fact_liquity_outstanding_supply_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('fact_liquity_outstanding_supply_polygon') }}
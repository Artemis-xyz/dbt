{{
    config(
        materialized='view',
        snowflake_warehouse='jito',
        database='jito',
        schema='core',
        alias='ez_metrics'
    )
}}

SELECT 
    day
    , fees
    , revenue
    , supply_side_fees
    , txns
    , dau
FROM {{ ref('fact_jito_dau_txns_fees') }}

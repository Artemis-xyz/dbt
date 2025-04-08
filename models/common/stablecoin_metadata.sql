
{{
    config(
        snowflake_warehouse="COMMON",
        database="common",
        schema="core",
        materialized='view'
    )
}}

SELECT 
    *
FROM {{ source("STABLECOINS", "stablecoin_metadata") }}

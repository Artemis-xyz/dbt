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
FROM PC_DBT_DB.PROD.ALL_CHAINS_GAS_DAU_TXNS_BY_CONTRACT_V2

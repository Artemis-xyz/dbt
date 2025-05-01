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
FROM {{ ref("all_chains_gas_dau_txns_by_contract_v2") }}

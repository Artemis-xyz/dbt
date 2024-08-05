{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_bsc_ecosystem_incentives",
    )
}}

{{ aave_v3_ecosystem_incentives('bsc', '0xC206C2764A9dBF27d599613b8F9A63ACd1160ab4', 'AAVE V3')}}
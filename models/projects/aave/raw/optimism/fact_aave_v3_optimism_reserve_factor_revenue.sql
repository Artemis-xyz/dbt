{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_optimism_reserve_factor_revenue",
    )
}}

{{ aave_v3_reserve_factor_revenue('optimism', '0x794a61358D6845594F94dc1DB02A252b5b4814aD', 'AAVE V3')}}
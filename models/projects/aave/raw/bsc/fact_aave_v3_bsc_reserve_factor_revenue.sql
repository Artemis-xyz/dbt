{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_bsc_reserve_factor_revenue",
    )
}}

{{ aave_v3_reserve_factor_revenue('bsc', '0x6807dc923806fE8Fd134338EABCA509979a7e0cB', 'AAVE V3')}}
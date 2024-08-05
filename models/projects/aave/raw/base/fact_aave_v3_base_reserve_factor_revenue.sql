{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_base_reserve_factor_revenue",
    )
}}

{{ aave_v3_reserve_factor_revenue('base', '0xA238Dd80C259a72e81d7e4664a9801593F98d1c5', 'AAVE V3')}}
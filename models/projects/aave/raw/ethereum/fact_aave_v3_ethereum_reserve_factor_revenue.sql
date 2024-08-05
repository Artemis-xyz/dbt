{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_ethereum_reserve_factor_revenue",
    )
}}

{{ aave_v3_reserve_factor_revenue('ethereum', '0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2', 'AAVE V3')}}
{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v2_ethereum_reserve_factor_revenue",
    )
}}

{{ aave_v2_reserve_factor_revenue('ethereum', '0x464C71f6c2F760DdA6093dCB91C24c39e5d6e18c', 'AAVE V2')}}
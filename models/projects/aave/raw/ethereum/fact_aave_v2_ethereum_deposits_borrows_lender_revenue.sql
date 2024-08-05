{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v2_ethereum_deposits_borrows_lender_revenue",
    )
}}

{{ aave_deposits_borrows_lender_revenue('ethereum', 'AAVE V2', '0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9', 'raw_aave_v2_ethereum_borrows_deposits_revenue', 'raw_aave_v2_lending_ethereum')}}
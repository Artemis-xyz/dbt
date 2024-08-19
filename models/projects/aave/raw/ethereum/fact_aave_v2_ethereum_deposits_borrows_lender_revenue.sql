{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v2_ethereum_deposits_borrows_lender_revenue",
    )
}}

{{ aave_deposits_borrows_lender_revenue('ethereum', 'AAVE V2', '0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9', '0x311Bb771e4F8952E6Da169b425E7e92d6Ac45756', 'raw_aave_v2_ethereum_rpc_data')}}
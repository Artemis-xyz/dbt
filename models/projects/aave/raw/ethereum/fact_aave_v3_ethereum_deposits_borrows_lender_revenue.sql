{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_ethereum_deposits_borrows_lender_revenue",
    )
}}

{{ aave_deposits_borrows_lender_revenue('ethereum', 'AAVE V3', '0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2', '0x64b761D848206f447Fe2dd461b0c635Ec39EbB27', 'raw_aave_v3_ethereum_rpc_data')}}
{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_gnosis_deposits_borrows_lender_revenue",
    )
}}

{{ aave_deposits_borrows_lender_revenue('gnosis', 'AAVE V3', '0xb50201558B00496A145fE76f7424749556E326D8', 'raw_aave_v3_ethereum_borrows_deposits_revenue')}}
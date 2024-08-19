{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_gnosis_deposits_borrows_lender_revenue",
    )
}}

{{ aave_deposits_borrows_lender_revenue('gnosis', 'AAVE V3', '0xb50201558B00496A145fE76f7424749556E326D8', '0x7304979ec9E4EaA0273b6A037a31c4e9e5A75D16', 'raw_aave_v3_gnosis_rpc_data')}}
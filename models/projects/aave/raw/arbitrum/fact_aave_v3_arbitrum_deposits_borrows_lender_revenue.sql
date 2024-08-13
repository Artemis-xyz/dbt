{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_arbitrum_deposits_borrows_lender_revenue",
    )
}}

{{ aave_deposits_borrows_lender_revenue('arbitrum', 'AAVE V3', '0x794a61358D6845594F94dc1DB02A252b5b4814aD', 'raw_aave_v3_arbitrum_rpc_data')}}
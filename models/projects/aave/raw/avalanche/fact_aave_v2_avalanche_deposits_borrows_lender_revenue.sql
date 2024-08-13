{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v2_avalanche_deposits_borrows_lender_revenue",
    )
}}

{{ aave_deposits_borrows_lender_revenue('avalanche', 'AAVE V2', '0x4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C', 'raw_aave_v2_avalanche_rpc_data')}}
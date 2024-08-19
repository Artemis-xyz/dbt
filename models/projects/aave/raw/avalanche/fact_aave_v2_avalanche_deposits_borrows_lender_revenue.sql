{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v2_avalanche_deposits_borrows_lender_revenue",
    )
}}

{{ aave_deposits_borrows_lender_revenue('avalanche', 'AAVE V2', '0x4F01AeD16D97E3aB5ab2B501154DC9bb0F1A5A2C', '0x230B618aD4C475393A7239aE03630042281BD86e', 'raw_aave_v2_avalanche_rpc_data')}}
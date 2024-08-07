{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="raw_v2_avalanche_rpc_data",
    )
}}

{{ raw_aave_rpc_data('raw_aave_v2_avalanche_borrows_deposits_revenue') }}
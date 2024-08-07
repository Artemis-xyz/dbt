{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="raw_v3_bsc_rpc_data",
    )
}}

{{ raw_aave_rpc_data('raw_aave_v3_bsc_borrows_deposits_revenue') }}
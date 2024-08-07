{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="raw_v3_arbitrum_rpc_data",
    )
}}

{{ raw_aave_rpc_data('raw_aave_v3_arbitrum_borrows_deposits_revenue', 'raw_aave_v3_lending_arbitrum') }}
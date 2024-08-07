{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="raw_v2_polygon_rpc_data",
    )
}}

{{ raw_aave_rpc_data('raw_aave_v2_polygon_borrows_deposits_revenue', 'raw_aave_v2_lending_polygon') }}
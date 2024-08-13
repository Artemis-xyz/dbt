{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v2_polygon_deposits_borrows_lender_revenue",
    )
}}

{{ aave_deposits_borrows_lender_revenue('polygon', 'AAVE V2', '0x8dff5e27ea6b7ac08ebfdf9eb090f32ee9a30fcf', 'raw_aave_v2_polygon_rpc_data')}}
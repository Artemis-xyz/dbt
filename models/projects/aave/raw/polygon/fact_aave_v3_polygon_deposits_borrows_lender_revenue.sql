{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_polygon_deposits_borrows_lender_revenue",
    )
}}

{{ aave_deposits_borrows_lender_revenue('polygon', 'AAVE V3', '0x794a61358D6845594F94dc1DB02A252b5b4814aD', '0x8145eddDf43f50276641b55bd3AD95944510021E', 'raw_aave_v3_polygon_rpc_data')}}
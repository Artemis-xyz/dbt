{{
    config(
        materialized = "table",
        snowflake_warehouse = "DYDX",
        database = "dydx_v4",
        schema = "raw",
        alias = "fact_perps_token_prices"
    )
}}

select * from {{ ref('fact_dydx_v4_perps_prices') }}
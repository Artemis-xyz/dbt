{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v2_poylgon_reserve_factor_revenue",
    )
}}

{{ aave_v2_reserve_factor_revenue('polygon', '0xe8599F3cc5D38a9aD6F3684cd5CEa72f10Dbc383', 'AAVE V2')}}
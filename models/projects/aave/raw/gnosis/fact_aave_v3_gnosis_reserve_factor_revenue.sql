{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v3_gnosis_reserve_factor_revenue",
    )
}}

{{ aave_v3_reserve_factor_revenue('gnosis', '0xb50201558B00496A145fE76f7424749556E326D8', 'AAVE V3')}}
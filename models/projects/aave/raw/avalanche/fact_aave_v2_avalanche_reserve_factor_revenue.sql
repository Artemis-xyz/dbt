{{
    config(
        materialized="table",
        snowflake_warehouse="AAVE",
        database="aave",
        schema="raw",
        alias="fact_v2_avalanche_reserve_factor_revenue",
    )
}}

{{ aave_v2_reserve_factor_revenue('avalanche', '0x467b92aF281d14cB6809913AD016a607b5ba8A36', 'AAVE V2')}}
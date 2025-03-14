{{
    config(
        materialized="table",
        snowflake_warehouse= "USDA",
        database="usda",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDa", breakdown='symbol') }}

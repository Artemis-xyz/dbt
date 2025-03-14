{{
    config(
        materialized="table",
        snowflake_warehouse= "USDF",
        database="usdf",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDF", breakdown='symbol') }}

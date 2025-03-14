{{
    config(
        materialized="table",
        snowflake_warehouse= "USDD",
        database="usdd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDD", breakdown='symbol') }}

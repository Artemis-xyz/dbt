{{
    config(
        materialized="table",
        snowflake_warehouse="USDP",
        database="usdp",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDP", breakdown='symbol') }}

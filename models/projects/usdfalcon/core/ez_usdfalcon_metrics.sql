{{
    config(
        materialized="table",
        snowflake_warehouse= "USDFALCON",
        database="usdfalcon",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDFALCON", breakdown='symbol') }}

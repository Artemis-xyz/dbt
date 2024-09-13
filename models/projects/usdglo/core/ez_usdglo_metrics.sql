{{
    config(
        materialized="table",
        snowflake_warehouse="USDGLO",
        database="usdglo",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDGLO", breakdown='symbol') }}

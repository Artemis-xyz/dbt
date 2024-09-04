{{
    config(
        materialized="table",
        snowflake_warehouse="USDC",
        database="usdc",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_metrics("USDC", breakdown='symbol') }}

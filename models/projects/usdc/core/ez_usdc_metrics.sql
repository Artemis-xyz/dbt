{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="USDC",
        database="usdc",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_ez_metrics("USDC") }}

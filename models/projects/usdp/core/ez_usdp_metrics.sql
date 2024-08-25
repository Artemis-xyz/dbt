{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="USDP",
        database="usdp",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_ez_metrics("USDP") }}

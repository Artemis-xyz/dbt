{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="USDGLO",
        database="usdglo",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_ez_metrics("USDGLO") }}

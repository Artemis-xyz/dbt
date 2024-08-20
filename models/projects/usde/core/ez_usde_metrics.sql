{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="USDE",
        database="usde",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_ez_metrics("USDE") }}

{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="USDT",
        database="usdt",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_ez_metrics("USDT") }}

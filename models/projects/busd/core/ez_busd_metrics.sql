{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="BUSD",
        database="busd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_ez_metrics("BUSD") }}

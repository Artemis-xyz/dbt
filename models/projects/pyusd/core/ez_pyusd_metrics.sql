{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="PYUSD",
        database="pyusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_ez_metrics("PYUSD") }}

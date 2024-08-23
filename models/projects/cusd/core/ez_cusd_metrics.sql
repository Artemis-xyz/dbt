{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="CUSD",
        database="cusd",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_ez_metrics("CUSD") }}

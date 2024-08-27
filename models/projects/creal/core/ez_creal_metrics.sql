{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="CREAL",
        database="creal",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_ez_metrics("CREAL") }}

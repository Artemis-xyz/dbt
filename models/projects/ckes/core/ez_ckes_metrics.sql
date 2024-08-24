{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="CKES",
        database="ckes",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_ez_metrics("CKES") }}

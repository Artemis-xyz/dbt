{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="DAI",
        database="dai",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_ez_metrics("DAI") }}

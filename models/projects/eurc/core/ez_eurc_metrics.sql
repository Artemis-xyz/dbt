{{
    config(
        materialized="incremental",
        unique_key="date",
        snowflake_warehouse="EURC",
        database="eurc",
        schema="core",
        alias="ez_metrics",
    )
}}

{{ get_stablecoin_ez_metrics("EURC") }}

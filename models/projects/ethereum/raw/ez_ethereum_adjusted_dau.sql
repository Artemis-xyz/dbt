{{
    config(
        materialized="incremental",
        snowflake_warehouse="ETHEREUM",
        database="ethereum",
        schema="raw",
        alias="ez_adjusted_dau",
        incremental_strategy="merge",
        unique_key="date"
    )
}}

{{ get_adjusted_dau("ethereum") }}

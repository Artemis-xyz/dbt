{{
    config(
        materialized="incremental",
        snowflake_warehouse="ETHEREUM",
        database="ethereum",
        schema="raw",
        alias="ez_adjusted_dau",
    )
}}

{{ get_adjusted_dau("ethereum") }}

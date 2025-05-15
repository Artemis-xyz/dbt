{{
    config(
        materialized="incremental",
        snowflake_warehouse="BASE_MD",
        database="base",
        schema="raw",
        alias="ez_adjusted_dau",
    )
}}

{{ get_adjusted_dau("base") }}

{{
    config(
        materialized="incremental",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="ez_adjusted_dau",
    )
}}

{{ get_adjusted_dau("optimism") }}

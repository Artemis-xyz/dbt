{{
    config(
        materialized="incremental",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="raw",
        alias="ez_adjusted_dau",
    )
}}

{{ get_adjusted_dau("arbitrum") }}

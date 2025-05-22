{{ 
    config(
        materialized="incremental",
        unique_key="recipient",
        snowflake_warehouse="BASE_MD",
    )
}}

{{ first_funding("base") }}

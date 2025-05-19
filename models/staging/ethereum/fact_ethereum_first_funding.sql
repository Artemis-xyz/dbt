{{ 
    config(
        materialized="incremental",
        unique_key="recipient",
        snowflake_warehouse="ETHEREUM",
    )
}}

{{ first_funding("ethereum") }}

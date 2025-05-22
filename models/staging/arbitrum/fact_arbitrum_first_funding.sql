{{ 
    config(
        materialized="incremental",
        unique_key="recipient",
        snowflake_warehouse="ARBITRUM_MD",
    )
}}

{{ first_funding("arbitrum") }}

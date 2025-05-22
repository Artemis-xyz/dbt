{{ 
    config(
        materialized="incremental",
        unique_key="recipient",
        snowflake_warehouse="OPTIMISM",
    )
}}

{{ first_funding("optimism") }}

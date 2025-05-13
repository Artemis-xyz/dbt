{{ 
    config(
        materialized="incremental",
        unique_key="recipient"
    )
}}

{{ first_funding("arbitrum") }}

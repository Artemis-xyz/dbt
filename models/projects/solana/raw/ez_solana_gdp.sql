{{ 
    config(
        materialized="table",
        database="solana",
        schema="raw",
        alias="ez_solana_gdp",
    )
}}

{{ get_chain_gdp("solana") }}
{{ 
    config(
        materialized="table",
        database="solana",
        schema="raw",
        alias="ez_solana_tea",
        tags=["tea"]
    )
}}

{{ get_chain_total_economic_activity("solana") }}
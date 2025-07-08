{{ 
    config(
        materialized="table",
        database="arbitrum",
        schema="raw",
        alias="ez_arbitrum_tea",
        tags=["tea"]
    )
}}

{{ get_chain_total_economic_activity("arbitrum") }}
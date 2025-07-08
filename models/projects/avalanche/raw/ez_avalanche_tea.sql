{{ 
    config(
        materialized="table",
        database="avalanche",
        schema="raw",
        alias="ez_avalanche_tea",
        tags=["tea"]
    )
}}

{{ get_chain_total_economic_activity("avalanche") }}
{{ 
    config(
        materialized="table",
        database="ethereum",
        schema="raw",
        alias="ez_ethereum_tea",
        tags=["tea"]
    )
}}

{{ get_chain_total_economic_activity("ethereum") }}
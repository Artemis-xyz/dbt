{{ 
    config(
        materialized="table",
        database="avalanche",
        schema="raw",
        alias="ez_avalanche_gdp",
        tags=["gdp"]
    )
}}

{{ get_chain_gdp("avalanche") }}
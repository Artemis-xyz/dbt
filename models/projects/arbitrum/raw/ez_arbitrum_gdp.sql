{{ 
    config(
        materialized="table",
        database="arbitrum",
        schema="raw",
        alias="ez_arbitrum_gdp",
        tags=["gdp"]
    )
}}

{{ get_chain_gdp("arbitrum") }}
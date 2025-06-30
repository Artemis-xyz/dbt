{{ 
    config(
        materialized="table",
        database="ethereum",
        schema="raw",
        alias="ez_ethereum_gdp",
        tags=["gdp"]
    )
}}

{{ get_chain_gdp("ethereum") }}
{{ 
    config(
        materialized="table",
        database="sui",
        schema="raw",
        alias="ez_sui_gdp",
        tags=["gdp"]
    )
}}

{{ get_chain_gdp("sui") }}
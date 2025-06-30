{{ 
    config(
        materialized="table",
        database="near",
        schema="raw",
        alias="ez_near_gdp",
        tags=["gdp"]
    )
}}

{{ get_chain_gdp("near") }}
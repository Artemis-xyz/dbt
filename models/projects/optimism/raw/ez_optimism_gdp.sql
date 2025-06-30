{{ 
    config(
        materialized="table",
        database="optimism",
        schema="raw",
        alias="ez_optimism_gdp",
        tags=["gdp"]
    )
}}

{{ get_chain_gdp("optimism") }}
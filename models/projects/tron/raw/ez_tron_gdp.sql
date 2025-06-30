{{ 
    config(
        materialized="table",
        database="tron",
        schema="raw",
        alias="ez_tron_gdp",
        tags=["gdp"]
    )
}}

{{ get_chain_gdp("tron") }}
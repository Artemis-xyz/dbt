{{ 
    config(
        materialized="table",
        database="polygon",
        schema="raw",
        alias="ez_polygon_gdp",
        tags=["gdp"]
    )
}}

{{ get_chain_gdp("polygon") }}
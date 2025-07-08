{{ 
    config(
        materialized="table",
        database="polygon",
        schema="raw",
        alias="ez_polygon_tea",
        tags=["tea"]
    )
}}

{{ get_chain_total_economic_activity("polygon") }}
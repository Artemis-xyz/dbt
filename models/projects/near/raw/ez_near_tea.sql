{{ 
    config(
        materialized="table",
        database="near",
        schema="raw",
        alias="ez_near_tea",
        tags=["tea"]
    )
}}

{{ get_chain_total_economic_activity("near") }}
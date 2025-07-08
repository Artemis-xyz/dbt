{{ 
    config(
        materialized="table",
        database="optimism",
        schema="raw",
        alias="ez_optimism_tea",
        tags=["tea"]
    )
}}

{{ get_chain_total_economic_activity("optimism") }}
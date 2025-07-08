{{ 
    config(
        materialized="table",
        database="tron",
        schema="raw",
        alias="ez_tron_tea",
        tags=["tea"]
    )
}}

{{ get_chain_total_economic_activity("tron") }}
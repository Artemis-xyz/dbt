{{
    config(
        materialized="view",
        snowflake_warehouse="OPTIMISM",
        database="optimism",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_optimism_bridge_transfers')}}
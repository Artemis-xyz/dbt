{{
    config(
        materialized="view",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_arbitrum_one_bridge_transfers')}}
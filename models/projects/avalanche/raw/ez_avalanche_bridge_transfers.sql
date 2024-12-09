{{
    config(
        materialized="view",
        snowflake_warehouse="AVALANCHE",
        database="avalanche",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_avalanche_bridge_transfers')}}
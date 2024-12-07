{{
    config(
        materialized="view",
        snowflake_warehouse="RAINBOW",
        database="rainbow",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_rainbow_bridge_transfers')}}
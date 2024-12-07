{{
    config(
        materialized="view",
        snowflake_warehouse="RAINBOW_BRIDGE",
        database="rainbow_bridge",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_rainbow_bridge_transfers')}}
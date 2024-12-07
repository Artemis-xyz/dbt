{{
    config(
        materialized="view",
        snowflake_warehouse="WORMHOLE",
        database="wormhole",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_wormhole_transfers')}}
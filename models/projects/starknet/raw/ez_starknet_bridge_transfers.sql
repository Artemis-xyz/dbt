{{
    config(
        materialized="view",
        snowflake_warehouse="STARKNET",
        database="starknet",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_starknet_bridge_transfers')}}
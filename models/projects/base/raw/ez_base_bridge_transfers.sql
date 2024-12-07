{{
    config(
        materialized="view",
        snowflake_warehouse="BASE",
        database="base",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_base_bridge_transfers')}}
{{
    config(
        materialized="view",
        snowflake_warehouse="ACROSS",
        database="across",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_across_transfers')}}
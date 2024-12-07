{{
    config(
        materialized="view",
        snowflake_warehouse="SYNAPSE",
        database="synapse",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_synapse_transfers')}}
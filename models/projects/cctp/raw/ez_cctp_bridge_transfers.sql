{{
    config(
        materialized="view",
        snowflake_warehouse="CCTP",
        database="cctp",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_cctp_transfers')}}
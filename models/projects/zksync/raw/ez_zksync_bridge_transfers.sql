{{
    config(
        materialized="view",
        snowflake_warehouse="ZKSYNC",
        database="zksync",
        schema="raw",
        alias="ez_bridge_transfers",
    )
}}


select *
from {{ref('fact_zksync_era_bridge_transfers')}}
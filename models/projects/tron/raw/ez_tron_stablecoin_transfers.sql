{{
    config(
        materialized="view",
        snowflake_warehouse="TRON",
        database="tron",
        schema="raw",
        alias="ez_stablecoin_transfers",
    )
}}

select
    *
from {{ref("fact_tron_stablecoin_transfers")}}
{{
    config(
        materialized="view",
        snowflake_warehouse="BSC_MD",
        database="bsc",
        schema="raw",
        alias="ez_stablecoin_transfers",
    )
}}

select
    *
from {{ref("fact_bsc_stablecoin_transfers")}}
{{
    config(
        materialized="view",
        snowflake_warehouse="ARBITRUM",
        database="arbitrum",
        schema="raw",
        alias="ez_stablecoin_transfers",
    )
}}

select
    *
from {{ref("fact_arbitrum_stablecoin_transfers")}}
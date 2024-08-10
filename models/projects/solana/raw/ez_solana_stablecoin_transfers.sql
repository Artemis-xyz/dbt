{{
    config(
        materialized="view",
        snowflake_warehouse="SOLANA",
        database="solana",
        schema="raw",
        alias="ez_stablecoin_transfers",
    )
}}

select
    *
from {{ref("fact_solana_stablecoin_transfers")}}
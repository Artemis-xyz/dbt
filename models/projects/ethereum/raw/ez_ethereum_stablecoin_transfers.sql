{{
    config(
        materialized="view",
        snowflake_warehouse="ETHEREUM",
        database="ethereum",
        schema="raw",
        alias="ez_stablecoin_transfers",
    )
}}

select
    *
from {{ref("fact_ethereum_stablecoin_transfers")}}
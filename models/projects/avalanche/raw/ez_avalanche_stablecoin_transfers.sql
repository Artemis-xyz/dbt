{{
    config(
        materialized="view",
        snowflake_warehouse="AVALANCHE",
        database="avalanche",
        schema="raw",
        alias="ez_stablecoin_transfers",
    )
}}

select
    *
from {{ref("fact_avalanche_stablecoin_transfers")}}
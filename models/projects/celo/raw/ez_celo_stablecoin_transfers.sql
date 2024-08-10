{{
    config(
        materialized="view",
        snowflake_warehouse="CELO",
        database="celo",
        schema="raw",
        alias="ez_stablecoin_transfers",
    )
}}

select
    *
from {{ref("fact_celo_stablecoin_transfers")}}
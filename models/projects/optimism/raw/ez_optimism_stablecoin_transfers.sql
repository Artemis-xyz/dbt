{{
    config(
        materialized="view",
        snowflake_warehouse="optimism",
        database="optimism",
        schema="raw",
        alias="ez_stablecoin_transfers",
    )
}}

select
    *
from {{ref("fact_optimism_stablecoin_transfers")}}
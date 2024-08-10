{{
    config(
        materialized="view",
        snowflake_warehouse="BASE",
        database="base",
        schema="raw",
        alias="ez_stablecoin_transfers",
    )
}}

select
    *
from {{ref("fact_base_stablecoin_transfers")}}
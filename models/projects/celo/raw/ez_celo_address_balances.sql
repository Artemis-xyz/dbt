{{
    config(
        materialized="view",
        snowflake_warehouse="CELO",
        database="celo",
        schema="raw",
        alias="ez_address_balances",
    )
}}

select * 
from {{ ref("fact_celo_address_balances") }}
{{
    config(
        materialized="view",
        snowflake_warehouse="CELO",
        database="celo",
        schema="raw",
        alias="ez_transactions",
    )
}}

select
    *
from {{ref("fact_celo_transactions")}}
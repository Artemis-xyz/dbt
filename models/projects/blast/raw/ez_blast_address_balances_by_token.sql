{{
    config(
        materialized="view",
        snowflake_warehouse="BLAST",
        database="blast",
        schema="raw",
        alias="ez_address_balances_by_token",
    )
}}

select * from {{ ref("fact_blast_address_balances_by_token") }}
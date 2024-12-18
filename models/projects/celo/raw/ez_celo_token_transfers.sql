{{
    config(
        materialized="view",
        snowflake_warehouse="CELO",
        database="celo",
        schema="raw",
        alias="ez_token_transfers",
    )
}}

select *
from {{ ref('fact_celo_token_transfers') }}
where block_timestamp < to_date(sysdate())
{{
    config(
        materialized="view",
        snowflake_warehouse="STELLAR",
        database="stellar",
        schema="raw",
        alias="ez_stablecoin_transfers",
    )
}}
select *
from {{ ref("fact_stellar_stablecoin_transfers") }}
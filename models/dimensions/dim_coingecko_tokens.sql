{{ config(materialized="table") }}
select *
from {{ ref("fact_coingecko_token_metadata") }}

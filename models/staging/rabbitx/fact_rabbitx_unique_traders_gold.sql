{{ config(materialized="table") }}

select date, unique_traders, app, category, chain
from {{ ref("fact_rabbitx_unique_traders") }}

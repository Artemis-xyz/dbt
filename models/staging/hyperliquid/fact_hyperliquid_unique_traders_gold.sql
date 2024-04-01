{{ config(materialized="table") }}

select date, unique_traders, app, category, chain
from {{ ref("fact_hyperliquid_unique_traders") }}

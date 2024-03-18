{{ config(materialized="table") }}
select date, unique_traders, app, category, chain
from {{ ref("fact_dydx_unique_traders") }}

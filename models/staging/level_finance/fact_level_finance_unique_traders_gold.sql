{{ config(materialized="table") }}

select date, unique_traders, app, category, chain
from {{ ref("fact_level_finance_unique_traders") }}

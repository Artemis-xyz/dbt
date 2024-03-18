{{ config(materialized="table") }}

select date, unique_traders, app, category, chain
from {{ ref("fact_ktx_finance_unique_traders") }}

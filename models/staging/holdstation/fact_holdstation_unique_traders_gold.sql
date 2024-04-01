{{ config(materialized="table") }}

select date, unique_traders, app, category, chain
from {{ ref("fact_holdstation_unique_traders") }}

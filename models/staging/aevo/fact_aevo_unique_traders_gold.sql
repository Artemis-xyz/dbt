{{ config(materialized="table") }}

select date, unique_traders, app, category, chain
from {{ ref("fact_aevo_unique_traders") }}

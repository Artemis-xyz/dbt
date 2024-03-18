{{ config(materialized="table") }}
select chain, date, app, unique_traders, category
from {{ ref("fact_vertex_unique_traders") }}

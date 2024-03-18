{{ config(materialized="table") }}
select chain, date, app, trading_volume, category
from {{ ref("fact_vertex_trading_volume") }}

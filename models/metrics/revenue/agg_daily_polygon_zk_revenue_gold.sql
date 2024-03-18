{{ config(materialized="table") }}
select date, expenses_native, expenses, revenue, 'polygon_zk' as chain
from {{ ref("agg_daily_polygon_zk_revenue") }}

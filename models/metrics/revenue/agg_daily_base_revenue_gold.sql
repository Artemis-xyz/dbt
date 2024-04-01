{{ config(materialized="table") }}
select chain, date, revenue
from {{ ref("agg_daily_base_revenue") }}

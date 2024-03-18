{{ config(materialized="table") }}
select chain, date, revenue
from {{ ref("agg_daily_optimism_revenue") }}

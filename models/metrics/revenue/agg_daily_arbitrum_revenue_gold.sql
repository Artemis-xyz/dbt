{{ config(materialized="table") }}
select chain, date, revenue
from {{ ref("agg_daily_arbitrum_revenue") }}

{{ config(materialized="table") }}
select chain, date, native_token_burn, revenue
from {{ ref("agg_daily_ethereum_revenue") }}

{{ config(materialized="table") }}
select date, unique_traders, app, category, chain
from {{ ref("fact_dydx_v4_unique_traders") }}
where date < to_date(sysdate())

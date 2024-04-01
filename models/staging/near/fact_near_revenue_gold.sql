{{ config(materialized="table") }}

select date, chain, revenue_native, revenue
from {{ ref("fact_near_revenue") }}
where date < to_date(sysdate())

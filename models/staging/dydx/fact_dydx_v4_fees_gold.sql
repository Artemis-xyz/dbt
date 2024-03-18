{{ config(materialized="table") }}
select *
from {{ ref("fact_dydx_v4_fees") }}
where date < to_date(sysdate())

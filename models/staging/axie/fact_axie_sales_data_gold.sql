{{ config(materialized="table") }}
select *
from {{ ref("fact_axie_sales_data") }}
where date < to_date(sysdate())

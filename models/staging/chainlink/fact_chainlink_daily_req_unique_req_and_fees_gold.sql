{{ config(materialized="table") }}
select *
from {{ ref("fact_chainlink_daily_req_unique_req_and_fees") }}
where date < to_date(sysdate())

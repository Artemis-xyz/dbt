{{ config(materialized="table") }}
select date, total_fees_usd as fees, fees_burned_usd as revenue, 'flow' as chain
from {{ ref("fact_flow_fees_revs") }}
where date < to_date(sysdate())

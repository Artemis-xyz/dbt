{{ config(materialized="table") }}
select date, fees, revenue, chain
from {{ ref("fact_tron_fees_revenue") }}

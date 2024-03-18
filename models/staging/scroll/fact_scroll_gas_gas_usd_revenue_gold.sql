{{ config(materialized="table") }}
select date, gas, gas_usd, revenue, chain
from {{ ref("fact_scroll_gas_gas_usd_revenue") }}

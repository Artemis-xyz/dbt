{{ config(materialized="table") }}
select date, gas_usd, revenue, chain
from {{ ref("fact_cosmoshub_fees_and_revenue") }}

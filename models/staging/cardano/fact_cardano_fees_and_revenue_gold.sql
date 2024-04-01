{{ config(materialized="table") }}
select date, gas_usd, revenue, chain
from {{ ref("fact_cardano_fees_and_revenue") }}

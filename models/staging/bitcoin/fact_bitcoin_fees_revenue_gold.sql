{{ config(materialized="table") }}
select date, chain, fees, fees_native, revenue
from {{ ref("fact_bitcoin_fees_revenue") }}

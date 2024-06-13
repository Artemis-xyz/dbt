{{ config(materialized="table", snowflake_warehouse="BITCOIN") }}
select date, chain, fees, fees_native, revenue
from {{ ref("fact_bitcoin_fees_revenue") }}

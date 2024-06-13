{{ config(materialized="table", snowflake_warehouse="POLKADOT") }}
select date, chain, daa, txns, gas, gas_usd, revenue
from {{ ref("fact_polkadot_daa_txns_gas_gas_usd_revenue") }}
